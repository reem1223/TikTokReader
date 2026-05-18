const { WebcastPushConnection } = require('tiktok-live-connector');
const { WebSocketServer } = require('ws');
const { HttpsProxyAgent } = require('https-proxy-agent');
const http = require('http');
const https = require('https');
const fs = require('fs');
const path = require('path');

const PORT = process.env.PORT || 3000;
const PROXY_URL = process.env.PROXY_URL || null; // e.g. http://user:pass@proxy.webshare.io:80

// Google Translate TTS — fetches MP3 audio for a given text and language
function fetchGoogleTTS(text, lang) {
    return new Promise((resolve, reject) => {
        const encoded = encodeURIComponent(text.substring(0, 200));
        const url = `https://translate.google.com/translate_tts?ie=UTF-8&tl=${lang}&client=tw-ob&q=${encoded}`;

        https.get(url, {
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Referer': 'https://translate.google.com/',
            }
        }, (response) => {
            if (response.statusCode === 302 || response.statusCode === 301) {
                // Follow redirect
                https.get(response.headers.location, (res2) => {
                    const chunks = [];
                    res2.on('data', (c) => chunks.push(c));
                    res2.on('end', () => resolve(Buffer.concat(chunks)));
                }).on('error', reject);
                return;
            }
            if (response.statusCode !== 200) {
                reject(new Error(`Google TTS returned status ${response.statusCode}`));
                return;
            }
            const chunks = [];
            response.on('data', (c) => chunks.push(c));
            response.on('end', () => resolve(Buffer.concat(chunks)));
        }).on('error', reject);
    });
}

// Create HTTP server to serve index.html and TTS API
const httpServer = http.createServer(async (req, res) => {
    // TTS proxy endpoint
    if (req.url.startsWith('/api/tts') && req.method === 'POST') {
        let body = '';
        req.on('data', (chunk) => body += chunk);
        req.on('end', async () => {
            try {
                const { text, lang } = JSON.parse(body);
                if (!text || !lang) {
                    res.writeHead(400, { 'Content-Type': 'application/json' });
                    res.end(JSON.stringify({ error: 'Missing text or lang' }));
                    return;
                }
                console.log(`[TTS] Generating ${lang}: "${text.substring(0, 50)}..."`);
                const audioBuffer = await fetchGoogleTTS(text, lang);
                const base64 = audioBuffer.toString('base64');
                res.writeHead(200, { 'Content-Type': 'application/json' });
                res.end(JSON.stringify({ audio: base64 }));
            } catch (err) {
                console.error('[TTS] Error:', err.message);
                res.writeHead(500, { 'Content-Type': 'application/json' });
                res.end(JSON.stringify({ error: err.message }));
            }
        });
        return;
    }

    // Keep-alive endpoint to prevent Render.com spin-down
    if (req.url === '/health') {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ status: 'ok', uptime: process.uptime() }));
        return;
    }

    if (req.url === '/' || req.url === '/index.html') {
        const filePath = path.join(__dirname, 'index.html');
        fs.readFile(filePath, (err, data) => {
            if (err) {
                res.writeHead(500);
                res.end('Error loading index.html');
                return;
            }
            res.writeHead(200, { 'Content-Type': 'text/html' });
            res.end(data);
        });
    } else {
        res.writeHead(404);
        res.end('Not found');
    }
});

// Create WebSocket server on the same port
const wss = new WebSocketServer({ server: httpServer });

wss.on('connection', (ws) => {
    let tiktokConnection = null;
    let currentUsername = null;
    let manualDisconnect = false;
    let reconnectAttempts = 0;
    let reconnectTimer = null;
    let usePollingFallback = false; // Switch to polling if websocket upgrade is refused
    let disableGiftInfo = false; // Disable extended gift info on 403
    let cachedRoomId = null; // Cache roomId to skip page scraping on reconnect
    const MAX_RECONNECT_ATTEMPTS = 20;
    const RECONNECT_DELAYS = [2000, 3000, 5000, 8000, 12000, 20000, 30000, 45000, 60000]; // escalating delays

    function connectToTikTok(username) {
        const mode = usePollingFallback ? 'polling' : 'websocket';
        const usingCache = cachedRoomId ? ` [roomId:${cachedRoomId}]` : '';
        console.log(`[TikTok] Connecting to @${username}... [${mode}]${usingCache}${PROXY_URL ? ' (via proxy)' : ''}${reconnectAttempts > 0 ? ` (attempt ${reconnectAttempts + 1})` : ''}`);

        // Clean up previous connection to avoid stale state
        if (tiktokConnection) {
            try { tiktokConnection.disconnect(); } catch (e) {}
            tiktokConnection = null;
        }

        const options = {
            processInitialData: true,
            enableExtendedGiftInfo: !disableGiftInfo,
            enableWebsocketUpgrade: !usePollingFallback,
            requestPollingIntervalMs: 2000,
            sessionId: usePollingFallback ? (process.env.TIKTOK_SESSION_ID || null) : null,
            emitRawEvents: true,
        };

        if (PROXY_URL) {
            const agent = new HttpsProxyAgent(PROXY_URL);
            options.requestOptions = {
                httpsAgent: agent,
                httpAgent: agent,
                proxy: false,
                timeout: 15000,
            };
            options.websocketOptions = {
                agent: agent,
            };
        }

        tiktokConnection = new WebcastPushConnection(username, options);

        // Pass cached roomId to skip page scraping on reconnect
        tiktokConnection.connect(cachedRoomId || undefined)
            .then((state) => {
                reconnectAttempts = 0;
                // Cache the roomId for future reconnects
                if (state.roomId) {
                    cachedRoomId = state.roomId;
                    console.log(`[TikTok] Cached roomId: ${cachedRoomId}`);
                }
                console.log(`[TikTok] ✅ Connected to @${username} [${mode}] — Room: "${state.roomInfo?.title || 'Live'}"`);
                ws.send(JSON.stringify({
                    type: 'connected',
                    roomInfo: state.roomInfo?.title || 'Live'
                }));
            })
            .catch((err) => {
                // Safely extract error info (err can contain circular refs)
                const msg = (err.exception ? err.exception.message : err.message) || '';
                const info = err.info || '';
                const fullErr = `${info} — ${msg}`;
                console.error(`[TikTok] Connection failed:`, fullErr);

                // Adapt strategy based on error
                if (fullErr.includes('websocket upgrade')) {
                    console.log('[TikTok] WebSocket upgrade refused — switching to polling mode');
                    usePollingFallback = true;
                }
                if (fullErr.includes('403') || fullErr.includes('available gifts')) {
                    console.log('[TikTok] Gift fetch 403 — disabling extended gift info');
                    disableGiftInfo = true;
                }
                // If roomId extraction failed WITH a cache, the cache might be stale — clear it
                if (fullErr.includes('room_id') && cachedRoomId) {
                    console.log('[TikTok] Cached roomId may be stale — clearing for next attempt');
                    cachedRoomId = null;
                }

                if (!manualDisconnect && reconnectAttempts < MAX_RECONNECT_ATTEMPTS) {
                    scheduleReconnect(username);
                } else {
                    ws.send(JSON.stringify({
                        type: 'error',
                        message: fullErr || 'Failed to connect. Is the user live?'
                    }));
                }
            });

        // Chat messages
        tiktokConnection.on('chat', (data) => {
            const isMod = data.isModerator ||
                (data.userBadges && data.userBadges.some(b => b.type && b.type.includes('moderator')));
            const isFollower = data.followRole >= 1;
            const badges = data.userBadges || [];
            const fanBadge = badges.find(b => b.badgeSceneType === 10);
            const isFan = !!fanBadge;
            const gifterLevel = data.gifterLevel || 0;
            const teamMemberLevel = data.teamMemberLevel || 0;
            const payload = {
                type: 'chat',
                user: data.uniqueId || data.nickname || 'Anonymous',
                displayName: data.nickname || data.uniqueId || 'Anonymous',
                comment: data.comment,
                isMod: !!isMod,
                isFollower: isFollower,
                isFan: isFan,
                gifterLevel: gifterLevel,
                teamMemberLevel: teamMemberLevel,
            };
            const tags = [isMod ? '[MOD]' : '', isFollower ? '[FOL]' : '', isFan ? '[FAN]' : '', gifterLevel ? `[GL:${gifterLevel}]` : ''].filter(Boolean).join(' ');
            console.log(`[Chat] @${payload.user}${tags ? ' ' + tags : ''}: ${payload.comment}`);
            ws.send(JSON.stringify(payload));
        });

        // Gift events
        tiktokConnection.on('gift', (data) => {
            if (data.giftType === 1 && !data.repeatEnd) return;
            const payload = {
                type: 'gift',
                user: data.uniqueId || data.nickname || 'Anonymous',
                displayName: data.nickname || data.uniqueId || 'Anonymous',
                giftName: data.giftName || 'gift',
                giftCount: data.repeatCount || 1,
                diamondCount: data.diamondCount || 0,
                giftPictureUrl: data.giftPictureUrl || '',
            };
            console.log(`[Gift] @${payload.user} sent ${payload.giftCount}x ${payload.giftName}`);
            ws.send(JSON.stringify(payload));
        });

        // Member join events
        tiktokConnection.on('member', (data) => {
            const payload = {
                type: 'member',
                user: data.uniqueId || data.nickname || 'Anonymous',
                displayName: data.nickname || data.uniqueId || 'Anonymous',
            };
            console.log(`[Join] @${payload.user} joined`);
            ws.send(JSON.stringify(payload));
        });

        // Battle/Match events
        let battleActive = false;
        let battleStartTime = 0;
        let lastBattleEndTime = 0;
        let lastTaskTime = 0;
        let lastMultiplierSent = null;
        let lastMissionSent = null;
        const connectionTime = Date.now();  // track when this connection was established
        const BATTLE_COOLDOWN = 15000;      // 15s cooldown after match end before allowing new match start
        const MATCH_START_GRACE = 60000;    // 60s grace: ignore battleStatus 2 right after match start (stale data)
        const CONNECT_GRACE = 30000;        // 30s grace: don't auto-detect battle from scores right after connect
        const TASK_DEBOUNCE = 8000;         // 8s debounce between same-type announcements

        tiktokConnection.on('linkMicBattle', (data) => {
            const hasBattleUsers = data.battleUsers && data.battleUsers.length > 0;
            const now = Date.now();

            console.log(`[Battle] linkMicBattle — users: ${hasBattleUsers}, active: ${battleActive}, status: ${JSON.stringify(data.battleStatus)}`);

            // Only trigger match start if not already in battle AND cooldown passed
            if (hasBattleUsers && !battleActive && (now - lastBattleEndTime > BATTLE_COOLDOWN)) {
                battleActive = true;
                battleStartTime = now;
                lastMultiplierSent = null;
                lastMissionSent = null;
                console.log(`[Battle] ✅ MATCH STARTED`);
                ws.send(JSON.stringify({ type: 'battle_start' }));
            }
        });

        tiktokConnection.on('linkMicArmies', (data) => {
            const now = Date.now();

            // Detect match end from battleStatus 2
            if (data.battleStatus === 2) {
                // Ignore stale end signals during grace period after match start
                if (battleActive && (now - battleStartTime < MATCH_START_GRACE)) {
                    console.log(`[Battle] ⏳ Ignoring battleStatus 2 (grace period — ${Math.round((now - battleStartTime) / 1000)}s after start)`);
                    return;
                }
                if (battleActive) {
                    battleActive = false;
                    lastBattleEndTime = now;
                    lastMultiplierSent = null;
                    lastMissionSent = null;
                    console.log(`[Battle] ❌ MATCH ENDED`);
                    ws.send(JSON.stringify({ type: 'battle_end' }));
                    return;
                }
                return;
            }

            // Suppress spam: only forward if armies have actual score data
            if (!data.battleArmies || data.battleArmies.length < 2) return;
            const armies = data.battleArmies;
            const scores = armies.map(a => a.points || 0);
            if (scores.every(s => s === 0)) return;

            // If we get live score data and battle isn't active, it means we missed the start
            // BUT skip this during connect grace period (stale data from reconnect)
            if (!battleActive && scores.some(s => s > 0)) {
                if (now - connectionTime < CONNECT_GRACE) {
                    console.log(`[Battle] ⏳ Ignoring scores (connect grace — ${Math.round((now - connectionTime) / 1000)}s after connect)`);
                    // Don't set battleActive — let linkMicBattle handle the real start
                } else {
                    battleActive = true;
                    battleStartTime = now;
                    lastMultiplierSent = null;
                    lastMissionSent = null;
                    console.log(`[Battle] ✅ MATCH STARTED (detected from live scores)`);
                    ws.send(JSON.stringify({ type: 'battle_start' }));
                }
            }

            ws.send(JSON.stringify({
                type: 'battle_update',
                scores: scores,
            }));
        });

        // Catch-all: process battle-related raw events
        tiktokConnection.on('rawData', (messageTypeName, binary) => {
            // Skip high-frequency known events
            if (['WebcastChatMessage', 'WebcastGiftMessage', 'WebcastMemberMessage', 'WebcastSocialMessage',
                 'WebcastRoomUserSeqMessage', 'WebcastControlMessage', 'WebcastLinkMicBattle',
                 'WebcastLinkMicArmies', 'WebcastLikeMessage', 'WebcastLinkMicOpponentGifts',
                 'WebcastGiftPanelUpdateMessage', 'WebcastBarrageMessage'].includes(messageTypeName)) return;

            console.log(`[RawData] Event type: ${messageTypeName}`);

            // Battle Task = missions and multiplier info
            if (messageTypeName === 'WebcastLinkmicBattleTaskMessage') {
                try {
                    const now = Date.now();
                    const text = binary.toString('utf-8').replace(/[^\x20-\x7E\u0590-\u05FF\u0600-\u06FF]/g, ' ').replace(/\s+/g, ' ').trim();
                    console.log(`[BattleTask] Raw: ${text.substring(0, 400)}`);

                    // Check for gifter pattern first — if present, the message is a MISSION
                    // "instructions_1 multi 2" + "gifter_1 multi 3" = "3 gifters needed to unlock double"
                    // The multiplier in instructions_1 is the REWARD, not an active multiplier
                    const gifterMatch = text.match(/gifter_\d+\s+multi\s+(\d+)/);
                    const multiplierMatch = text.match(/instructions_\d+\s+multi\s+(\d+)/);

                    if (gifterMatch) {
                        // It's a gifter mission — multiplier is just the reward description
                        const gifterCount = gifterMatch[1];
                        const rewardNum = multiplierMatch ? parseInt(multiplierMatch[1]) : null;
                        const reward = rewardNum === 2 ? 'כפול' : rewardNum === 3 ? 'משולש' : '';
                        const missionKey = 'gifter_' + gifterCount + '_' + (rewardNum || '');
                        // Announce if different from last OR if debounce expired (allows corrections)
                        if (missionKey !== lastMissionSent || (now - lastTaskTime > TASK_DEBOUNCE)) {
                            if (missionKey !== lastMissionSent) {
                                lastTaskTime = now;
                                lastMissionSent = missionKey;
                                console.log(`[BattleTask] ✅ Gifter mission: ${gifterCount} users need to gift → unlock ${reward || '?'}`);
                                ws.send(JSON.stringify({
                                    type: 'battle_mission',
                                    missionType: 'gifter',
                                    gifterCount: gifterCount,
                                    reward: reward,
                                }));
                            }
                        }
                    } else if (multiplierMatch) {
                        // No gifter context — multiplier is actually ACTIVE
                        const num = parseInt(multiplierMatch[1]);
                        const multiplier = num === 2 ? 'double' : num === 3 ? 'triple' : null;
                        if (multiplier && multiplier !== lastMultiplierSent) {
                            lastTaskTime = now;
                            lastMultiplierSent = multiplier;
                            console.log(`[BattleTask] ✅ Multiplier ACTIVE: ${multiplier}`);
                            ws.send(JSON.stringify({
                                type: 'battle_multiplier',
                                multiplier: multiplier,
                            }));
                        }
                    }

                    // Pattern: "sum N" — accumulation challenge (e.g., ice challenge, need N total points)
                    const sumMatch = text.match(/sum\s+(\d+)/);
                    if (sumMatch) {
                        const sumTarget = sumMatch[1];
                        const missionKey = 'sum_' + sumTarget;
                        if (missionKey !== lastMissionSent) {
                            lastTaskTime = now;
                            lastMissionSent = missionKey;
                            console.log(`[BattleTask] ✅ Challenge target: ${sumTarget} points`);
                            ws.send(JSON.stringify({
                                type: 'battle_mission',
                                missionType: 'score',
                                score: sumTarget,
                            }));
                        }
                    }
                } catch (e) {
                    console.error(`[BattleTask] Parse error:`, e.message);
                }
            }
        });

        // Auto-reconnect on disconnect
        tiktokConnection.on('disconnected', () => {
            console.log('[TikTok] Disconnected from livestream');
            if (!manualDisconnect) {
                ws.send(JSON.stringify({ type: 'status', message: 'Connection lost. Attempting to reconnect...' }));
                scheduleReconnect(username);
            } else {
                ws.send(JSON.stringify({
                    type: 'error',
                    message: 'Disconnected from livestream'
                }));
            }
        });

        tiktokConnection.on('streamEnd', (actionId) => {
            console.log(`[TikTok] Stream ended (action: ${actionId})`);
            manualDisconnect = true; // Don't reconnect if stream ended
            ws.send(JSON.stringify({
                type: 'error',
                message: 'The livestream has ended.'
            }));
        });

        tiktokConnection.on('error', (err) => {
            // Safely extract error info (err can contain circular refs)
            const msg = (err.exception ? err.exception.message : err.message) || '';
            const info = err.info || '';
            const code = (err.exception && err.exception.code) || '';
            console.error(`[TikTok] Error: [${info}] ${msg} ${code}`);
            // Reconnect on websocket/network/SSL errors (not on "user not live" type errors)
            const fullErr = `${info} ${msg} ${code}`;
            if (!manualDisconnect && fullErr.match(/ECONNRESET|ETIMEDOUT|ECONNREFUSED|EPIPE|socket|websocket|network|SSL|bad record|tls/i)) {
                scheduleReconnect(username);
            }
        });
    }

    function scheduleReconnect(username) {
        // Prevent duplicate reconnect timers
        if (reconnectTimer) return;

        if (manualDisconnect || reconnectAttempts >= MAX_RECONNECT_ATTEMPTS) {
            ws.send(JSON.stringify({
                type: 'error',
                message: `Disconnected. All ${MAX_RECONNECT_ATTEMPTS} reconnect attempts failed. Click Connect to try again.`
            }));
            return;
        }
        const delay = RECONNECT_DELAYS[Math.min(reconnectAttempts, RECONNECT_DELAYS.length - 1)];
        reconnectAttempts++;
        console.log(`[TikTok] Reconnecting in ${delay / 1000}s... (attempt ${reconnectAttempts}/${MAX_RECONNECT_ATTEMPTS})`);
        ws.send(JSON.stringify({
            type: 'reconnecting',
            attempt: reconnectAttempts,
            maxAttempts: MAX_RECONNECT_ATTEMPTS,
            delaySec: Math.round(delay / 1000),
        }));
        reconnectTimer = setTimeout(() => {
            reconnectTimer = null;
            if (!manualDisconnect && ws.readyState === 1) {
                connectToTikTok(username);
            }
        }, delay);
    }

    console.log('[WS] Client connected');

    // Keep WebSocket alive with periodic pings (prevents Render.com idle timeout)
    const wsPingInterval = setInterval(() => {
        if (ws.readyState === 1) {
            ws.ping();
        }
    }, 30000); // every 30 seconds

    ws.on('pong', () => {
        // Client is alive
    });

    ws.on('message', (message) => {
        let data;
        try {
            data = JSON.parse(message);
        } catch {
            return;
        }

        if (data.type === 'connect') {
            currentUsername = data.username;
            manualDisconnect = false;
            reconnectAttempts = 0;
            usePollingFallback = false;
            disableGiftInfo = false;
            cachedRoomId = null;
            connectToTikTok(currentUsername);
        }

        if (data.type === 'disconnect') {
            manualDisconnect = true;
            if (reconnectTimer) { clearTimeout(reconnectTimer); reconnectTimer = null; }
            if (tiktokConnection) {
                tiktokConnection.disconnect();
                tiktokConnection = null;
                console.log('[TikTok] Manually disconnected');
            }
        }
    });

    ws.on('close', () => {
        console.log('[WS] Client disconnected');
        clearInterval(wsPingInterval);
        manualDisconnect = true;
        if (reconnectTimer) { clearTimeout(reconnectTimer); reconnectTimer = null; }
        if (tiktokConnection) {
            tiktokConnection.disconnect();
            tiktokConnection = null;
        }
    });
});

httpServer.listen(PORT, () => {
    console.log(`\n✅ TikTok Live Reader Server running!`);
    console.log(`📺 Open http://localhost:${PORT} in your browser\n`);

    // Test proxy on startup
    if (PROXY_URL) {
        console.log(`[Proxy] Testing proxy connection...`);
        const agent = new HttpsProxyAgent(PROXY_URL);
        https.get('https://api.ipify.org?format=json', { agent }, (res) => {
            let body = '';
            res.on('data', c => body += c);
            res.on('end', () => {
                console.log(`[Proxy] ✅ Proxy working! External IP: ${body}`);
            });
        }).on('error', (err) => {
            console.error(`[Proxy] ❌ Proxy test FAILED:`, err.message);
        });
    }
});

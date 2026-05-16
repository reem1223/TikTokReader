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
    const MAX_RECONNECT_ATTEMPTS = 10;
    const RECONNECT_DELAYS = [3000, 5000, 10000, 15000, 30000]; // escalating delays

    function connectToTikTok(username) {
        console.log(`[TikTok] Connecting to @${username}...${PROXY_URL ? ' (via proxy)' : ''}${reconnectAttempts > 0 ? ` (attempt ${reconnectAttempts + 1})` : ''}`);

        const options = {
            processInitialData: true,
            enableExtendedGiftInfo: true,
            enableWebsocketUpgrade: true,
            requestPollingIntervalMs: 2000,
            sessionId: null,
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

        tiktokConnection.connect()
            .then((state) => {
                reconnectAttempts = 0;
                console.log(`[TikTok] Connected to @${username} — Room: "${state.roomInfo?.title || 'Live'}"`);
                ws.send(JSON.stringify({
                    type: 'connected',
                    roomInfo: state.roomInfo?.title || 'Live'
                }));
            })
            .catch((err) => {
                console.error(`[TikTok] Connection failed:`, err.message);
                if (!manualDisconnect && reconnectAttempts < MAX_RECONNECT_ATTEMPTS) {
                    scheduleReconnect(username);
                } else {
                    ws.send(JSON.stringify({
                        type: 'error',
                        message: err.message || 'Failed to connect. Is the user live?'
                    }));
                }
            });

        // Chat messages
        let initialChatCount = 0;
        tiktokConnection.on('chat', (data) => {
            initialChatCount++;
            const payload = {
                type: 'chat',
                user: data.uniqueId || data.nickname || 'Anonymous',
                displayName: data.nickname || data.uniqueId || 'Anonymous',
                comment: data.comment,
                isInitial: initialChatCount <= 20,
            };
            console.log(`[Chat] @${payload.user}: ${payload.comment}`);
            ws.send(JSON.stringify(payload));
        });

        // After initial data is processed, mark subsequent messages as live
        tiktokConnection.on('websocketConnected', () => {
            initialChatCount = 999;
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
        let lastBattleEndTime = 0;
        let lastTaskTime = 0;
        let lastMultiplierSent = null;
        const BATTLE_COOLDOWN = 15000; // 15s cooldown after match end before allowing new match start
        const TASK_DEBOUNCE = 10000;   // 10s debounce between mission announcements

        tiktokConnection.on('linkMicBattle', (data) => {
            const hasBattleUsers = data.battleUsers && data.battleUsers.length > 0;
            const now = Date.now();

            // Only trigger match start if not already in battle AND cooldown passed
            if (hasBattleUsers && !battleActive && (now - lastBattleEndTime > BATTLE_COOLDOWN)) {
                battleActive = true;
                console.log(`[Battle] ✅ MATCH STARTED`);
                ws.send(JSON.stringify({ type: 'battle_start' }));
            }
        });

        tiktokConnection.on('linkMicArmies', (data) => {
            // Detect match end from battleStatus 2
            if (data.battleStatus === 2 && battleActive) {
                battleActive = false;
                lastBattleEndTime = Date.now();
                lastMultiplierSent = null;
                console.log(`[Battle] ❌ MATCH ENDED`);
                return;
            }

            // Suppress spam: only forward if armies have actual score data
            if (!data.battleArmies || data.battleArmies.length < 2) return;
            const armies = data.battleArmies;
            const scores = armies.map(a => a.points || 0);
            if (scores.every(s => s === 0)) return; // skip empty scores

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
                    console.log(`[BattleTask] Raw: ${text.substring(0, 300)}`);

                    // Parse "multi N" values from the task message
                    // Pattern: "pm_mt_live_match_instructions_1 multi 2" = double multiplier
                    // Pattern: "pm_mt_live_match_instructions_2 multi 750" = target score
                    const multiMatches = text.match(/multi\s+(\d+)/g);
                    if (multiMatches && multiMatches.length >= 1) {
                        let multiplier = null;
                        let targetScore = null;

                        for (const m of multiMatches) {
                            const num = parseInt(m.replace('multi', '').trim());
                            if (num === 2 || num === 3) {
                                multiplier = num === 2 ? 'double' : 'triple';
                            } else if (num >= 10) {
                                targetScore = num.toString();
                            }
                        }

                        // Send multiplier announcement (with debounce)
                        if (multiplier && multiplier !== lastMultiplierSent && (now - lastTaskTime > TASK_DEBOUNCE)) {
                            lastTaskTime = now;
                            lastMultiplierSent = multiplier;
                            console.log(`[BattleTask] ✅ Multiplier: ${multiplier}, Target: ${targetScore || '?'}`);
                            ws.send(JSON.stringify({
                                type: 'battle_multiplier',
                                multiplier: multiplier,
                                targetScore: targetScore,
                            }));
                        }

                        // Send mission announcement (with debounce, only if has target score)
                        if (targetScore && (now - lastTaskTime > TASK_DEBOUNCE / 2)) {
                            console.log(`[BattleTask] ✅ Mission target: ${targetScore}`);
                            ws.send(JSON.stringify({
                                type: 'battle_mission',
                                score: targetScore,
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
                scheduleReconnect(username);
            } else {
                ws.send(JSON.stringify({
                    type: 'error',
                    message: 'Disconnected from livestream'
                }));
            }
        });

        tiktokConnection.on('error', (err) => {
            console.error('[TikTok] Error:', err.message);
        });
    }

    function scheduleReconnect(username) {
        if (manualDisconnect || reconnectAttempts >= MAX_RECONNECT_ATTEMPTS) {
            ws.send(JSON.stringify({
                type: 'error',
                message: 'Disconnected. Max reconnect attempts reached.'
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
            if (!manualDisconnect && ws.readyState === 1) {
                connectToTikTok(username);
            }
        }, delay);
    }

    console.log('[WS] Client connected');

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

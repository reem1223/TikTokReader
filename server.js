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
        tiktokConnection.on('chat', (data) => {
            const payload = {
                type: 'chat',
                user: data.uniqueId || data.nickname || 'Anonymous',
                comment: data.comment
            };
            console.log(`[Chat] @${payload.user}: ${payload.comment}`);
            ws.send(JSON.stringify(payload));
        });

        // Gift events
        tiktokConnection.on('gift', (data) => {
            if (data.giftType === 1 && !data.repeatEnd) return;
            const payload = {
                type: 'gift',
                user: data.uniqueId || data.nickname || 'Anonymous',
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
            };
            console.log(`[Join] @${payload.user} joined`);
            ws.send(JSON.stringify(payload));
        });

        // Battle/Match events
        let battleActive = false;
        let battleTimerInterval = null;
        let lastBattleDuration = 0;
        let tenSecFired = false;

        tiktokConnection.on('linkMicBattle', (data) => {
            console.log(`[Battle] Status: ${data.battleStatus}, type: ${data.battleType || 'unknown'}`);
            // battleStatus: 1 = started, 2 = finished
            if (data.battleStatus === 1) {
                battleActive = true;
                tenSecFired = false;
                lastBattleDuration = data.duration || 120;
                ws.send(JSON.stringify({
                    type: 'battle_start',
                    duration: lastBattleDuration,
                }));

                // Start timer tracking for 10-second warning
                let remaining = lastBattleDuration;
                if (battleTimerInterval) clearInterval(battleTimerInterval);
                battleTimerInterval = setInterval(() => {
                    remaining--;
                    if (remaining === 10 && !tenSecFired) {
                        tenSecFired = true;
                        ws.send(JSON.stringify({ type: 'battle_timer_10' }));
                    }
                    if (remaining <= 0) {
                        clearInterval(battleTimerInterval);
                        battleTimerInterval = null;
                    }
                }, 1000);

            } else if (data.battleStatus === 2) {
                battleActive = false;
                if (battleTimerInterval) { clearInterval(battleTimerInterval); battleTimerInterval = null; }
            }
        });

        tiktokConnection.on('linkMicArmies', (data) => {
            if (!data.battleArmies || data.battleArmies.length < 2) return;

            const armies = data.battleArmies;
            const scores = armies.map(a => a.points || 0);
            console.log(`[Battle] Scores: ${scores.join(' vs ')}`);

            ws.send(JSON.stringify({
                type: 'battle_update',
                scores: scores,
                battleArmies: armies,
            }));
        });

        // Envelope events (double/triple score, missions)
        tiktokConnection.on('envelope', (data) => {
            console.log(`[Envelope]`, JSON.stringify(data).substring(0, 200));
            ws.send(JSON.stringify({
                type: 'envelope',
                data: data,
            }));
        });

        // Listen for rawData to catch mission/multiplier events
        tiktokConnection.on('roomUpdate', (data) => {
            console.log(`[RoomUpdate]`, JSON.stringify(data).substring(0, 200));
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

const { WebcastPushConnection } = require('tiktok-live-connector');
const { WebSocketServer } = require('ws');
const http = require('http');
const https = require('https');
const fs = require('fs');
const path = require('path');

const PORT = process.env.PORT || 3000;

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

    console.log('[WS] Client connected');

    ws.on('message', (message) => {
        let data;
        try {
            data = JSON.parse(message);
        } catch {
            return;
        }

        if (data.type === 'connect') {
            const username = data.username;
            console.log(`[TikTok] Connecting to @${username}...`);

            tiktokConnection = new WebcastPushConnection(username, {
                processInitialData: true,
                enableExtendedGiftInfo: true,
                enableWebsocketUpgrade: true,
                requestPollingIntervalMs: 2000,
                sessionId: null,
            });

            tiktokConnection.connect()
                .then((state) => {
                    console.log(`[TikTok] Connected to @${username} — Room: "${state.roomInfo?.title || 'Live'}"`);
                    ws.send(JSON.stringify({
                        type: 'connected',
                        roomInfo: state.roomInfo?.title || 'Live'
                    }));
                })
                .catch((err) => {
                    console.error(`[TikTok] Connection failed:`, err.message);
                    ws.send(JSON.stringify({
                        type: 'error',
                        message: err.message || 'Failed to connect. Is the user live?'
                    }));
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
                // Only process when gift streak is finished or non-streak gifts
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

            // Handle disconnection from TikTok
            tiktokConnection.on('disconnected', () => {
                console.log('[TikTok] Disconnected from livestream');
                ws.send(JSON.stringify({
                    type: 'error',
                    message: 'Disconnected from livestream'
                }));
            });

            tiktokConnection.on('error', (err) => {
                console.error('[TikTok] Error:', err.message);
            });
        }

        if (data.type === 'disconnect') {
            if (tiktokConnection) {
                tiktokConnection.disconnect();
                tiktokConnection = null;
                console.log('[TikTok] Manually disconnected');
            }
        }
    });

    ws.on('close', () => {
        console.log('[WS] Client disconnected');
        if (tiktokConnection) {
            tiktokConnection.disconnect();
            tiktokConnection = null;
        }
    });
});

httpServer.listen(PORT, () => {
    console.log(`\n✅ TikTok Live Reader Server running!`);
    console.log(`📺 Open http://localhost:${PORT} in your browser\n`);
});

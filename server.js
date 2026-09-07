const { TikTokLiveConnection, WebcastEvent, ControlEvent, SignConfig, UserOfflineError, SignatureRateLimitError } = require('tiktok-live-connector');
const { WebSocketServer } = require('ws');
const { HttpsProxyAgent } = require('https-proxy-agent');
const http = require('http');
const https = require('https');
const fs = require('fs');
const path = require('path');

const PORT = process.env.PORT || 3000;
const PROXY_URL = process.env.PROXY_URL || null; // e.g. http://user:pass@proxy.webshare.io:80
const SIGN_API_KEY = process.env.SIGN_API_KEY || null; // Free key from https://www.eulerstream.com (raises rate limits)

// TikTok requires signed request URLs. Signing is delegated to Euler Stream; without a key the
// service still works but is rate limited per-IP, which is unreliable on shared hosting.
if (SIGN_API_KEY) {
    SignConfig.apiKey = SIGN_API_KEY;
}

// Badge scene types used by TikTok to describe a user in chat payloads
const BADGE_SCENE_USER_GRADE = 8; // "gifter level" badge
const BADGE_SCENE_FAN_CLUB = 10; // fan club badge

// Reads a badge level out of a user's badge list, e.g. the level 26 grade badge
function getBadgeLevel(user, sceneType) {
    const badge = (user.badgeList || []).find((b) => b.sceneType === sceneType);
    const level = badge && badge.privilegeLogExtra && badge.privilegeLogExtra.level;
    return level ? parseInt(level, 10) || 0 : 0;
}

// Flattens the protobuf user object into the fields the frontend expects
function describeUser(data) {
    const user = data.user || {};
    const identity = data.userIdentity || {};
    const fanLevel = getBadgeLevel(user, BADGE_SCENE_FAN_CLUB);
    return {
        user: user.displayId || user.nickname || 'Anonymous',
        displayName: user.nickname || user.displayId || 'Anonymous',
        isMod: !!identity.isModeratorOfAnchor,
        isFollower: !!(identity.isFollowerOfAnchor || identity.isMutualFollowingWithAnchor),
        isFan: fanLevel > 0 || !!identity.isSubscriberOfAnchor,
        gifterLevel: getBadgeLevel(user, BADGE_SCENE_USER_GRADE),
    };
}

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
    let cachedRoomId = null; // Cache roomId to skip username lookup on reconnect
    const MAX_RECONNECT_ATTEMPTS = 20;
    const RECONNECT_DELAYS = [2000, 3000, 5000, 8000, 12000, 20000, 30000, 45000, 60000]; // escalating delays

    function send(payload) {
        if (ws.readyState === 1) {
            ws.send(JSON.stringify(payload));
        }
    }

    async function connectToTikTok(username) {
        // Clean up previous connection to avoid stale state
        if (tiktokConnection) {
            try { await tiktokConnection.disconnect(); } catch (e) {}
            tiktokConnection = null;
        }

        const usingCache = cachedRoomId ? ` [roomId:${cachedRoomId}]` : '';
        console.log(`[TikTok] Connecting to @${username}...${usingCache}${PROXY_URL ? ' (via proxy)' : ' (direct)'}${SIGN_API_KEY ? ' (with sign key)' : ' (anonymous signing)'}${reconnectAttempts > 0 ? ` (attempt ${reconnectAttempts + 1})` : ''}`);

        const options = {
            processInitialData: reconnectAttempts === 0, // Skip initial data on reconnects (stale)
            fetchRoomInfoOnConnect: true,
            enableExtendedGiftInfo: false,
        };

        if (SIGN_API_KEY) {
            options.signApiKey = SIGN_API_KEY;
        }

        if (PROXY_URL) {
            const agent = new HttpsProxyAgent(PROXY_URL);
            options.webClientOptions = { agent: { https: agent, http: agent } };
            options.wsClientOptions = { agent };
        }

        tiktokConnection = new TikTokLiveConnection(username, options);
        registerEventHandlers(tiktokConnection, username);

        try {
            // Pass cached roomId to skip the username lookup on reconnect
            const state = await tiktokConnection.connect(cachedRoomId || undefined);
            reconnectAttempts = 0;
            if (state.roomId) {
                cachedRoomId = state.roomId;
                console.log(`[TikTok] Cached roomId: ${cachedRoomId}`);
            }
            // Anonymous clients often get a stripped room info response, so fall back to a generic label
            const roomInfo = state.roomInfo || {};
            const title = roomInfo.title || (roomInfo.data && roomInfo.data.title) || 'Live';
            console.log(`[TikTok] ✅ Connected to @${username} — Room: "${title}"`);
            send({ type: 'connected', roomInfo: title });
        } catch (err) {
            handleConnectError(err, username);
        }
    }

    function handleConnectError(err, username) {
        const message = (err && err.message) || 'Unknown error';
        console.error(`[TikTok] Connection failed: ${message}`);

        // A stale cached roomId will keep failing — drop it so the next attempt re-resolves the username
        if (cachedRoomId) {
            console.log('[TikTok] Clearing cached roomId for next attempt');
            cachedRoomId = null;
        }

        if (err instanceof UserOfflineError) {
            // Not an error we can retry our way out of — the stream has to actually start
            manualDisconnect = true;
            send({ type: 'error', message: `@${username} is not live right now.` });
            return;
        }

        if (err instanceof SignatureRateLimitError) {
            const waitSec = Math.max(1, Math.round(err.retryAfter || 30));
            console.log(`[TikTok] Sign server rate limited — retrying in ${waitSec}s${SIGN_API_KEY ? '' : ' (set SIGN_API_KEY to raise the limit)'}`);
            send({ type: 'status', message: `Rate limited by the signing service. Retrying in ${waitSec}s...` });
            if (!manualDisconnect) {
                scheduleReconnect(username, waitSec * 1000);
            }
            return;
        }

        if (!manualDisconnect && reconnectAttempts < MAX_RECONNECT_ATTEMPTS) {
            scheduleReconnect(username);
        } else {
            send({ type: 'error', message: message || 'Failed to connect. Is the user live?' });
        }
    }

    function registerEventHandlers(connection, username) {
        // Chat messages
        connection.on(WebcastEvent.CHAT, (data) => {
            const payload = { type: 'chat', ...describeUser(data), comment: data.content };
            const tags = [
                payload.isMod ? '[MOD]' : '',
                payload.isFollower ? '[FOL]' : '',
                payload.isFan ? '[FAN]' : '',
                payload.gifterLevel ? `[GL:${payload.gifterLevel}]` : '',
            ].filter(Boolean).join(' ');
            console.log(`[Chat] @${payload.user}${tags ? ' ' + tags : ''}: ${payload.comment}`);
            send(payload);
        });

        // Gift events — for streakable gifts wait for the streak to end so we count it once
        connection.on(WebcastEvent.GIFT, (data) => {
            const gift = data.gift || {};
            if (gift.type === 1 && !data.repeatEnd) return;
            const image = gift.image && gift.image.urlList && gift.image.urlList[0];
            const payload = {
                type: 'gift',
                ...describeUser(data),
                giftName: gift.name || 'gift',
                giftCount: data.repeatCount || 1,
                diamondCount: gift.diamondCount || 0,
                giftPictureUrl: image || '',
            };
            console.log(`[Gift] @${payload.user} sent ${payload.giftCount}x ${payload.giftName}`);
            send(payload);
        });

        // Member join events
        connection.on(WebcastEvent.MEMBER, (data) => {
            const payload = { type: 'member', ...describeUser(data) };
            console.log(`[Join] @${payload.user} joined`);
            send(payload);
        });

        // Auto-reconnect on disconnect
        connection.on(ControlEvent.DISCONNECTED, () => {
            console.log('[TikTok] Disconnected from livestream');
            if (!manualDisconnect) {
                send({ type: 'status', message: 'Connection lost. Attempting to reconnect...' });
                scheduleReconnect(username);
            } else {
                send({ type: 'error', message: 'Disconnected from livestream' });
            }
        });

        connection.on(WebcastEvent.STREAM_END, (event) => {
            console.log(`[TikTok] Stream ended (action: ${event && event.action})`);
            manualDisconnect = true; // Don't reconnect if stream ended
            send({ type: 'error', message: 'The livestream has ended.' });
        });

        connection.on(ControlEvent.ERROR, (err) => {
            const message = (err && err.message) || 'Unknown error';
            console.error(`[TikTok] Error: ${message}`);
            // Reconnect on network-level errors; ignore decode noise from unknown message types
            if (!manualDisconnect && /ECONNRESET|ETIMEDOUT|ECONNREFUSED|EPIPE|socket|websocket|network|SSL|bad record|tls/i.test(message)) {
                scheduleReconnect(username);
            }
        });
    }

    function scheduleReconnect(username, overrideDelay) {
        // Prevent duplicate reconnect timers
        if (reconnectTimer) return;

        if (manualDisconnect || reconnectAttempts >= MAX_RECONNECT_ATTEMPTS) {
            send({ type: 'error', message: `Disconnected. All ${MAX_RECONNECT_ATTEMPTS} reconnect attempts failed. Click Connect to try again.` });
            return;
        }
        const delay = overrideDelay || RECONNECT_DELAYS[Math.min(reconnectAttempts, RECONNECT_DELAYS.length - 1)];
        reconnectAttempts++;
        console.log(`[TikTok] Reconnecting in ${delay / 1000}s... (attempt ${reconnectAttempts}/${MAX_RECONNECT_ATTEMPTS})`);
        send({
            type: 'reconnecting',
            attempt: reconnectAttempts,
            maxAttempts: MAX_RECONNECT_ATTEMPTS,
            delaySec: Math.round(delay / 1000),
        });
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

    if (!SIGN_API_KEY) {
        console.log('[Sign] No SIGN_API_KEY set — using anonymous signing (per-IP rate limits apply).');
        console.log('[Sign] Get a free key at https://www.eulerstream.com and set SIGN_API_KEY to raise them.\n');
    }

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

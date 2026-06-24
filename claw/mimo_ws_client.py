#!/usr/bin/env python3
"""
MiMo Studio - Pure HTTP/WS Chat Client
No browser/Playwright needed. Just cookies + websockets.

Usage:
  python3 mimo_ws_client.py "your message here"
  echo "your message" | python3 mimo_ws_client.py
"""
import asyncio, json, uuid, sys, os
import subprocess, urllib.parse

COOKIES_PATH = '/tmp/mimo_cookies.json'

def load_cookies():
    with open(COOKIES_PATH) as f:
        cookies = json.load(f)
    cookie_str = '; '.join(f'{c["name"]}={c["value"]}' for c in cookies)
    ph_val = next((c['value'] for c in cookies if c['name'] == 'xiaomichatbot_ph'), '')
    if ph_val.startswith('"') and ph_val.endswith('"'):
        ph_val = ph_val[1:-1]
    return cookie_str, ph_val

COOKIE_STR, PH_VALUE = load_cookies()
BASE = 'https://aistudio.xiaomimimo.com'
PH = f'xiaomichatbot_ph={urllib.parse.quote(PH_VALUE, safe="")}'


def api(method, path, body=None, with_ph=False):
    """HTTP API call via curl (avoids Python requests cookie issues)."""
    url = f'{BASE}{path}'
    if with_ph:
        sep = '&' if '?' in url else '?'
        url += f'{sep}{PH}'
    cmd = ['curl', '-s', '-X', method, url,
        '-H', f'Cookie: {COOKIE_STR}',
        '-H', 'Content-Type: application/json',
        '-H', 'Accept-Language: zh-CN',
        '-H', 'x-timeZone: Asia/Shanghai',
    ]
    if body:
        cmd += ['-d', json.dumps(body)]
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    try:
        return json.loads(r.stdout)
    except:
        return {'raw': r.stdout, 'exit': r.returncode}


async def mimo_chat(message, on_delta=None, on_event=None):
    """
    Send a message to MiMo Claw via WebSocket and return the full response.
    
    Args:
        message: Text message to send
        on_delta: Optional callback(text) for streaming deltas
        on_event: Optional callback(event, payload) for all events
    
    Returns:
        (full_text, error) tuple
    """
    import websockets
    
    # Ensure Claw is available
    d = api('GET', '/open-apis/user/mimo-claw/status')
    status = d.get('data', {}).get('status')
    if status != 'AVAILABLE':
        d = api('POST', '/open-apis/user/mimo-claw/create', {}, with_ph=True)
        if d.get('data', {}).get('status') != 'AVAILABLE':
            return '', f'Claw not available: {status}'
    
    # Get WS ticket + userId
    d = api('GET', '/open-apis/user/ws/ticket', with_ph=True)
    ticket = d.get('data', {}).get('ticket')
    d = api('GET', '/open-apis/user/mi/get')
    userId = d.get('data', {}).get('userId')
    
    if not ticket or not userId:
        return '', f'Failed to get ticket/userId'
    
    ws_url = f'wss://aistudio.xiaomimimo.com/ws/proxy?ticket={ticket}&userId={userId}'
    
    async with websockets.connect(ws_url, ping_interval=30, ping_timeout=10) as ws:
        # Wait for challenge
        await asyncio.wait_for(ws.recv(), timeout=10)
        
        # Connect
        req_id = str(uuid.uuid4())
        await ws.send(json.dumps({
            "type": "req", "id": req_id, "method": "connect",
            "params": {
                "minProtocol": 3, "maxProtocol": 4,
                "client": {"id": "cli", "version": "mimo-ws-client", "platform": "Linux", "mode": "cli"},
                "role": "operator",
                "scopes": ["operator.admin", "operator.read", "operator.write", "operator.approvals", "operator.pairing"],
                "caps": ["tool-events"], "userAgent": "Mozilla/5.0", "locale": "zh-CN"
            }
        }))
        
        while True:
            msg = await asyncio.wait_for(ws.recv(), timeout=10)
            d = json.loads(msg)
            if d.get('id') == req_id:
                if not d.get('ok'):
                    return '', f'Connect failed'
                break
        
        # Send chat message
        # Protocol 4 (openclaw 2026.5.27) renamed the default session key from
        # "agent:main:default" to "agent:main:main" (confirmed via live capture).
        session_key = "agent:main:main"
        msg_id = str(uuid.uuid4())
        await ws.send(json.dumps({
            "type": "req", "id": msg_id, "method": "chat.send",
            "params": {
                "sessionKey": session_key,
                "message": message,
                "deliver": False,
                "idempotencyKey": msg_id
            }
        }))
        
        # Collect response
        full_text = ""
        for _ in range(500):
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=120)
                data = json.loads(raw)
                
                if data.get('type') == 'res':
                    if data.get('id') == msg_id and not data.get('ok'):
                        return '', f'chat.send error: {data}'
                    continue
                
                if data.get('type') != 'event':
                    continue
                
                event = data.get('event', '')
                payload = data.get('payload', {})
                
                if event in ('health', 'tick'):
                    # protocol-4 transport heartbeats — ignore
                    continue
                
                if on_event:
                    on_event(event, payload)
                
                # agent events with assistant stream = text deltas
                if event == 'agent' and payload.get('stream') == 'assistant':
                    delta = payload.get('data', {}).get('delta', '')
                    if delta:
                        full_text += delta
                        if on_delta:
                            on_delta(delta)
                
                # chat event with state=final = done
                if event == 'chat' and payload.get('state') == 'final':
                    # Prefer text from final event
                    msg_content = payload.get('message', {})
                    if msg_content.get('content'):
                        for block in msg_content['content']:
                            if block.get('type') == 'text':
                                final_text = block.get('text', '')
                                if final_text and len(final_text) >= len(full_text):
                                    full_text = final_text
                    break
                
            except asyncio.TimeoutError:
                break
        
        return full_text, None


def main():
    if len(sys.argv) > 1:
        message = ' '.join(sys.argv[1:])
    elif not sys.stdin.isatty():
        message = sys.stdin.read().strip()
    else:
        print("Usage: python3 mimo_ws_client.py 'your message'")
        sys.exit(1)
    
    # Streaming output
    def on_delta(d):
        print(d, end='', flush=True)
    
    text, err = asyncio.run(mimo_chat(message, on_delta=on_delta))
    
    if err:
        print(f"\nError: {err}", file=sys.stderr)
        sys.exit(1)
    
    if not text.strip():
        print("\n(No response)")
    
    print()  # newline after streaming


if __name__ == '__main__':
    main()

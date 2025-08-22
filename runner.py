# runner.py — ультра-мінімальний
import os, time, threading, sys
from http.server import BaseHTTPRequestHandler, HTTPServer

PORT = int(os.getenv("PORT", "8080"))

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/health":
            self.send_response(200); self.end_headers(); self.wfile.write(b"OK")
        else:
            self.send_response(404); self.end_headers()
    def log_message(self, *_):
        return

def start_health_server():
    srv = HTTPServer(("", PORT), Handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    print(f"[sanity] health server on :{PORT} (/health)", flush=True)

if __name__ == "__main__":
    print("[sanity] starting...", flush=True)
    start_health_server()
    i = 0
    while True:
        print(f"[sanity] tick {i}", flush=True)
        time.sleep(15)
        i += 1

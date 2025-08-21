import os, time, threading, traceback, sys
from http.server import BaseHTTPRequestHandler, HTTPServer
from bitrix24_whisper_telegram_monitor_v2 import process

POLL_INTERVAL = int(os.getenv("POLL_INTERVAL_SECONDS", "180"))
PORT = int(os.getenv("PORT", "8080"))

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/health":
            self.send_response(200); self.end_headers(); self.wfile.write(b"OK")
        else:
            self.send_response(404); self.end_headers()
    def log_message(self, *_):  # тихий http.server
        return

def start_health_server():
    srv = HTTPServer(("", PORT), Handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    print(f"[runner] health server on :{PORT} (/health)", flush=True)

if __name__ == "__main__":
    print("[runner] starting…", flush=True)
    start_health_server()
    while True:
        try:
            print("[runner] tick -> process()", flush=True)
            process()
            print("[runner] done, sleep", flush=True)
        except Exception:
            traceback.print_exc(); sys.stdout.flush()
        time.sleep(POLL_INTERVAL)

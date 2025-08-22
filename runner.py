import os, time, threading, traceback, sys, importlib
from http.server import BaseHTTPRequestHandler, HTTPServer

MODULE_NAME = "bitrix24_monitor_rt"  # якщо файл з процесом названий інакше — замініть тут
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL_SECONDS", "60"))
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
    print(f"[runner] health server on :{PORT} (/health)", flush=True)

def load_process():
    try:
        m = importlib.import_module(MODULE_NAME)
        print(f"[runner] imported {MODULE_NAME}", flush=True)
        return getattr(m, "process", None)
    except Exception:
        print("[runner] import failed:", flush=True)
        traceback.print_exc(); sys.stdout.flush()
        return None

if __name__ == "__main__":
    print("[runner] starting…", flush=True)
    start_health_server()
    while True:
        try:
            print("[runner] tick -> import & process()", flush=True)
            process = load_process()
            if process:
                process()
                print("[runner] done, sleep", flush=True)
            else:
                print("[runner] no process(); will retry", flush=True)
        except Exception:
            traceback.print_exc(); sys.stdout.flush()
        time.sleep(POLL_INTERVAL)

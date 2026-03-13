import os, json, logging
from datetime import datetime, timezone, timedelta
from http.server import HTTPServer, BaseHTTPRequestHandler

MSK = timezone(timedelta(hours=3))
LOG = []  # in-memory buffer, last 500 events

logging.basicConfig(level=logging.INFO)

class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length)
        ts = datetime.now(tz=MSK).strftime("%Y-%m-%d %H:%M:%S")
        try:
            data = json.loads(body)
        except Exception:
            data = {"raw": body.decode(errors="replace")}

        entry = {"ts": ts, "path": self.path, "data": data}
        LOG.append(entry)
        if len(LOG) > 500:
            LOG.pop(0)

        logging.info(f"[{ts}] webhook: {json.dumps(data, ensure_ascii=False)[:200]}")

        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b'{"ok":true}')

    def do_GET(self):
        if self.path == "/events":
            body = json.dumps(LOG[-50:], ensure_ascii=False, indent=2).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(body)
        else:
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"Salambot webhook receiver OK")

    def log_message(self, *args):
        pass

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8765))
    logging.info(f"Starting on port {port}")
    HTTPServer(("0.0.0.0", port), Handler).serve_forever()

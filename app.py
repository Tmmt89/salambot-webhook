import os, json, base64, logging, tempfile
from datetime import datetime, timezone, timedelta
from http.server import HTTPServer, BaseHTTPRequestHandler
from google.oauth2 import service_account
from googleapiclient.discovery import build

MSK = timezone(timedelta(hours=3))
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

SPREADSHEET_ID = os.environ["SPREADSHEET_ID"]
SA_B64 = os.environ["GOOGLE_SA_B64"]

# Init Sheets client from base64-encoded service account
def get_sheets():
    sa_json = base64.b64decode(SA_B64).decode()
    sa_info = json.loads(sa_json)
    creds = service_account.Credentials.from_service_account_info(
        sa_info, scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    return build("sheets", "v4", credentials=creds)

sheets_client = get_sheets()

def append_rows(sheet_name, rows):
    try:
        sheets_client.spreadsheets().values().append(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{sheet_name}!A1",
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": rows}
        ).execute()
    except Exception as e:
        logging.error(f"Sheets append error: {e}")

CHANNELS = {
    "031dfe75-177e-4d48-ac8f-eec06f5af645": ("WhatsApp", "79389044222"),
    "37052acb-9f82-45a5-a99b-4ac1e22852d5": ("Instagram", "rise.kids_"),
    "4416859f-ac47-46de-aa99-8f04a715ae4e": ("Instagram", "rise_arabic"),
    "83897fd7-b0b0-4c22-9768-154242d72926": ("Instagram", "rise_english"),
}

class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length)
        ts = datetime.now(tz=MSK).strftime("%Y-%m-%d %H:%M:%S")

        try:
            data = json.loads(body)
        except Exception:
            data = {"raw": body.decode(errors="replace")}

        logging.info(f"Webhook: {json.dumps(data, ensure_ascii=False)[:300]}")

        # Parse messages
        messages = data.get("messages", [])
        if messages:
            rows = []
            for m in messages:
                ch_id = m.get("channelId", "")
                ch_type, ch_name = CHANNELS.get(ch_id, ("Unknown", ch_id[:8]))
                direction = "→ Входящее" if m.get("direction") == "inbound" else "← Исходящее"
                contact = m.get("contactName") or m.get("contactId", "")
                phone = m.get("chatId", "")
                text = m.get("text") or m.get("caption") or f"[{m.get('type','media')}]"
                status = m.get("status", "")
                chat_id = m.get("chatId", "")
                msg_id = m.get("messageId", "")
                rows.append([ts, direction, ch_name, ch_type, contact, phone, text, status, chat_id, msg_id])
            append_rows("Сообщения", rows)

        # Parse statuses
        statuses = data.get("statuses", [])
        if statuses:
            rows = [[ts, "status_update", s.get("chatId",""), json.dumps(s, ensure_ascii=False)] for s in statuses]
            append_rows("События", rows)

        # Parse contacts/deals
        for key in ["contacts", "deals"]:
            items = data.get(key, [])
            if items:
                rows = [[ts, key, "", json.dumps(i, ensure_ascii=False)] for i in items]
                append_rows("События", rows)

        # Unknown events
        known = {"messages", "statuses", "contacts", "deals", "test"}
        for k, v in data.items():
            if k not in known:
                append_rows("События", [[ts, k, "", json.dumps({k: v}, ensure_ascii=False)]])

        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b'{"ok":true}')

    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"Salambot webhook receiver OK")

    def log_message(self, *args):
        pass

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8765))
    logging.info(f"Starting on port {port}, sheet: {SPREADSHEET_ID}")
    HTTPServer(("0.0.0.0", port), Handler).serve_forever()

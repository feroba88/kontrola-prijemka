import base64
import json
import io
import os
import re
import unicodedata
import traceback
from datetime import datetime
from typing import Optional, Tuple, Dict, Any, List

import pandas as pd

from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from google.oauth2 import service_account
from googleapiclient.discovery import build


# ================== KONFIGURÁCIA ==================

SENDER_EMAIL = "preposli@preposlimail.com"
RECIPIENTS = {
    "703": "frantisek.bacik@gmail.com",
    "704": "miroslava.frey@gmail.com",
    "714": "frantisek.bacik@gmail.com",
    "911": "bacikivan@gmail.com",
}
FALLBACK_EMAIL = "frantisek.bacik@gmail.com"

# Gmail API
GMAIL_SA_FILE = os.getenv("GMAIL_SA_FILE", "gmail-sa-key.json")
BASE_GMAIL_SCOPES = [
    "https://www.googleapis.com/auth/gmail.modify",
    "https://www.googleapis.com/auth/gmail.send",
]

INTERNAL_LABEL_NAME = os.getenv("KONTROLA_INTERNAL_LABEL", "_kontrola_prijemky_auto")
PROCESSED_LABEL_NAME = os.getenv("KONTROLA_PROCESSED_LABEL", "kontrola_spracovane")
FAILED_LABEL_NAME = os.getenv("KONTROLA_FAILED_LABEL", "kontrola_failed")
# Centralized architecture: this service is subscriber-only by default.
KONTROLA_ENABLE_WATCH_RENEW = os.getenv("KONTROLA_ENABLE_WATCH_RENEW", "0") == "1"

# Excluded IDs (podľa tvojich pravidiel)
EXCLUDED_IDS = {"506875", "506874", "503179"}

# ID pravidlo
ID_LEN = 6

# Výstup - názvy hárokov
SHEET_ORDERED_NOT_RECEIVED = "OBJEDNANE_NEPRISLO"
SHEET_NOT_ORDERED = "NEOBJEDNANE_PRISLO"
SHEET_ORDERED_AND_RECEIVED = "OBJEDNANE_AJ_PRISLO"

ATTACHMENT_EXTENSIONS = (".xls", ".xlsx")

STATION_SUBJECT_RE = re.compile(r"([0-9]{3})")


# ================== AUTH ==================

def get_gmail_service():
    key_path = resolve_gmail_sa_path()
    creds = service_account.Credentials.from_service_account_file(
        key_path,
        scopes=BASE_GMAIL_SCOPES,
        subject=SENDER_EMAIL,
    )
    return build("gmail", "v1", credentials=creds, cache_discovery=False)


def resolve_gmail_sa_path() -> str:
    module_dir = os.path.dirname(__file__)
    repo_root = os.path.dirname(module_dir)
    candidates = [
        GMAIL_SA_FILE,
        os.path.join(repo_root, "gmail-sa-key.json"),
        os.path.join(module_dir, "gmail-sa-key.json"),
        "gmail-sa-key.json",
    ]

    seen = set()
    normalized_candidates = []
    for candidate in candidates:
        if not candidate:
            continue
        normalized = os.path.abspath(candidate)
        if normalized in seen:
            continue
        seen.add(normalized)
        normalized_candidates.append(normalized)
        if os.path.exists(normalized):
            return normalized

    searched = ", ".join(normalized_candidates)
    raise FileNotFoundError(f"Gmail SA key not found. Searched: {searched}")


# ================== GMAIL HELPERS ==================

def decode_b64url(data: str) -> bytes:
    padded = data + ("=" * (-len(data) % 4))
    return base64.urlsafe_b64decode(padded.encode("utf-8"))


def parse_pubsub_notification(request):
    event = request.get_json(silent=True)
    if not isinstance(event, dict):
        return None, None

    message = event.get("message", {})
    if not isinstance(message, dict):
        return None, None

    data_b64 = message.get("data")
    if not data_b64:
        return None, None

    try:
        decoded = decode_b64url(data_b64).decode("utf-8")
        payload = json.loads(decoded)
        email_address = payload.get("emailAddress")
        history_id = payload.get("historyId")
        if email_address and history_id:
            return email_address, str(history_id)
    except Exception:
        pass

    return None, None


def build_candidate_query() -> str:
    return (
        f"label:{INTERNAL_LABEL_NAME} "
        f"-label:{PROCESSED_LABEL_NAME} "
        f"-label:{FAILED_LABEL_NAME} "
        f"has:attachment -from:{SENDER_EMAIL} "
        f"is:unread"
    )


def list_candidate_message_ids(service):
    query = build_candidate_query()
    ids = []
    page_token = None

    while True:
        resp = service.users().messages().list(
            userId="me",
            q=query,
            maxResults=50,
            pageToken=page_token,
        ).execute()
        ids.extend([m["id"] for m in resp.get("messages", []) if m.get("id")])
        page_token = resp.get("nextPageToken")
        if not page_token:
            break

    return ids


def get_label_id(service, label_name: str) -> str:
    labels = service.users().labels().list(userId="me").execute().get("labels", [])
    for label in labels:
        if label.get("name") == label_name:
            return label["id"]
    raise RuntimeError(f"Štítok '{label_name}' nebol nájdený.")


def iter_parts(payload):
    stack = [payload]
    while stack:
        part = stack.pop()
        yield part
        for child in part.get("parts", []) or []:
            stack.append(child)


def extract_excel_attachments(service, message):
    message_id = message["id"]
    payload = message.get("payload", {})
    result = []

    for part in iter_parts(payload):
        filename = (part.get("filename") or "").strip()
        if not filename.lower().endswith(ATTACHMENT_EXTENSIONS):
            continue

        body = part.get("body", {}) or {}
        data = body.get("data")
        attachment_id = body.get("attachmentId")

        if data:
            file_bytes = decode_b64url(data)
        elif attachment_id:
            att = service.users().messages().attachments().get(
                userId="me",
                messageId=message_id,
                id=attachment_id,
            ).execute()
            if not att.get("data"):
                continue
            file_bytes = decode_b64url(att["data"])
        else:
            continue

        result.append((filename, file_bytes))

    return result


def get_header(payload, name: str) -> str:
    headers = payload.get("headers", []) or []
    target = name.lower()
    for hdr in headers:
        if (hdr.get("name") or "").lower() == target:
            return hdr.get("value", "")
    return ""


def extract_station_from_subject(subject: str) -> str:
    if not subject:
        return ""
    match = STATION_SUBJECT_RE.search(subject)
    if not match:
        return ""
    return match.group(1)


def mark_message_processed(service, message_id: str, processed_label_id: str, internal_label_id: str):
    service.users().messages().modify(
        userId="me",
        id=message_id,
        body={"addLabelIds": [processed_label_id], "removeLabelIds": ["UNREAD", internal_label_id]},
    ).execute()


def mark_message_failed(service, message_id: str, failed_label_id: str, internal_label_id: str):
    service.users().messages().modify(
        userId="me",
        id=message_id,
        body={"addLabelIds": [failed_label_id], "removeLabelIds": [internal_label_id]},
    ).execute()


def send_email_via_gmail_api(service, recipient: str, subject: str, body_text: str, attachment_name: str, attachment_bytes: bytes):
    msg = MIMEMultipart()
    msg["From"] = SENDER_EMAIL
    msg["To"] = recipient
    msg["Subject"] = subject

    msg.attach(MIMEText(body_text, "plain"))

    part = MIMEApplication(attachment_bytes, _subtype="xlsx")
    part.add_header("Content-Disposition", "attachment", filename=attachment_name)
    msg.attach(part)

    raw_message = base64.urlsafe_b64encode(msg.as_bytes()).decode("utf-8")

    service.users().messages().send(userId="me", body={"raw": raw_message}).execute()
    print(f"✅ Email odoslaný na: {recipient}")


# ================== TEXT / NORMALIZE HELPERS ==================

def strip_diacritics(s: str) -> str:
    if s is None:
        return ""
    s = str(s)
    s = unicodedata.normalize("NFKD", s)
    return "".join(ch for ch in s if not unicodedata.combining(ch))


def norm_key(s: str) -> str:
    return strip_diacritics(str(s)).lower().strip()


def extract_first_int_as_str(value: Any) -> str:
    if value is None:
        return "0"
    return str(value).strip()


def extract_first_int(value: Any) -> int:
    if value is None:
        return 0
    s = str(value)
    m = re.search(r"(\d+)", s)
    return int(m.group(1)) if m else 0


def valid_id(id_value: Any) -> str:
    if id_value is None:
        return ""
    return str(id_value).strip()


# ================== EXCEL LOAD & PROCESS ==================

def _engine_for_path(path: str) -> Optional[str]:
    p = (path or "").lower()
    if p.endswith(".xlsx"):
        return "openpyxl"
    if p.endswith(".xls"):
        return "xlrd"
    return None


def load_order_excel(file_bytes: bytes, filename: str) -> Dict[str, Dict[str, Any]]:
    engine = _engine_for_path(filename)
    df = pd.read_excel(
        io.BytesIO(file_bytes),
        header=None,
        skiprows=1,
        usecols=[0, 1, 2],
        engine=engine,
    )
    df.columns = ["ID", "NAME", "QTY"]

    out: Dict[str, Dict[str, Any]] = {}
    for _, row in df.iterrows():
        id_ = valid_id(row["ID"])
        if not id_:
            continue
        if len(id_) != ID_LEN:
            continue
        if id_ in EXCLUDED_IDS:
            continue

        name = str(row["NAME"]).strip() if row["NAME"] is not None else ""
        qty_raw = extract_first_int_as_str(row["QTY"])
        qty_int = extract_first_int(row["QTY"])

        out[id_] = {
            "id": id_,
            "name": name,
            "qty_raw": qty_raw,
            "qty_int": qty_int,
        }
    return out


def load_receipt_excel(file_bytes: bytes, filename: str) -> Dict[str, Dict[str, Any]]:
    engine = _engine_for_path(filename)
    df = pd.read_excel(
        io.BytesIO(file_bytes),
        header=None,
        skiprows=1,
        usecols=[0, 1, 3],
        engine=engine,
    )
    df.columns = ["ID", "NAME", "QTY"]

    out: Dict[str, Dict[str, Any]] = {}
    for _, row in df.iterrows():
        id_ = valid_id(row["ID"])
        if not id_:
            continue
        if len(id_) != ID_LEN:
            continue
        if id_ in EXCLUDED_IDS:
            continue

        name = str(row["NAME"]).strip() if row["NAME"] is not None else ""
        qty_raw = extract_first_int_as_str(row["QTY"])
        qty_int = extract_first_int(row["QTY"])

        out[id_] = {
            "id": id_,
            "name": name,
            "qty_raw": qty_raw,
            "qty_int": qty_int,
        }
    return out


def compare(order_data: Dict[str, Dict[str, Any]],
            receipt_data: Dict[str, Dict[str, Any]]) -> Tuple[List[Dict[str, Any]],
                                                              List[Dict[str, Any]],
                                                              List[Dict[str, Any]]]:
    ordered_not_received: List[Dict[str, Any]] = []
    not_ordered: List[Dict[str, Any]] = []
    ordered_and_received: List[Dict[str, Any]] = []

    for id_, o in order_data.items():
        if id_ not in receipt_data:
            ordered_not_received.append({
                "ID": id_,
                "Názov": o.get("name", ""),
                "Množstvo z objednávky": o.get("qty_raw", ""),
                "Množstvo z príjemky": "",
            })

    for id_, r in receipt_data.items():
        if id_ not in order_data:
            not_ordered.append({
                "ID": id_,
                "Názov": r.get("name", ""),
                "Množstvo z objednávky": "",
                "Množstvo z príjemky": r.get("qty_raw", ""),
            })

    for id_, o in order_data.items():
        if id_ in receipt_data:
            r = receipt_data[id_]
            name = o.get("name", "") or r.get("name", "")
            ordered_and_received.append({
                "ID": id_,
                "Názov": name,
                "Množstvo z objednávky": o.get("qty_raw", ""),
                "Množstvo z príjemky": r.get("qty_raw", ""),
            })

    def sort_key(item: Dict[str, Any]) -> str:
        return norm_key(item.get("Názov", ""))

    ordered_not_received.sort(key=sort_key)
    not_ordered.sort(key=sort_key)
    ordered_and_received.sort(key=sort_key)

    return ordered_not_received, not_ordered, ordered_and_received


def build_report_xlsx(ordered_not_received: List[Dict[str, Any]],
                      not_ordered: List[Dict[str, Any]],
                      ordered_and_received: List[Dict[str, Any]]) -> bytes:
    buf = io.BytesIO()

    df1 = pd.DataFrame(ordered_not_received, columns=["ID", "Názov", "Množstvo z objednávky", "Množstvo z príjemky"])
    df2 = pd.DataFrame(not_ordered, columns=["ID", "Názov", "Množstvo z objednávky", "Množstvo z príjemky"])
    df3 = pd.DataFrame(ordered_and_received, columns=["ID", "Názov", "Množstvo z objednávky", "Množstvo z príjemky"])

    with pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
        df1.to_excel(writer, sheet_name=SHEET_ORDERED_NOT_RECEIVED, index=False)
        df2.to_excel(writer, sheet_name=SHEET_NOT_ORDERED, index=False)
        df3.to_excel(writer, sheet_name=SHEET_ORDERED_AND_RECEIVED, index=False)

    buf.seek(0)
    return buf.getvalue()


# ================== HLAVNÁ FUNKCIA ==================

def process_single_message(service, message_id: str, label_ids: Dict[str, str]):
    message = service.users().messages().get(userId="me", id=message_id, format="full").execute()
    payload = message.get("payload", {})
    subject = get_header(payload, "Subject")
    station = extract_station_from_subject(subject)

    if not station or station not in RECIPIENTS:
        print(f"SKIP SUBJECT_STATION_INVALID message={message_id} subject={subject}")
        mark_message_failed(service, message_id, label_ids["failed"], label_ids["internal"])
        return False

    attachments = extract_excel_attachments(service, message)
    if len(attachments) < 2:
        print(f"SKIP EXPECT_2_ATTACHMENTS message={message_id} subject={subject} length={len(attachments)}")
        mark_message_failed(service, message_id, label_ids["failed"], label_ids["internal"])
        return False

    # Nájdeme ktorý súbor je objednávka a ktorý príjemka podľa názvu v prilohe
    order_file, receipt_file = None, None
    for filename, filebytes in attachments:
        fn_lower = strip_diacritics(filename).lower()
        if "obj" in fn_lower:
            order_file = (filename, filebytes)
        elif "prijem" in fn_lower:
            receipt_file = (filename, filebytes)
            
    if not order_file and not receipt_file:
        # Fallback (prvá je objednávka, druhá je príjemka?) - ale radšej nie
        print(f"SKIP CANNOT_IDENTIFY_FILES message={message_id}")
        mark_message_failed(service, message_id, label_ids["failed"], label_ids["internal"])
        return False
        
    if not order_file:
         # Ak nenašlo obj., ale len prijemku, zoberie druhé ako obj
         print(f"WARING: could not find 'obj' in filename, guessing...")
         order_file = next(a for a in attachments if a[0] != receipt_file[0])
    if not receipt_file:
         print(f"WARING: could not find 'prijem' in filename, guessing...")
         receipt_file = next(a for a in attachments if a[0] != order_file[0])

    try:
        order_data = load_order_excel(order_file[1], filename=order_file[0])
        receipt_data = load_receipt_excel(receipt_file[1], filename=receipt_file[0])
    except Exception as e:
        print("FAIL EXCEL_PARSE_FAIL:", type(e).__name__, str(e))
        print(traceback.format_exc())
        mark_message_failed(service, message_id, label_ids["failed"], label_ids["internal"])
        return False

    try:
        ordered_not_received, not_ordered, ordered_and_received = compare(order_data, receipt_data)
    except Exception as e:
        print("FAIL COMPARE_FAIL:", type(e).__name__, str(e))
        print(traceback.format_exc())
        mark_message_failed(service, message_id, label_ids["failed"], label_ids["internal"])
        return False

    try:
        report_bytes = build_report_xlsx(ordered_not_received, not_ordered, ordered_and_received)
    except Exception as e:
        print("FAIL REPORT_BUILD_FAIL:", type(e).__name__, str(e))
        print(traceback.format_exc())
        mark_message_failed(service, message_id, label_ids["failed"], label_ids["internal"])
        return False

    recipient = RECIPIENTS.get(station, FALLBACK_EMAIL)
    now_local = datetime.now().strftime("%d%m%y")
    out_subject = f"Kontrola príjemky {station} - {now_local}"
    attachment_name = f"Kontrola_prijemky_{station}_{now_local}.xlsx"

    body = (
        f"Automatizovaná kontrola príjemky\n"
        f"Stanica: {station}\n\n"
        f"Pôvodný e-mail: {subject}\n\n"
        f"Objednané a neprišlo: {len(ordered_not_received)}\n"
        f"Neobjednané a prišlo: {len(not_ordered)}\n"
        f"Objednané aj prišlo: {len(ordered_and_received)}\n\n"
        f"Vylúčené ID: {', '.join(sorted(EXCLUDED_IDS))}\n"
    )

    try:
        send_email_via_gmail_api(service, recipient, out_subject, body, attachment_name, report_bytes)
    except Exception as e:
        print("FAIL EMAIL_FAIL:", type(e).__name__, str(e))
        print(traceback.format_exc())
        mark_message_failed(service, message_id, label_ids["failed"], label_ids["internal"])
        return False

    mark_message_processed(service, message_id, label_ids["processed"], label_ids["internal"])
    print(f"DONE {station}")
    return True


def kontrola_prijemky_gcf(request):
    """
    Hlavný entrypoint pre HTTP.
    Rozlišuje Pub/Sub notifikácie z Gmailu a obnovovací handler.
    """
    path = (getattr(request, "path", None) or "").rstrip("/")
    renew_header = (request.headers.get("X-Renew-Watch", "") if hasattr(request, "headers") else "")

    if path == "/renew_watch" or renew_header == "1":
        return renew_watch_gcf(request)

    email_address, history_id = parse_pubsub_notification(request)
    if not email_address or not history_id:
        return ("OK", 200)

    print(f"PUBSUB_OK email={email_address} historyId={history_id}")

    try:
        service = get_gmail_service()
        label_ids = {
            "internal": get_label_id(service, INTERNAL_LABEL_NAME),
            "processed": get_label_id(service, PROCESSED_LABEL_NAME),
            "failed": get_label_id(service, FAILED_LABEL_NAME),
        }
    except Exception as e:
        print("FAIL GMAIL_INIT_FAIL", type(e).__name__, str(e))
        return ("ERROR", 500)

    candidate_ids = list_candidate_message_ids(service)
    if not candidate_ids:
        print("NO_CANDIDATES")
        return ("OK", 200)

    print("CANDIDATES_FOUND", len(candidate_ids))

    ok_count = 0
    fail_count = 0

    for message_id in candidate_ids:
        try:
            if process_single_message(service, message_id, label_ids):
                ok_count += 1
            else:
                fail_count += 1
        except Exception as e:
            fail_count += 1
            print("FAIL MESSAGE_PROCESS_FAIL", message_id, type(e).__name__, str(e))
            try:
                mark_message_failed(service, message_id, label_ids["failed"], label_ids["internal"])
            except Exception:
                pass

    print(f"DONE processed_ok={ok_count} processed_failed={fail_count}")
    return ("OK", 200)


def renew_watch_gcf(request):
    del request

    if not KONTROLA_ENABLE_WATCH_RENEW:
        print("WATCH_RENEW_SKIPPED KONTROLA_ENABLE_WATCH_RENEW=0")
        return (json.dumps({"ok": True, "skipped": True, "reason": "watch renew disabled for this service"}), 200, {"Content-Type": "application/json"})

    topic_name = os.getenv("KONTROLA_GMAIL_WATCH_TOPIC", "").strip()
    if not topic_name:
        print("WATCH_RENEW_SKIPPED KONTROLA_GMAIL_WATCH_TOPIC_MISSING")
        return (json.dumps({"ok": True, "skipped": True, "reason": "KONTROLA_GMAIL_WATCH_TOPIC is not set"}), 200, {"Content-Type": "application/json"})

    label_ids_env = [
        item.strip()
        for item in os.getenv("KONTROLA_GMAIL_WATCH_LABEL_IDS", "INBOX").split(",")
        if item.strip()
    ]
    behavior = os.getenv("KONTROLA_GMAIL_WATCH_FILTER_BEHAVIOR", "include").strip().lower()
    if behavior not in ("include", "exclude"):
        behavior = "include"

    try:
        service = get_gmail_service()
        body = {"topicName": topic_name, "labelIds": label_ids_env, "labelFilterBehavior": behavior}

        try:
            response = service.users().watch(userId="me", body=body).execute()
        except Exception:
            body.pop("labelFilterBehavior", None)
            body["labelFilterAction"] = behavior
            response = service.users().watch(userId="me", body=body).execute()

        print("WATCH_RENEW_OK", response)
        return (json.dumps(response), 200, {"Content-Type": "application/json"})
    except Exception as e:
        print("FAIL WATCH_RENEW_FAIL", type(e).__name__, str(e))
        return ("ERROR", 500)

import base64
import json
import io
import os
import re
import unicodedata
import hashlib
import traceback
from datetime import datetime, timezone
from typing import Optional, Tuple, Dict, Any, List

import pandas as pd

from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from google.cloud import storage
from google.api_core.exceptions import NotFound, PreconditionFailed

from google.oauth2 import service_account
from googleapiclient.discovery import build


# ================== KONFIGURÁCIA ==================

SOURCE_BUCKET_NAME = "objednavky-vstup-ggt"

REQUIRED_PREFIX = "kontrola_prijemky/"  # očakávame: kontrola_prijemky/<station>/<job_id>/_READY.json
READY_FILENAME = "_READY.json"

# Routing podľa station folderu
SENDER_EMAIL = "preposli@preposlimail.com"
RECIPIENTS = {
    "703": "frantisek.bacik@gmail.com",
    "704": "miroslava.frey@gmail.com",
    "714": "frantisek.bacik@gmail.com",
    "911": "bacikivan@gmail.com",
}
FALLBACK_EMAIL = "frantisek.bacik@gmail.com"

# Gmail API
GMAIL_SA_FILE = "gmail-sa-key.json"
GMAIL_SCOPES = ["https://www.googleapis.com/auth/gmail.send"]

# ✅ LOCKS / PROCESSED v GCS
ENABLE_LOCKS = True
LOCKS_BASE_PREFIX = "_locks/"  # root folder
LOCKS_NAMESPACE = "kontrola_prijemky"
LOCKS_PREFIX = f"{LOCKS_BASE_PREFIX}{LOCKS_NAMESPACE}/locked/"
PROCESSED_PREFIX = f"{LOCKS_BASE_PREFIX}{LOCKS_NAMESPACE}/processed/"

# Vstupné súbory v rámci job foldera (podpora xlsx aj xls)
ORDER_FILENAMES = ["objednavka.xlsx", "objednavka.xls"]
RECEIPT_FILENAMES = ["prijemka.xlsx", "prijemka.xls"]

# Excluded IDs (podľa tvojich pravidiel)
EXCLUDED_IDS = {"506875", "506874", "503179"}

# ID pravidlo
ID_LEN = 6

# Výstup - názvy hárokov
SHEET_ORDERED_NOT_RECEIVED = "OBJEDNANE_NEPRISLO"
SHEET_NOT_ORDERED = "NEOBJEDNANE_PRISLO"
SHEET_ORDERED_AND_RECEIVED = "OBJEDNANE_AJ_PRISLO"


# ================== EVENT PARSE ==================

def parse_event(request) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Podporí viac formátov:
    - priamy JSON s {name,bucket,generation}
    - {data:{...}}
    - PubSub push: {message:{data:<base64 JSON>}}
    """
    event = request.get_json(silent=True)
    full_path = None
    bucket_name = None
    generation = None

    if not isinstance(event, dict):
        return None, None, None

    if "name" in event and "bucket" in event:
        full_path = event.get("name")
        bucket_name = event.get("bucket")
        generation = event.get("generation")

    elif "data" in event and isinstance(event["data"], dict):
        data = event["data"]
        full_path = data.get("name")
        bucket_name = data.get("bucket")
        generation = data.get("generation")

    elif "message" in event:
        msg = event.get("message", {})
        data_b64 = msg.get("data")
        if data_b64:
            try:
                decoded = base64.b64decode(data_b64).decode("utf-8")
                data_json = json.loads(decoded)
                full_path = data_json.get("name")
                bucket_name = data_json.get("bucket")
                generation = data_json.get("generation")
            except Exception as e:
                print("❌ PubSub decode failed:", repr(e))

    return full_path, bucket_name, generation


# ================== PATH HELPERS ==================

def is_ready_marker(full_path: str) -> bool:
    return bool(full_path) and full_path.endswith("/" + READY_FILENAME)

def extract_station_and_job(full_path: str) -> Tuple[str, str]:
    """
    Očakávame:
      kontrola_prijemky/<station>/<job_id>/_READY.json
    """
    if not full_path or not full_path.startswith(REQUIRED_PREFIX):
        return "", ""

    rest = full_path[len(REQUIRED_PREFIX):]  # "<station>/<job_id>/_READY.json"
    parts = rest.split("/")
    if len(parts) < 3:
        return "", ""

    station = (parts[0] or "").strip()
    job_id = (parts[1] or "").strip()
    return station, job_id

def build_job_prefix(station: str, job_id: str) -> str:
    return f"{REQUIRED_PREFIX}{station}/{job_id}/"

def build_input_paths(station: str, job_id: str) -> Tuple[List[str], List[str]]:
    prefix = build_job_prefix(station, job_id)
    order_paths = [prefix + fn for fn in ORDER_FILENAMES]
    receipt_paths = [prefix + fn for fn in RECEIPT_FILENAMES]
    return order_paths, receipt_paths


# ================== LOCKS (GCS) ==================

def _safe_key_for_job(bucket_name: str, station: str, job_id: str) -> str:
    raw = f"{bucket_name}::kontrola_prijemky::{station}::{job_id}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()

def _lock_blob_name(bucket_name: str, station: str, job_id: str) -> str:
    return f"{LOCKS_PREFIX}{_safe_key_for_job(bucket_name, station, job_id)}.lock"

def _done_blob_name(bucket_name: str, station: str, job_id: str) -> str:
    return f"{PROCESSED_PREFIX}{_safe_key_for_job(bucket_name, station, job_id)}.done"

def already_done(storage_client: storage.Client, bucket_name: str, station: str, job_id: str) -> bool:
    b = storage_client.bucket(bucket_name)
    return b.blob(_done_blob_name(bucket_name, station, job_id)).exists()

def create_lock_or_skip(storage_client: storage.Client, bucket_name: str, station: str, job_id: str) -> bool:
    b = storage_client.bucket(bucket_name)
    blob = b.blob(_lock_blob_name(bucket_name, station, job_id))
    try:
        blob.upload_from_string(
            f"locked {datetime.now(timezone.utc).isoformat()} path=kontrola_prijemky/{station}/{job_id}",
            content_type="text/plain",
            if_generation_match=0,  # create only if not exists
        )
        return True
    except PreconditionFailed:
        return False

def mark_done(storage_client: storage.Client, bucket_name: str, station: str, job_id: str):
    b = storage_client.bucket(bucket_name)
    blob = b.blob(_done_blob_name(bucket_name, station, job_id))
    try:
        blob.upload_from_string(
            f"done {datetime.now(timezone.utc).isoformat()} path=kontrola_prijemky/{station}/{job_id}",
            content_type="text/plain",
            if_generation_match=0,
        )
    except PreconditionFailed:
        pass

def release_lock(storage_client: storage.Client, bucket_name: str, station: str, job_id: str):
    b = storage_client.bucket(bucket_name)
    blob = b.blob(_lock_blob_name(bucket_name, station, job_id))
    try:
        blob.delete()
        print("LOCK_RELEASED", station, job_id)
    except Exception as e:
        print("LOCK_RELEASE_FAIL", type(e).__name__, str(e))

def delete_lock_if_exists(storage_client: storage.Client, bucket_name: str, station: str, job_id: str):
    b = storage_client.bucket(bucket_name)
    blob = b.blob(_lock_blob_name(bucket_name, station, job_id))
    try:
        if blob.exists():
            blob.delete()
            print("LOCK_DELETED", station, job_id)
    except Exception as e:
        print("LOCK_DELETE_FAIL", type(e).__name__, str(e))


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
    """Vráti pôvodné množstvo ako string (kvôli 'ks' atď.)."""
    if value is None:
        return "0"
    return str(value).strip()

def extract_first_int(value: Any) -> int:
    """Zoberie prvé celé číslo z textu (napr. '10 ks' -> 10). Ak nič, tak 0."""
    if value is None:
        return 0
    s = str(value)
    m = re.search(r"(\d+)", s)
    return int(m.group(1)) if m else 0

def valid_id(id_value: Any) -> str:
    if id_value is None:
        return ""
    return str(id_value).strip()


# ================== DOWNLOAD HELPERS ==================

def _engine_for_path(path: str) -> Optional[str]:
    p = (path or "").lower()
    if p.endswith(".xlsx"):
        return "openpyxl"
    if p.endswith(".xls"):
        return "xlrd"
    return None  # nechá pandas auto

def download_first_existing(bucket, candidate_paths: List[str]) -> Tuple[bytes, str]:
    """
    Skúsi stiahnuť prvý existujúci súbor z candidate_paths.
    Vráti (bytes, used_path).
    """
    last_err = None
    for p in candidate_paths:
        try:
            data = bucket.blob(p).download_as_bytes()
            return data, p
        except NotFound as e:
            last_err = e
    raise last_err or NotFound(f"No matching input file found in: {candidate_paths}")


# ================== EXCEL LOAD & PROCESS ==================

def load_order_excel(file_bytes: bytes, engine: Optional[str]) -> Dict[str, Dict[str, Any]]:
    """
    Objednávka: ID (A=0) | Názov (B=1) | Množstvo (C=2)
    Preskočí hlavičku (prvý riadok).
    """
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

def load_receipt_excel(file_bytes: bytes, engine: Optional[str]) -> Dict[str, Dict[str, Any]]:
    """
    Príjemka: ID (A=0) | Názov (B=1) | Množstvo (D=3)
    Preskočí hlavičku (prvý riadok).
    """
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
    """
    Vráti 3 zoznamy:
    1) objednané a neprišlo
    2) neobjednané a prišlo
    3) objednané aj prišlo
    """
    ordered_not_received: List[Dict[str, Any]] = []
    not_ordered: List[Dict[str, Any]] = []
    ordered_and_received: List[Dict[str, Any]] = []

    # 1) objednané ale neprišlo
    for id_, o in order_data.items():
        if id_ not in receipt_data:
            ordered_not_received.append({
                "ID": id_,
                "Názov": o.get("name", ""),
                "Množstvo z objednávky": o.get("qty_raw", ""),
                "Množstvo z príjemky": "",
            })

    # 2) neobjednané ale prišlo
    for id_, r in receipt_data.items():
        if id_ not in order_data:
            not_ordered.append({
                "ID": id_,
                "Názov": r.get("name", ""),
                "Množstvo z objednávky": "",
                "Množstvo z príjemky": r.get("qty_raw", ""),
            })

    # 3) objednané aj prišlo
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

    # triedenie podľa názvu (bez diakritiky, case-insensitive)
    def sort_key(item: Dict[str, Any]) -> str:
        return norm_key(item.get("Názov", ""))

    ordered_not_received.sort(key=sort_key)
    not_ordered.sort(key=sort_key)
    ordered_and_received.sort(key=sort_key)

    return ordered_not_received, not_ordered, ordered_and_received


def build_report_xlsx(ordered_not_received: List[Dict[str, Any]],
                      not_ordered: List[Dict[str, Any]],
                      ordered_and_received: List[Dict[str, Any]]) -> bytes:
    """
    1 Excel, 3 hárky
    """
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


# ================== GMAIL SEND ==================

def send_email_via_gmail_api(recipient: str, subject: str, body_text: str, attachment_name: str, attachment_bytes: bytes):
    msg = MIMEMultipart()
    msg["From"] = SENDER_EMAIL
    msg["To"] = recipient
    msg["Subject"] = subject

    msg.attach(MIMEText(body_text, "plain"))

    part = MIMEApplication(attachment_bytes, _subtype="xlsx")
    part.add_header("Content-Disposition", "attachment", filename=attachment_name)
    msg.attach(part)

    raw_message = base64.urlsafe_b64encode(msg.as_bytes()).decode("utf-8")

    # robustná cesta k SA key (aby nepadalo na working directory)
    key_path = os.path.join(os.path.dirname(__file__), GMAIL_SA_FILE)
    print("GMAIL_KEY_PATH", key_path, "exists=", os.path.exists(key_path))

    creds = service_account.Credentials.from_service_account_file(
        key_path,
        scopes=GMAIL_SCOPES,
        subject=SENDER_EMAIL,
    )
    service = build("gmail", "v1", credentials=creds)
    service.users().messages().send(userId="me", body={"raw": raw_message}).execute()
    print(f"✅ Email odoslaný na: {recipient}")


# ================== HLAVNÁ FUNKCIA ==================

def kontrola_prijemky_gcf(request):
    full_path, bucket_name, generation = parse_event(request)

    if not full_path or not bucket_name:
        print("SKIP EVENT_INVALID (missing full_path/bucket_name)")
        return ("OK", 200)

    if bucket_name != SOURCE_BUCKET_NAME:
        print(f"SKIP BUCKET_MISMATCH: {bucket_name} expected={SOURCE_BUCKET_NAME}")
        return ("OK", 200)

    if not full_path.startswith(REQUIRED_PREFIX):
        print(f"SKIP PREFIX_MISMATCH: {full_path} expected_prefix={REQUIRED_PREFIX}")
        return ("OK", 200)

    # ✅ Spracujeme iba _READY.json (1 spustenie na job)
    if not is_ready_marker(full_path):
        print(f"SKIP NOT_READY_MARKER: {full_path}")
        return ("OK", 200)

    station, job_id = extract_station_and_job(full_path)
    if station not in RECIPIENTS or not job_id:
        print(f"SKIP BAD_PATH: station={station} job_id={job_id} path={full_path}")
        return ("OK", 200)

    recipient = RECIPIENTS.get(station, FALLBACK_EMAIL)
    print(f"📦 KONTROLA_PRIJEMKY START station={station} job_id={job_id} recipient={recipient}")

    storage_client = storage.Client()
    lock_acquired = False

    # ✅ LOCK / DONE (job-level)
    if ENABLE_LOCKS:
        try:
            if already_done(storage_client, bucket_name, station, job_id):
                print("SKIP ALREADY_PROCESSED:", station, job_id)
                return ("OK", 200)

            if not create_lock_or_skip(storage_client, bucket_name, station, job_id):
                print("SKIP LOCK_EXISTS:", station, job_id)
                return ("OK", 200)

            lock_acquired = True

        except Exception as e:
            print("FAIL LOCK_FAIL:", type(e).__name__, str(e))
            print(traceback.format_exc())
            return ("OK", 200)

    bucket = storage_client.bucket(bucket_name)

    # --------- 1) download oba súbory ---------
    order_paths, receipt_paths = build_input_paths(station, job_id)

    try:
        order_bytes, used_order_path = download_first_existing(bucket, order_paths)
        receipt_bytes, used_receipt_path = download_first_existing(bucket, receipt_paths)
        print(
            "FILES_DOWNLOADED",
            "used_order_path=", used_order_path,
            "used_receipt_path=", used_receipt_path,
            "order_bytes=", len(order_bytes),
            "receipt_bytes=", len(receipt_bytes),
        )
    except NotFound:
        print("FAIL INPUT_NOT_FOUND:", order_paths, receipt_paths)
        # 🔥 dôležité: uvoľni lock, inak sa to už nikdy nezopakuje
        if ENABLE_LOCKS and lock_acquired:
            release_lock(storage_client, bucket_name, station, job_id)
        return ("OK", 200)
    except Exception as e:
        print("FAIL DOWNLOAD_FAIL:", type(e).__name__, str(e))
        print(traceback.format_exc())
        if ENABLE_LOCKS and lock_acquired:
            release_lock(storage_client, bucket_name, station, job_id)
        return ("OK", 200)

    # --------- 2) parse Excel ---------
    try:
        order_engine = _engine_for_path(used_order_path)
        receipt_engine = _engine_for_path(used_receipt_path)
        print("EXCEL_ENGINES", "order_engine=", order_engine, "receipt_engine=", receipt_engine)

        order_data = load_order_excel(order_bytes, engine=order_engine)
        receipt_data = load_receipt_excel(receipt_bytes, engine=receipt_engine)
        print("PARSE_OK order_items=", len(order_data), "receipt_items=", len(receipt_data))
    except Exception as e:
        print("FAIL EXCEL_PARSE_FAIL:", type(e).__name__, str(e))
        print(traceback.format_exc())
        if ENABLE_LOCKS and lock_acquired:
            release_lock(storage_client, bucket_name, station, job_id)
        return ("ERROR", 500)

    # --------- 3) compare ---------
    try:
        ordered_not_received, not_ordered, ordered_and_received = compare(order_data, receipt_data)
        print(
            "COMPARE_OK",
            "ordered_not_received=", len(ordered_not_received),
            "not_ordered=", len(not_ordered),
            "ordered_and_received=", len(ordered_and_received)
        )
    except Exception as e:
        print("FAIL COMPARE_FAIL:", type(e).__name__, str(e))
        print(traceback.format_exc())
        if ENABLE_LOCKS and lock_acquired:
            release_lock(storage_client, bucket_name, station, job_id)
        return ("ERROR", 500)

    # --------- 4) build report ---------
    try:
        report_bytes = build_report_xlsx(ordered_not_received, not_ordered, ordered_and_received)
        print("REPORT_BUILT bytes=", len(report_bytes))
    except Exception as e:
        print("FAIL REPORT_BUILD_FAIL:", type(e).__name__, str(e))
        print(traceback.format_exc())
        if ENABLE_LOCKS and lock_acquired:
            release_lock(storage_client, bucket_name, station, job_id)
        return ("ERROR", 500)

    # --------- 5) email ---------
    try:
        now_local = datetime.now().strftime("%d%m%y")
        subject = f"Kontrola príjemky {station} - {now_local}"
        attachment_name = f"Kontrola_prijemky_{station}_{now_local}.xlsx"

        body = (
            f"Automatizovaná kontrola príjemky\n"
            f"Stanica: {station}\n"
            f"Job: {job_id}\n\n"
            f"Objednané a neprišlo: {len(ordered_not_received)}\n"
            f"Neobjednané a prišlo: {len(not_ordered)}\n"
            f"Objednané aj prišlo: {len(ordered_and_received)}\n\n"
            f"Vylúčené ID: {', '.join(sorted(EXCLUDED_IDS))}\n"
        )

        send_email_via_gmail_api(recipient, subject, body, attachment_name, report_bytes)
        print("EMAIL_SENT", subject)

    except Exception as e:
        print("FAIL EMAIL_FAIL:", type(e).__name__, str(e))
        print(traceback.format_exc())
        # 🔥 uvoľni lock, nech sa dá retry
        if ENABLE_LOCKS and lock_acquired:
            release_lock(storage_client, bucket_name, station, job_id)
        return ("ERROR", 500)

    # --------- 6) done ---------
    if ENABLE_LOCKS:
        mark_done(storage_client, bucket_name, station, job_id)
        # upratanie locku (nech nezavadzia)
        delete_lock_if_exists(storage_client, bucket_name, station, job_id)

    print("DONE", station, job_id)
    return ("OK", 200)

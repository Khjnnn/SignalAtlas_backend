# -*- coding: utf-8 -*-
"""
KIS Open API Flask Proxy Server
- Session auth APIs
- SQLite-backed analyses CRUD
- KIS market proxy APIs
- Scheduled analysis refresh (daily 18:00)
"""

import json
import logging
import os
import re
import shutil
import sqlite3
import sys
import threading
import time
import urllib.request
import zipfile
from datetime import datetime, timedelta
from functools import wraps
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from flask import Flask, Response, jsonify, request, session
from flask_cors import CORS
from werkzeug.security import check_password_hash

try:
    import psycopg
    from psycopg.rows import dict_row
except ImportError:  # pragma: no cover - optional until DATABASE_URL is set
    psycopg = None
    dict_row = None

BASE_DIR = Path(__file__).resolve().parent


def _bootstrap_kis_config() -> None:
    """Ensure KIS config exists on ephemeral Linux hosts like Railway."""
    config_root = Path.home() / "KIS" / "config"
    config_root.mkdir(parents=True, exist_ok=True)

    src_yaml = BASE_DIR / "open-trading-api-main" / "kis_devlp.yaml"
    dst_yaml = config_root / "kis_devlp.yaml"
    if src_yaml.exists() and not dst_yaml.exists():
        shutil.copy2(src_yaml, dst_yaml)


_bootstrap_kis_config()

# KIS SDK path registration
_SDK_DIR = os.path.join(os.path.dirname(__file__), "open-trading-api-main", "examples_llm")
sys.path.insert(0, _SDK_DIR)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

_ka_module: Optional[Any] = None
_ka_import_error: Optional[str] = None


def _get_kis_module() -> Any:
    global _ka_module, _ka_import_error
    if _ka_module is not None:
        return _ka_module

    try:
        _ka_module = __import__("kis_auth")
        _ka_import_error = None
        return _ka_module
    except Exception as exc:
        _ka_import_error = str(exc)
        logger.error("KIS module import failed: %s", exc)
        raise RuntimeError(
            "KIS backend initialization failed. Check kis_devlp.yaml and KIS credentials."
        ) from exc


DB_PATH = BASE_DIR / "analyses.db"
LEGACY_ANALYSES_PATH = BASE_DIR / "analyses.json"

KOREAN_STOCK_CODE_RE = re.compile(r"^\d{6}$")
US_SYMBOL_RE = re.compile(r"^[A-Za-z][A-Za-z0-9.\-]{0,14}$")
MARKET_RE = re.compile(r"^(NAS|NYS|AMS|HKS|TSE)$")
ENV_KEY_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

SCHEDULE_REFRESH_HOUR = 18

_stock_master: List[Dict[str, str]] = []
_refresh_lock = threading.Lock()
_refresh_thread_started = False


def _load_env_file(path: Path) -> bool:
    """
    Load KEY=VALUE pairs from an env-style file into process env.
    Existing process env values take precedence.
    """
    if not path.exists():
        return False

    try:
        for raw_line in path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue

            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")

            if not ENV_KEY_RE.match(key):
                continue

            os.environ.setdefault(key, value)

        logger.info("Loaded env values from %s", path.name)
        return True
    except Exception as exc:
        logger.warning("Failed to read %s: %s", path, exc)
        return False


_load_env_file(BASE_DIR / ".env")


def _bool_env(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _session_cookie_secure() -> bool:
    if os.getenv("SESSION_COOKIE_SECURE") is not None:
        return _bool_env("SESSION_COOKIE_SECURE", False)
    return os.getenv("FLASK_ENV", "production").lower() != "development"


def _session_cookie_samesite() -> str:
    value = os.getenv("SESSION_COOKIE_SAMESITE", "").strip()
    if value:
        return value
    return "None" if _session_cookie_secure() else "Lax"


def _admin_password_hash() -> str:
    value = os.getenv("ADMIN_PASSWORD_HASH", "")
    return value.strip()


def _allowed_origins() -> List[str]:
    raw = os.getenv("ALLOWED_ORIGINS", "").strip()
    if raw:
        values = [x.strip() for x in raw.split(",") if x.strip()]
        if values:
            return values

    # Safe fallback set when ALLOWED_ORIGINS is not provided.
    return [
        "http://localhost:5173",
        "http://localhost:5174",
        r"https://.*\.pages\.dev",
        r"https://.*\.railway\.app",
    ]


def _database_url() -> str:
    value = os.getenv("DATABASE_URL", "").strip()
    if value.startswith("postgres://"):
        return "postgresql://" + value[len("postgres://") :]
    return value


def _use_postgres() -> bool:
    return bool(_database_url())


def _get_db_connection() -> Any:
    database_url = _database_url()
    if database_url:
        if psycopg is None:
            raise RuntimeError("DATABASE_URL is set but psycopg is not installed")
        return psycopg.connect(database_url, row_factory=dict_row)

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _init_db() -> None:
    with _get_db_connection() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS analyses (
                id TEXT PRIMARY KEY,
                payload TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )


def _count_analyses(conn: Any) -> int:
    row = conn.execute("SELECT COUNT(*) as count FROM analyses").fetchone()
    return int(row["count"])


def _insert_analysis_if_missing(conn: Any, analysis_id: str, payload: str, created_at: str, updated_at: str) -> None:
    if _use_postgres():
        conn.execute(
            """
            INSERT INTO analyses (id, payload, created_at, updated_at)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING
            """,
            (analysis_id, payload, created_at, updated_at),
        )
        return

    conn.execute(
        "INSERT OR IGNORE INTO analyses (id, payload, created_at, updated_at) VALUES (?, ?, ?, ?)",
        (analysis_id, payload, created_at, updated_at),
    )


def _migrate_legacy_json_if_needed() -> None:
    if not LEGACY_ANALYSES_PATH.exists():
        return

    try:
        with _get_db_connection() as conn:
            if _count_analyses(conn) > 0:
                return

            with LEGACY_ANALYSES_PATH.open("r", encoding="utf-8") as f:
                rows = json.load(f)

            now = datetime.now().isoformat()
            for row in rows if isinstance(rows, list) else []:
                row_id = str(row.get("id") or "").strip()
                if not row_id:
                    continue
                _insert_analysis_if_missing(conn, row_id, json.dumps(row, ensure_ascii=False), now, now)

        backend_name = "Postgres" if _use_postgres() else "SQLite"
        logger.info("Legacy analyses.json migrated into %s", backend_name)
    except Exception as exc:
        logger.warning("Legacy migration skipped due to error: %s", exc)


def _load_analyses() -> List[Dict[str, Any]]:
    with _get_db_connection() as conn:
        rows = conn.execute("SELECT payload FROM analyses ORDER BY created_at DESC").fetchall()

    result: List[Dict[str, Any]] = []
    for row in rows:
        try:
            result.append(json.loads(row["payload"]))
        except json.JSONDecodeError:
            continue
    return result


def _load_analysis_rows() -> List[Tuple[str, Dict[str, Any]]]:
    with _get_db_connection() as conn:
        rows = conn.execute("SELECT id, payload FROM analyses ORDER BY created_at DESC").fetchall()

    result: List[Tuple[str, Dict[str, Any]]] = []
    for row in rows:
        try:
            payload = json.loads(row["payload"])
            result.append((row["id"], payload))
        except json.JSONDecodeError:
            continue
    return result


def _save_analysis(analysis: Dict[str, Any]) -> None:
    now = datetime.now().isoformat()
    payload = json.dumps(analysis, ensure_ascii=False)
    with _get_db_connection() as conn:
        if _use_postgres():
            conn.execute(
                "INSERT INTO analyses (id, payload, created_at, updated_at) VALUES (%s, %s, %s, %s)",
                (analysis["id"], payload, now, now),
            )
        else:
            conn.execute(
                "INSERT INTO analyses (id, payload, created_at, updated_at) VALUES (?, ?, ?, ?)",
                (analysis["id"], payload, now, now),
            )


def _update_analysis(analysis_id: str, payload: Dict[str, Any]) -> None:
    now = datetime.now().isoformat()
    payload_json = json.dumps(payload, ensure_ascii=False)
    with _get_db_connection() as conn:
        if _use_postgres():
            conn.execute(
                "UPDATE analyses SET payload = %s, updated_at = %s WHERE id = %s",
                (payload_json, now, analysis_id),
            )
        else:
            conn.execute(
                "UPDATE analyses SET payload = ?, updated_at = ? WHERE id = ?",
                (payload_json, now, analysis_id),
            )


def _delete_analysis(analysis_id: str) -> bool:
    with _get_db_connection() as conn:
        if _use_postgres():
            cur = conn.execute("DELETE FROM analyses WHERE id = %s", (analysis_id,))
        else:
            cur = conn.execute("DELETE FROM analyses WHERE id = ?", (analysis_id,))
        return cur.rowcount > 0


def _ensure_auth() -> None:
    try:
        ka = _get_kis_module()
        ka.auth(svr="prod")
    except Exception as exc:
        logger.error("KIS auth failed: %s", exc)
        raise


def _kis_get(api_url: str, tr_id: str, params: Dict[str, str]) -> Tuple[Dict[str, Any], int]:
    _ensure_auth()
    ka = _get_kis_module()
    res = ka._url_fetch(api_url, tr_id, "", params)

    if res.isOK():
        return res.getResponse().json(), 200

    err_code = res.getErrorCode()
    err_msg = res.getErrorMessage()
    logger.error("KIS API error [%s]: %s", err_code, err_msg)
    return {"error": f"[{err_code}] {err_msg}"}, 500


def _validate_days(value: str, default: int = 100) -> Optional[int]:
    if value is None or value == "":
        return default
    try:
        days = int(value)
    except ValueError:
        return None
    if days < 1 or days > 3650:
        return None
    return days


def _require_non_empty_query(value: str) -> Optional[str]:
    q = (value or "").strip()
    if not q:
        return None
    if len(q) > 50:
        return None
    return q


def _download_and_parse_master(market: str) -> List[Dict[str, str]]:
    items: List[Dict[str, str]] = []
    tmp_dir = BASE_DIR / "tmp_master"

    try:
        url = f"https://new.real.download.dws.co.kr/common/master/{market}_code.mst.zip"
        tmp_dir.mkdir(exist_ok=True)

        zip_path = tmp_dir / f"{market}_code.zip"
        urllib.request.urlretrieve(url, str(zip_path))

        with zipfile.ZipFile(zip_path) as zf:
            zf.extractall(tmp_dir)

        mst_path = tmp_dir / f"{market}_code.mst"
        if mst_path.exists():
            with mst_path.open("r", encoding="cp949") as f:
                for row in f:
                    tail_len = 228 if market == "kospi" else 222
                    front = row[: len(row) - tail_len]
                    code = front[:9].strip()
                    name = front[21:].strip()
                    if code and name and len(code) == 6:
                        items.append({"code": code, "name": name})

        logger.info("%s master loaded: %d", market.upper(), len(items))
    except Exception as exc:
        logger.warning("%s master download failed: %s", market, exc)
    finally:
        if tmp_dir.exists():
            for child in tmp_dir.iterdir():
                if child.is_file():
                    child.unlink(missing_ok=True)
            tmp_dir.rmdir()

    return items


def _load_stock_master() -> None:
    global _stock_master
    _stock_master = _download_and_parse_master("kospi") + _download_and_parse_master("kosdaq")


def _require_admin(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if session.get("is_admin") is not True:
            return jsonify({"error": "Unauthorized"}), 401
        return fn(*args, **kwargs)

    return wrapper


def _extract_market_symbol(payload: Dict[str, Any]) -> Tuple[str, str, str]:
    """
    Returns tuple: (kind, code_or_excd, symbol)
    kind: 'domestic' | 'overseas' | 'skip'
    """
    raw_symbol = str(payload.get("name") or "").strip().upper()

    if KOREAN_STOCK_CODE_RE.match(raw_symbol):
        return ("domestic", raw_symbol, raw_symbol)

    if not US_SYMBOL_RE.match(raw_symbol):
        return ("skip", "", "")

    excd_candidates = [
        str(payload.get("exchangeName") or "").strip().upper(),
        str(payload.get("industry") or "").strip().upper(),
    ]

    excd = "NAS"
    for candidate in excd_candidates:
        if MARKET_RE.match(candidate):
            excd = candidate
            break

    return ("overseas", excd, raw_symbol)


def _auto_refresh_summary(existing_summary: str, price: float, change_rate: float) -> str:
    head = (existing_summary or "").split("\n\n[Auto Refresh]")[0].strip()
    stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    direction = "+" if change_rate >= 0 else ""
    refresh_line = f"[Auto Refresh] {stamp} | Price: {price:,.2f} | Change: {direction}{change_rate:.2f}%"
    if not head:
        return refresh_line
    return f"{head}\n\n{refresh_line}"


def _refresh_single_payload(payload: Dict[str, Any]) -> Tuple[bool, Optional[str], Dict[str, Any]]:
    kind, market_or_code, symbol = _extract_market_symbol(payload)
    if kind == "skip":
        return (False, "skip: invalid symbol", payload)

    try:
        if kind == "domestic":
            data, status = _kis_get(
                "/uapi/domestic-stock/v1/quotations/inquire-price",
                "FHKST01010100",
                {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": market_or_code},
            )
            if status != 200:
                return (False, f"domestic fetch failed: {data.get('error', 'unknown')}", payload)

            output = data.get("output") or {}
            price = float(output.get("stck_prpr") or 0)
            change_rate = float(output.get("prdy_ctrt") or 0)
            volume = int(float(output.get("acml_vol") or 0))
            currency = "KRW"
        else:
            data, status = _kis_get(
                "/uapi/overseas-price/v1/quotations/price",
                "HHDFS00000300",
                {"AUTH": "", "EXCD": market_or_code, "SYMB": symbol},
            )
            if status != 200:
                return (False, f"overseas fetch failed: {data.get('error', 'unknown')}", payload)

            output = data.get("output") or {}
            price = float(output.get("last") or 0)
            change_rate = float(output.get("rate") or 0)
            volume = int(float(output.get("tvol") or 0))
            currency = "USD"

        updated = dict(payload)
        updated["price"] = price
        updated["priceChange"] = f"{'+' if change_rate >= 0 else ''}{change_rate:.2f}%"
        updated["volume"] = volume
        updated["currency"] = updated.get("currency") or currency
        updated["lastRefreshAt"] = datetime.now().isoformat(timespec="seconds")
        updated["summary"] = _auto_refresh_summary(str(updated.get("summary") or ""), price, change_rate)

        return (True, None, updated)
    except Exception as exc:
        return (False, str(exc), payload)


def _refresh_all_analyses(trigger: str = "manual") -> Dict[str, Any]:
    if not _refresh_lock.acquire(blocking=False):
        return {
            "ok": False,
            "busy": True,
            "message": "Refresh already running",
            "trigger": trigger,
        }

    total = 0
    refreshed = 0
    failed = 0
    skipped = 0

    try:
        rows = _load_analysis_rows()
        total = len(rows)

        for analysis_id, payload in rows:
            ok, reason, updated_payload = _refresh_single_payload(payload)

            if ok:
                _update_analysis(analysis_id, updated_payload)
                refreshed += 1
                continue

            if reason and reason.startswith("skip:"):
                skipped += 1
            else:
                failed += 1
                logger.warning("Refresh failed for %s: %s", analysis_id, reason)

        stamp = datetime.now().isoformat(timespec="seconds")
        logger.info(
            "Analysis refresh done (%s): total=%d refreshed=%d failed=%d skipped=%d",
            trigger,
            total,
            refreshed,
            failed,
            skipped,
        )

        return {
            "ok": True,
            "trigger": trigger,
            "total": total,
            "refreshed": refreshed,
            "failed": failed,
            "skipped": skipped,
            "refreshedAt": stamp,
        }
    finally:
        _refresh_lock.release()


def _seconds_until_next_daily_refresh(hour: int) -> int:
    now = datetime.now()
    target = now.replace(hour=hour, minute=0, second=0, microsecond=0)
    if now >= target:
        target += timedelta(days=1)
    return max(1, int((target - now).total_seconds()))


def _daily_refresh_worker() -> None:
    logger.info("Daily refresh worker started. Target time: %02d:00", SCHEDULE_REFRESH_HOUR)
    while True:
        wait_seconds = _seconds_until_next_daily_refresh(SCHEDULE_REFRESH_HOUR)
        logger.info("Next analysis refresh in %d seconds", wait_seconds)
        time.sleep(wait_seconds)

        try:
            _refresh_all_analyses(trigger="scheduled")
        except Exception as exc:
            logger.exception("Scheduled refresh failed: %s", exc)


def _start_refresh_scheduler() -> None:
    global _refresh_thread_started
    if _refresh_thread_started:
        return

    thread = threading.Thread(target=_daily_refresh_worker, daemon=True, name="analysis-refresh-worker")
    thread.start()
    _refresh_thread_started = True


def create_app() -> Flask:
    app = Flask(__name__)

    app.config["SECRET_KEY"] = os.getenv("FLASK_SECRET_KEY", "dev-change-this-key")
    app.config["SESSION_COOKIE_HTTPONLY"] = True
    app.config["SESSION_COOKIE_SAMESITE"] = _session_cookie_samesite()
    app.config["SESSION_COOKIE_SECURE"] = _session_cookie_secure()

    cors_origins = _allowed_origins()
    CORS(
        app,
        resources={r"/api/*": {"origins": cors_origins}},
        supports_credentials=True,
    )

    logger.info("CORS origins: %s", cors_origins)
    logger.info(
        "Session cookie config: secure=%s samesite=%s",
        app.config["SESSION_COOKIE_SECURE"],
        app.config["SESSION_COOKIE_SAMESITE"],
    )

    _init_db()
    _migrate_legacy_json_if_needed()

    @app.route("/", methods=["GET"])
    def root() -> Tuple[Response, int]:
        return jsonify({"ok": True, "service": "signalatlas-backend"}), 200

    @app.route("/api/auth/me", methods=["GET"])
    def auth_me() -> Tuple[Response, int]:
        return jsonify({"isAdmin": session.get("is_admin") is True}), 200

    @app.route("/api/auth/login", methods=["POST"])
    def auth_login() -> Tuple[Response, int]:
        payload = request.get_json(silent=True) or {}
        password = str(payload.get("password") or "")
        password_hash = _admin_password_hash()

        if not password_hash:
            return jsonify({"error": "Server is not configured with ADMIN_PASSWORD_HASH"}), 500

        if not check_password_hash(password_hash, password):
            return jsonify({"error": "Invalid credentials"}), 401

        session["is_admin"] = True
        return jsonify({"ok": True, "isAdmin": True}), 200

    @app.route("/api/auth/logout", methods=["POST"])
    def auth_logout() -> Tuple[Response, int]:
        session.clear()
        return jsonify({"ok": True, "isAdmin": False}), 200

    @app.route("/api/kis/health")
    def health() -> Response:
        try:
            _ensure_auth()
            token_ok = True
        except Exception:
            token_ok = False
        return jsonify({"status": "ok", "token_valid": token_ok, "master_count": len(_stock_master)})

    @app.route("/api/kis/search")
    def search_stock() -> Tuple[Response, int]:
        q = _require_non_empty_query(request.args.get("q", ""))
        if q is None:
            return jsonify([]), 200

        q_lower = q.lower()
        results = []
        for item in _stock_master:
            if q_lower in item["name"].lower() or q_lower in item["code"]:
                results.append(item)
            if len(results) >= 10:
                break

        return jsonify(results), 200

    @app.route("/api/kis/domestic/price")
    def domestic_price() -> Tuple[Response, int]:
        code = (request.args.get("code") or "").strip()
        if not KOREAN_STOCK_CODE_RE.match(code):
            return jsonify({"error": "Invalid code. Expected 6 digits."}), 400

        data, status = _kis_get(
            "/uapi/domestic-stock/v1/quotations/inquire-price",
            "FHKST01010100",
            {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": code},
        )
        return jsonify(data), status

    @app.route("/api/kis/domestic/chart")
    def domestic_chart() -> Tuple[Response, int]:
        code = (request.args.get("code") or "").strip()
        days = _validate_days(request.args.get("days", "100"))

        if not KOREAN_STOCK_CODE_RE.match(code):
            return jsonify({"error": "Invalid code. Expected 6 digits."}), 400
        if days is None:
            return jsonify({"error": "Invalid days. Use integer between 1 and 3650."}), 400

        end_date = datetime.now().strftime("%Y%m%d")
        start_date = (datetime.now() - timedelta(days=days)).strftime("%Y%m%d")

        data, status = _kis_get(
            "/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice",
            "FHKST03010100",
            {
                "FID_COND_MRKT_DIV_CODE": "J",
                "FID_INPUT_ISCD": code,
                "FID_INPUT_DATE_1": start_date,
                "FID_INPUT_DATE_2": end_date,
                "FID_PERIOD_DIV_CODE": "D",
                "FID_ORG_ADJ_PRC": "0",
            },
        )
        return jsonify(data), status

    @app.route("/api/kis/domestic/info")
    def domestic_info() -> Tuple[Response, int]:
        code = (request.args.get("code") or "").strip()
        if not KOREAN_STOCK_CODE_RE.match(code):
            return jsonify({"error": "Invalid code. Expected 6 digits."}), 400

        data, status = _kis_get(
            "/uapi/domestic-stock/v1/quotations/search-stock-info",
            "CTPF1002R",
            {"PRDT_TYPE_CD": "300", "PDNO": code},
        )
        return jsonify(data), status

    @app.route("/api/kis/overseas/price")
    def overseas_price() -> Tuple[Response, int]:
        excd = (request.args.get("excd") or "").strip().upper()
        symb = (request.args.get("symb") or "").strip().upper()

        if not MARKET_RE.match(excd) or not US_SYMBOL_RE.match(symb):
            return jsonify({"error": "Invalid excd/symb"}), 400

        data, status = _kis_get(
            "/uapi/overseas-price/v1/quotations/price",
            "HHDFS00000300",
            {"AUTH": "", "EXCD": excd, "SYMB": symb},
        )
        return jsonify(data), status

    @app.route("/api/kis/overseas/chart")
    def overseas_chart() -> Tuple[Response, int]:
        excd = (request.args.get("excd") or "").strip().upper()
        symb = (request.args.get("symb") or "").strip().upper()
        days = _validate_days(request.args.get("days", "100"))

        if not MARKET_RE.match(excd) or not US_SYMBOL_RE.match(symb):
            return jsonify({"error": "Invalid excd/symb"}), 400
        if days is None:
            return jsonify({"error": "Invalid days. Use integer between 1 and 3650."}), 400

        end_date = datetime.now().strftime("%Y%m%d")
        start_date = (datetime.now() - timedelta(days=days)).strftime("%Y%m%d")

        data, status = _kis_get(
            "/uapi/overseas-price/v1/quotations/inquire-daily-chartprice",
            "FHKST03030100",
            {
                "FID_COND_MRKT_DIV_CODE": "N",
                "FID_INPUT_ISCD": symb,
                "FID_INPUT_DATE_1": start_date,
                "FID_INPUT_DATE_2": end_date,
                "FID_PERIOD_DIV_CODE": "D",
            },
        )
        return jsonify(data), status

    @app.route("/api/analyses", methods=["GET"])
    def get_analyses() -> Tuple[Response, int]:
        return jsonify(_load_analyses()), 200

    @app.route("/api/analyses", methods=["POST"])
    @_require_admin
    def add_analysis() -> Tuple[Response, int]:
        body = request.get_json(silent=True)
        if not isinstance(body, dict):
            return jsonify({"error": "Invalid request body"}), 400

        from uuid import uuid4

        body["id"] = str(uuid4())
        _save_analysis(body)
        return jsonify(body), 201

    @app.route("/api/analyses/refresh", methods=["POST"])
    @_require_admin
    def refresh_analyses() -> Tuple[Response, int]:
        result = _refresh_all_analyses(trigger="manual")
        status = 200 if result.get("ok") else 409
        return jsonify(result), status

    @app.route("/api/analyses/<analysis_id>", methods=["DELETE"])
    @_require_admin
    def delete_analysis(analysis_id: str) -> Tuple[Response, int]:
        if not analysis_id:
            return jsonify({"error": "Invalid analysis id"}), 400

        ok = _delete_analysis(analysis_id)
        if not ok:
            return jsonify({"error": "Not found"}), 404

        return jsonify({"ok": True}), 200

    return app


app = create_app()


if __name__ == "__main__":
    logger.info("=" * 50)
    logger.info("KIS Proxy Server start (port 5000)")
    logger.info("SDK path: %s", _SDK_DIR)
    logger.info("=" * 50)

    _load_stock_master()

    is_debug = os.getenv("FLASK_ENV", "production").lower() == "development"
    enable_scheduler = _bool_env("ENABLE_SCHEDULED_REFRESH", True)
    if enable_scheduler:
        # In debug mode, run scheduler only on reloader child process.
        if (not is_debug) or os.getenv("WERKZEUG_RUN_MAIN") == "true":
            _start_refresh_scheduler()

    app.run(host="0.0.0.0", port=5000, debug=is_debug)







#!/usr/bin/env python3
"""
ANGLE WIN AIOS — Hub trung tâm (FastAPI + SQLite)
=================================================
Đây là mảnh ghép nối 3 thành phần thành một hệ thống end-to-end:

    n8n (cron)  ─┐
                 ├──► [AIOS HUB]  ◄──►  Dashboard (angle-win-aios-v2.html)
    CrewAI agent ┘        │
                          └─ SQLite (aios.db)

Hub đảm nhiệm:
  • Phục vụ dashboard tại  GET  /
  • Nhận webhook từ n8n + crewai:
        POST /queue            — angle briefs vào hàng chờ duyệt
        POST /win  /flop       — verdict hiệu suất từ TikTok analytics
        POST /competitor-data  — dữ liệu đối thủ từ Apify
  • Sinh angle thật/mock:
        POST /generate-angles  — (CREWAI_ENDPOINT) gọi pipeline CrewAI
  • API cho dashboard đồng bộ:
        GET  /api/state            — toàn bộ state (queue/angles/competitor)
        POST /api/approve|/reject  — đẩy hành động duyệt từ UI về hub
        POST /api/generate         — nút "Generate Queue" gọi backend thật
        GET  /api/health           — kiểm tra kết nối + mode

Chạy:  uvicorn aios_server:app --host 0.0.0.0 --port 8800
Hoặc:  python aios_server.py
"""

import os
import json
import time
import asyncio
import sqlite3
import logging
import threading
from pathlib import Path
from datetime import datetime
from contextlib import closing

from fastapi import FastAPI, Request, HTTPException, Header, Depends
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware

# Import lõi sinh angle (đã refactor để import được, có mock fallback)
import crewai_angle_agent as crew

# requests để forward log-event sang N8N (fire-and-forget). Optional.
try:
    import requests
except Exception:
    requests = None

# ──────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent
DB_PATH = Path(os.getenv("AIOS_DB", BASE_DIR / "aios.db"))
DASHBOARD_FILE = BASE_DIR / os.getenv("AIOS_DASHBOARD", "angle-win-aios-v2.html")
PRODUCT_NAME = os.getenv("PRODUCT_NAME", crew.PRODUCT_NAME)

# P1 — Bảo mật:
# AIOS_TOKEN: nếu set, các webhook (n8n/Apify) phải gửi header X-AIOS-Token khớp.
# AIOS_STRICT_WS: nếu "1", chỉ chấp nhận workspace đã đăng ký (bảng workspaces) → ws lạ bị 403.
AIOS_TOKEN = os.getenv("AIOS_TOKEN", "")
STRICT_WS = os.getenv("AIOS_STRICT_WS", "").lower() in ("1", "true", "yes")

# Module A — Master Event Logging: N8N webhook nhận mọi event để ghi Google Sheets ở background.
N8N_MASTER_WEBHOOK = os.getenv("N8N_MASTER_WEBHOOK", "")

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("aios")

app = FastAPI(title="Angle Win AIOS Hub", version="1.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_methods=["*"], allow_headers=["*"],
)


# ──────────────────────────────────────────────
# DATABASE
# ──────────────────────────────────────────────
def db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


DEFAULT_WS = "default"


def init_db():
    """
    Tạo schema multi-tenant. Mọi bảng có cột workspace_id để cô lập dữ liệu nhóm.
    queue/angles dùng PRIMARY KEY ghép (workspace_id, id) → id trùng giữa các nhóm
    không đụng nhau. competitor/events dùng id tự tăng + cột workspace_id.
    """
    with closing(db()) as conn, conn:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS queue (
            workspace_id TEXT NOT NULL DEFAULT 'default',
            id TEXT NOT NULL,
            hook TEXT, p1 TEXT, p2 TEXT, p3 TEXT, p4 TEXT, p5 TEXT, p6 TEXT,
            qc REAL, status TEXT DEFAULT 'pending', reason TEXT DEFAULT '',
            painSource TEXT, sourceTag TEXT, source TEXT, ts TEXT, raw TEXT,
            cluster_id TEXT, approach TEXT, platform TEXT DEFAULT 'TikTok',
            PRIMARY KEY (workspace_id, id)
        );
        CREATE TABLE IF NOT EXISTS angles (
            workspace_id TEXT NOT NULL DEFAULT 'default',
            id TEXT NOT NULL,
            hook TEXT, p1 TEXT, p2 TEXT, p3 TEXT, p4 TEXT, p5 TEXT, p6 TEXT,
            qcScore REAL, status TEXT DEFAULT 'draft', result TEXT,
            hookRate REAL, comments INTEGER, sales INTEGER,
            date TEXT, fromQueue INTEGER DEFAULT 0, ts TEXT,
            approach TEXT, platform TEXT DEFAULT 'TikTok',
            PRIMARY KEY (workspace_id, id)
        );
        CREATE TABLE IF NOT EXISTS competitor (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            workspace_id TEXT NOT NULL DEFAULT 'default',
            brand TEXT, url TEXT, hook TEXT, views INTEGER, likes INTEGER,
            date TEXT, ts TEXT
        );
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            workspace_id TEXT NOT NULL DEFAULT 'default',
            kind TEXT, payload TEXT, ts TEXT
        );
        CREATE TABLE IF NOT EXISTS workspaces (
            id TEXT PRIMARY KEY,
            name TEXT,
            product_name TEXT,
            product_desc TEXT,
            chroma_collection TEXT,
            created_ts TEXT
        );
        CREATE TABLE IF NOT EXISTS jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            workspace_id TEXT NOT NULL,
            kind TEXT DEFAULT 'generate',
            count INTEGER DEFAULT 5,
            status TEXT DEFAULT 'pending',   -- pending|running|done|error
            result_count INTEGER,
            error TEXT,
            created_ts TEXT, started_ts TEXT, done_ts TEXT
        );
        """)
        # Luôn có workspace 'default' để hệ thống chạy ngay
        conn.execute(
            "INSERT OR IGNORE INTO workspaces(id,name,product_name,product_desc,chroma_collection,created_ts) "
            "VALUES('default','Default', ?, ?, 'pain_bank', ?)",
            (PRODUCT_NAME, "", now_iso()))
        # Migration nhẹ: DB cũ (trước multi-tenant) thiếu cột workspace_id → thêm vào
        for tbl in ("queue", "angles", "competitor", "events"):
            cols = {r["name"] for r in conn.execute(f"PRAGMA table_info({tbl})")}
            if "workspace_id" not in cols:
                conn.execute(
                    f"ALTER TABLE {tbl} ADD COLUMN workspace_id TEXT NOT NULL DEFAULT 'default'")
                logger.info(f"Migration: thêm workspace_id vào bảng {tbl}")
        # Migration A/B Matrix + Multi-platform: queue/angles cần cluster_id/approach/platform
        qcols = {r["name"] for r in conn.execute("PRAGMA table_info(queue)")}
        for col in ("cluster_id", "approach", "platform"):
            if col not in qcols:
                conn.execute(f"ALTER TABLE queue ADD COLUMN {col} TEXT")
                logger.info(f"Migration: thêm {col} vào bảng queue")
        acols = {r["name"] for r in conn.execute("PRAGMA table_info(angles)")}
        for col in ("approach", "platform"):
            if col not in acols:
                conn.execute(f"ALTER TABLE angles ADD COLUMN {col} TEXT")
                logger.info(f"Migration: thêm {col} vào bảng angles")
        wcols = {r["name"] for r in conn.execute("PRAGMA table_info(workspaces)")}
        if "platform" not in wcols:
            conn.execute("ALTER TABLE workspaces ADD COLUMN platform TEXT DEFAULT 'TikTok'")
            logger.info("Migration: thêm platform vào bảng workspaces")
    logger.info(f"SQLite (multi-tenant) sẵn sàng tại {DB_PATH}")


def now_iso():
    return datetime.now().isoformat(timespec="seconds")


# ──────────────────────────────────────────────
# WORKSPACE registry helpers (P1/P2)
# ──────────────────────────────────────────────
def ws_get(ws_id: str):
    """Trả row workspaces (dict) hoặc None."""
    with closing(db()) as conn:
        r = conn.execute("SELECT * FROM workspaces WHERE id=?", (ws_id,)).fetchone()
        return dict(r) if r else None


def ws_upsert(ws_id: str, name: str = None, product_name: str = None,
              product_desc: str = None, chroma_collection: str = None,
              platform: str = None):
    """Đăng ký / cập nhật một workspace (giữ giá trị cũ nếu field None)."""
    cur = ws_get(ws_id) or {}
    with closing(db()) as conn, conn:
        conn.execute(
            "INSERT OR REPLACE INTO workspaces"
            "(id,name,product_name,product_desc,chroma_collection,platform,created_ts) "
            "VALUES(?,?,?,?,?,?,?)",
            (ws_id,
             name if name is not None else cur.get("name") or ws_id,
             product_name if product_name is not None else cur.get("product_name") or PRODUCT_NAME,
             product_desc if product_desc is not None else cur.get("product_desc") or "",
             chroma_collection if chroma_collection is not None else cur.get("chroma_collection") or "pain_bank",
             platform if platform is not None else cur.get("platform") or "TikTok",
             cur.get("created_ts") or now_iso()))
    return ws_get(ws_id)


# ──────────────────────────────────────────────
# DEPENDENCY INJECTION — workspace + token (P1)
# ──────────────────────────────────────────────
def get_workspace(x_workspace_id: str = Header(default=DEFAULT_WS, alias="X-Workspace-ID")) -> str:
    """
    Trích workspace_id từ header client. Thiếu → 'default'.
    AIOS_STRICT_WS=1: chỉ chấp nhận ws đã đăng ký (bảng workspaces) → ws lạ 403.
    Mặc định (dev): ws lạ được tự đăng ký để hệ thống vẫn chạy.
    """
    ws = (x_workspace_id or "").strip() or DEFAULT_WS
    if ws_get(ws) is None:
        if STRICT_WS:
            raise HTTPException(403, f"workspace '{ws}' chưa đăng ký")
        ws_upsert(ws)  # dev: auto-register silo mới
    return ws


def get_workspace_raw(x_workspace_id: str = Header(default=DEFAULT_WS, alias="X-Workspace-ID")) -> str:
    """Trích ws KHÔNG enforce allowlist — dùng cho endpoint đăng ký ws (bootstrap)."""
    return (x_workspace_id or "").strip() or DEFAULT_WS


def require_token(x_aios_token: str = Header(default="", alias="X-AIOS-Token")) -> bool:
    """Chốt bảo mật webhook nội bộ (n8n/Apify). AIOS_TOKEN rỗng → bỏ qua (dev)."""
    if AIOS_TOKEN and x_aios_token != AIOS_TOKEN:
        raise HTTPException(401, "X-AIOS-Token sai hoặc thiếu")
    return True


# ──────────────────────────────────────────────
# RBAC — phân quyền theo header X-User-Role (viewer < editor < admin)
# ──────────────────────────────────────────────
ROLE_RANK = {"viewer": 0, "editor": 1, "admin": 2}


def get_role(x_user_role: str = Header(default="editor", alias="X-User-Role")) -> str:
    r = (x_user_role or "editor").strip().lower()
    return r if r in ROLE_RANK else "editor"


def require_editor(role: str = Depends(get_role)) -> str:
    """Viewer chỉ được xem → chặn thao tác ghi (sinh/duyệt/loại)."""
    if ROLE_RANK[role] < ROLE_RANK["editor"]:
        raise HTTPException(403, "Cần quyền Editor trở lên cho thao tác này")
    return role


def require_admin(role: str = Depends(get_role)) -> str:
    """Chỉ Admin (vd: clone chéo team)."""
    if ROLE_RANK[role] < ROLE_RANK["admin"]:
        raise HTTPException(403, "Cần quyền Admin cho thao tác này")
    return role


def log_event(kind: str, payload: dict, ws: str = DEFAULT_WS):
    with closing(db()) as conn, conn:
        conn.execute(
            "INSERT INTO events(workspace_id,kind,payload,ts) VALUES(?,?,?,?)",
            (ws, kind, json.dumps(payload, ensure_ascii=False), now_iso()))


# ──────────────────────────────────────────────
# MODULE A — MASTER EVENT LOGGING (fire-and-forget → N8N → Google Sheets)
# ──────────────────────────────────────────────
def _forward_to_n8n(packet: dict):
    """
    Bắn 1 gói event sang N8N Master Webhook ở BACKGROUND (daemon thread).
    Tuyệt đối KHÔNG block luồng chính: lỗi mạng/timeout chỉ ghi debug.
    N8N chịu trách nhiệm ghi Google Sheets hoàn toàn ở background.
    """
    if not N8N_MASTER_WEBHOOK or requests is None:
        return

    def _send():
        try:
            requests.post(N8N_MASTER_WEBHOOK, json=packet, timeout=5)
        except Exception as e:  # nuốt mọi lỗi — log không được phép làm gãy hệ thống
            logger.debug(f"forward N8N lỗi (bỏ qua): {e}")

    threading.Thread(target=_send, daemon=True).start()


def master_log(ws: str, event_type: str, angle_id=None, payload=None):
    """
    Lưu vết 1 thao tác đổi trạng thái: ghi events (audit nội bộ) + bắn sang N8N.
    Gói chuẩn: [Timestamp, X-Workspace-ID, EventType, Angle_ID, Payload].
    """
    packet = {
        "timestamp": now_iso(),
        "workspace_id": ws,
        "eventType": event_type,
        "angleId": angle_id,
        "payload": payload or {},
    }
    log_event(event_type, packet, ws)   # audit nội bộ (events table)
    _forward_to_n8n(packet)             # fire-and-forget sang N8N
    return packet


# ──────────────────────────────────────────────
# NORMALIZER: brief CrewAI → queue item (khớp field dashboard)
# ──────────────────────────────────────────────
def first_beat(script_outline):
    if isinstance(script_outline, list) and script_outline:
        return str(script_outline[0])
    if isinstance(script_outline, str):
        return script_outline
    return ""


def qc_to_10(qc):
    """Chuẩn hoá QC score về thang /10 (chấp nhận input /50 hoặc /10)."""
    try:
        qc = float(qc)
    except (TypeError, ValueError):
        return 7
    if qc > 10:          # giả định thang /50
        qc = qc / 5.0
    return round(qc, 1)


def brief_to_queue_item(brief: dict, product: str) -> dict:
    """Map một angle brief (output crewai) sang queue item 6P của dashboard.
    Lưu ý: p2 = PAIN thật (không lấy angle_name); p4 = PROMISE thật (không lấy script beat)."""
    if not isinstance(brief, dict):
        brief = {"angle_name": str(brief)}
    pain = (brief.get("pain") or "").strip()
    # P5 nên là loại proof ngắn gọn (Testimonial/Before-After...), không phải cả câu visual_notes
    proof = brief.get("proof") or brief.get("proof_type") or brief.get("visual_notes") or "Demo"
    return {
        "id": str(brief.get("angle_id") or crew_uid()),
        "hook": brief.get("pain_hook") or brief.get("hook") or "",
        "p1": brief.get("persona") or "",
        "p2": pain[:80],                                   # Pain Point = pain thật
        "p3": brief.get("moment") or brief.get("setting") or "",
        "p4": (brief.get("promise") or "Cải thiện rõ rệt, dễ làm theo")[:80],
        "p5": str(proof)[:40],
        "p6": brief.get("cta") or "Link trong bio",
        "qc": qc_to_10(brief.get("qc_score")),
        "status": "pending",
        "reason": "",
        "painSource": pain[:120],
        "sourceTag": f"AI · {product}",
        "source": "crewai",
        "ts": now_iso(),
        "cluster_id": brief.get("cluster_id"),
        "approach": brief.get("approach"),
        "platform": brief.get("platform") or "TikTok",
        "raw": json.dumps(brief, ensure_ascii=False),
    }


def crew_uid():
    import uuid
    return uuid.uuid4().hex[:8]


def insert_queue_items(items: list, ws: str = DEFAULT_WS) -> int:
    cols = ("id", "hook", "p1", "p2", "p3", "p4", "p5", "p6", "qc",
            "status", "reason", "painSource", "sourceTag", "source", "ts", "raw",
            "cluster_id", "approach", "platform")
    allcols = cols + ("workspace_id",)
    n = 0
    with closing(db()) as conn, conn:
        for it in items:
            conn.execute(
                f"INSERT OR REPLACE INTO queue({','.join(allcols)}) "
                f"VALUES({','.join('?' * len(allcols))})",
                tuple(it.get(c) for c in cols) + (ws,),
            )
            n += 1
    return n


# ──────────────────────────────────────────────
# WEBHOOK INGEST  (gọi bởi n8n + crewai)
# ──────────────────────────────────────────────
@app.post("/queue")
async def webhook_queue(req: Request, ws: str = Depends(get_workspace),
                        _tok: bool = Depends(require_token)):
    """
    Nhận angle briefs. Chấp nhận 2 dạng body:
      • {product, angles: [brief, ...]}   (output crewai)
      • [brief, ...]                       (list trực tiếp)
    """
    body = await req.json()
    if isinstance(body, list):
        angles, product = body, PRODUCT_NAME
    else:
        angles = body.get("angles") or body.get("data") or []
        product = body.get("product") or PRODUCT_NAME
    items = [brief_to_queue_item(b, product) for b in angles]
    count = insert_queue_items(items, ws)
    log_event("queue", {"received": len(angles), "inserted": count, "product": product}, ws)
    logger.info(f"/queue [{ws}] ← {count} angles vào hàng chờ (product={product})")
    return {"ok": True, "queued": count, "workspace": ws}


def _record_verdict(body: dict, verdict: str, ws: str):
    angle_id = str(body.get("angleId") or body.get("ad_id") or body.get("angle_id") or crew_uid())
    hook_rate = body.get("hookRate") or body.get("hook_rate")
    comments = body.get("intentComments") or body.get("intent_comments") or body.get("comments")
    sales = body.get("sales")
    ts = body.get("timestamp") or now_iso()

    def num(v, cast=float):
        try:
            return cast(v)
        except (TypeError, ValueError):
            return None

    with closing(db()) as conn, conn:
        row = conn.execute(
            "SELECT id FROM angles WHERE id=? AND workspace_id=?", (angle_id, ws)).fetchone()
        if row:
            conn.execute(
                "UPDATE angles SET result=?, hookRate=?, comments=?, sales=?, status='tested' "
                "WHERE id=? AND workspace_id=?",
                (verdict, num(hook_rate), num(comments, int), num(sales, int), angle_id, ws),
            )
        else:
            conn.execute(
                "INSERT INTO angles(workspace_id,id,hook,qcScore,status,result,hookRate,comments,sales,date,fromQueue,ts) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                (ws, angle_id, f"(từ analytics) {angle_id}", None, "tested", verdict,
                 num(hook_rate), num(comments, int), num(sales, int),
                 ts[:10], 0, now_iso()),
            )
    master_log(ws, verdict, angle_id,
               {"hookRate": hook_rate, "comments": comments, "sales": sales})
    logger.info(f"/{verdict} [{ws}] ← angle {angle_id} (hook={hook_rate}, sales={sales})")
    return {"ok": True, "angleId": angle_id, "verdict": verdict, "workspace": ws}


@app.post("/win")
async def webhook_win(req: Request, ws: str = Depends(get_workspace),
                      _tok: bool = Depends(require_token)):
    return _record_verdict(await req.json(), "win", ws)


@app.post("/flop")
async def webhook_flop(req: Request, ws: str = Depends(get_workspace),
                       _tok: bool = Depends(require_token)):
    return _record_verdict(await req.json(), "flop", ws)


@app.post("/competitor-data")
async def webhook_competitor(req: Request, ws: str = Depends(get_workspace),
                             _tok: bool = Depends(require_token)):
    body = await req.json()
    videos = body.get("competitor_videos") or body.get("videos") or []
    collected = body.get("collected_at") or now_iso()
    with closing(db()) as conn, conn:
        for v in videos:
            conn.execute(
                "INSERT INTO competitor(workspace_id,brand,url,hook,views,likes,date,ts) "
                "VALUES(?,?,?,?,?,?,?,?)",
                (ws,
                 v.get("brand") or v.get("authorMeta", {}).get("name"),
                 v.get("url") or v.get("webVideoUrl"),
                 v.get("hook") or (v.get("text") or "")[:120],
                 v.get("views") or v.get("playCount"),
                 v.get("likes") or v.get("diggCount"),
                 v.get("date") or collected[:10], now_iso()),
            )
    log_event("competitor-data", {"count": len(videos)}, ws)
    logger.info(f"/competitor-data [{ws}] ← {len(videos)} video đối thủ")
    return {"ok": True, "stored": len(videos), "workspace": ws}


# ──────────────────────────────────────────────
# GENERATE  (CREWAI_ENDPOINT — gọi bởi n8n WF2; cũng dùng cho nút UI)
# ──────────────────────────────────────────────
def _extract_pains(raw_pains) -> list:
    """n8n gửi raw output Chroma {documents:[[...]]} hoặc list string."""
    if isinstance(raw_pains, dict):
        docs = raw_pains.get("documents")
        if isinstance(docs, list) and docs and isinstance(docs[0], list):
            return [str(x) for x in docs[0]]
        return []
    if isinstance(raw_pains, list):
        return [str(x) for x in raw_pains]
    return []


@app.post("/generate-angles")
async def generate_angles(req: Request, ws: str = Depends(get_workspace),
                          _tok: bool = Depends(require_token)):
    body = await req.json() if await req.body() else {}
    pains = _extract_pains(body.get("pains")) or None
    count = int(body.get("count") or 5)
    # P2: product + pain-bank collection của riêng workspace; body có thể override
    wsrow = ws_get(ws) or {}
    product = body.get("product") or wsrow.get("product_name") or PRODUCT_NAME
    collection = wsrow.get("chroma_collection") or None

    logger.info(f"/generate-angles [{ws}] → sinh {count} angles cho '{product}' "
                f"(collection={collection}, mode={'mock' if crew.MOCK_MODE else 'crewai'})")
    result = crew.generate_angles_from_pains(pains=pains, product=product,
                                             count=count, collection=collection)

    # Tự đẩy thẳng vào hàng chờ của đúng workspace để dashboard thấy ngay
    items = [brief_to_queue_item(b, product) for b in result["angles"]]
    queued = insert_queue_items(items, ws)
    result["queued"] = queued
    result["workspace"] = ws
    log_event("generate", {"count": result["total_angles"], "mode": result["mode"]}, ws)
    return JSONResponse(result)


# ──────────────────────────────────────────────
# API CHO DASHBOARD
# ──────────────────────────────────────────────
def rows(table, ws, tail="", params=()):
    """SELECT luôn gắn workspace_id=? — cô lập dữ liệu nhóm.
    tail: phần thêm sau (vd "AND kind IN (...) ORDER BY id DESC LIMIT ?")."""
    with closing(db()) as conn:
        cur = conn.execute(
            f"SELECT * FROM {table} WHERE workspace_id=? {tail}", (ws, *params))
        return [dict(r) for r in cur.fetchall()]


@app.get("/api/health")
async def health(ws: str = Depends(get_workspace)):
    return {"ok": True, "mode": "mock" if crew.MOCK_MODE else "crewai",
            "product": PRODUCT_NAME, "workspace": ws, "ts": now_iso()}


# ──────────────────────────────────────────────
# AUTO-LEARNING — học từ Win/Flop của workspace để bias sinh angle
# ──────────────────────────────────────────────
def learning_summary(ws: str) -> dict:
    """Tổng hợp win-rate theo approach & persona (chỉ angle đã có kết quả) của 1 ws."""
    tested = rows("angles", ws, "AND result IN ('win','flop')")

    def agg(key):
        m = {}
        for a in tested:
            k = (a.get(key) or "").strip()
            if not k:
                continue
            d = m.setdefault(k, {"wins": 0, "total": 0})
            d["total"] += 1
            if a.get("result") == "win":
                d["wins"] += 1
        out = [{"key": k, "wins": v["wins"], "total": v["total"],
                "winRate": round(v["wins"] / v["total"] * 100)} for k, v in m.items()]
        out.sort(key=lambda x: (-x["winRate"], -x["total"]))
        return out

    by_approach = agg("approach")
    by_persona = agg("p1")
    # Bias chỉ khi đủ dữ liệu (>=2 mẫu) để tránh học vội
    bias_approach = by_approach[0]["key"] if by_approach and by_approach[0]["total"] >= 2 else None
    bias_persona = by_persona[0]["key"] if by_persona and by_persona[0]["total"] >= 2 else None
    return {
        "samples": len(tested),
        "byApproach": by_approach,
        "byPersona": by_persona,
        "bias": {"approach": bias_approach, "persona": bias_persona},
    }


@app.get("/api/insights")
async def api_insights(ws: str = Depends(get_workspace)):
    """Trả insight Auto-learning cho dashboard hiển thị + biết hệ thống đang bias gì."""
    return learning_summary(ws)


def _bucket_key_label(ts: str, bucket: str):
    """Trả (key sắp xếp được, nhãn hiển thị) cho 1 timestamp theo mốc."""
    ts = str(ts or "")
    try:
        dt = datetime.fromisoformat(ts.replace("Z", ""))
    except Exception:
        dt = None
    if bucket == "day":
        if dt:
            return dt.strftime("%Y-%m-%d"), dt.strftime("%d/%m")
        return ts[:10], ts[5:10]
    if bucket == "week":
        if dt:
            y, w, _ = dt.isocalendar()
            return f"{y}-W{w:02d}", f"T{w}/{y}"
        return ts[:10], ts[:10]
    if bucket == "month":
        if dt:
            return dt.strftime("%Y-%m"), dt.strftime("%m/%Y")
        return ts[:7], ts[:7]
    # mặc định: giờ
    if dt:
        return dt.strftime("%Y-%m-%d %H"), dt.strftime("%d/%m %Hh")
    return ts[:13], ts[11:13] + "h"


@app.get("/api/verdicts")
async def api_verdicts(limit: int = 50, bucket: str = "hour",
                       from_ts: str = None, to_ts: str = None,
                       ws: str = Depends(get_workspace)):
    """
    Feed WIN/FLOP realtime + timeline gộp theo mốc thời gian (chỉ của workspace này).
    bucket ∈ {hour, day, week, month} (mặc định hour).
    from_ts/to_ts: lọc khoảng ngày (YYYY-MM-DD hoặc ISO). to_ts dạng ngày → tự +cuối ngày.
    """
    if bucket not in ("hour", "day", "week", "month"):
        bucket = "hour"

    # Mệnh đề lọc khoảng ngày dùng chung cho feed + timeline
    date_clause, date_params = "", []
    if from_ts:
        date_clause += " AND ts >= ?"
        date_params.append(from_ts)
    if to_ts:
        to_bound = (to_ts + "T23:59:59") if len(to_ts) == 10 else to_ts
        date_clause += " AND ts <= ?"
        date_params.append(to_bound)

    def parse(e):
        try:
            p = json.loads(e["payload"])
        except Exception:
            p = {}
        # Hỗ trợ cả 2 shape: phẳng (cũ) và gói master_log (payload lồng trong 'payload')
        src = {**p, **(p.get("payload") or {})} if isinstance(p, dict) else {}
        return {
            "verdict": e["kind"],
            "angleId": src.get("angleId") or src.get("ad_id") or src.get("angle_id"),
            "hookRate": src.get("hookRate") or src.get("hook_rate"),
            "comments": src.get("intentComments") or src.get("intent_comments") or src.get("comments"),
            "sales": src.get("sales"),
            "spend": src.get("spend"),
            "ts": src.get("timestamp") or e["ts"],
        }

    # Feed: N verdict gần nhất (DESC) — chỉ của workspace này, trong khoảng ngày
    feed = [parse(e) for e in
            rows("events", ws, f"AND kind IN ('win','flop'){date_clause} ORDER BY id DESC LIMIT ?",
                 (*date_params, limit))]

    # Buckets: gộp TOÀN BỘ verdict của workspace theo mốc (ASC) để xem xu hướng dài hạn
    all_evs = [parse(e) for e in
               rows("events", ws, f"AND kind IN ('win','flop'){date_clause} ORDER BY id ASC",
                    tuple(date_params))]
    agg = {}
    order = []
    for f in all_evs:
        key, label = _bucket_key_label(f["ts"], bucket)
        if key not in agg:
            agg[key] = {"key": key, "label": label, "wins": 0, "flops": 0}
            order.append(key)
        if f["verdict"] == "win":
            agg[key]["wins"] += 1
        else:
            agg[key]["flops"] += 1
    timeline = []
    for key in sorted(order):
        b = agg[key]
        tot = b["wins"] + b["flops"]
        timeline.append({**b, "total": tot,
                         "winRate": round(b["wins"] / tot * 100) if tot else 0})

    wins = sum(1 for f in all_evs if f["verdict"] == "win")
    flops = len(all_evs) - wins
    total = wins + flops
    return {
        "summary": {"wins": wins, "flops": flops, "total": total,
                    "winRate": round(wins / total * 100) if total else 0},
        "bucket": bucket,
        "feed": feed,
        "timeline": timeline,
    }


@app.get("/api/state")
async def api_state(ws: str = Depends(get_workspace)):
    """Trả state CỦA RIÊNG workspace để dashboard hydrate (merge vào S)."""
    return {
        "meta": {"mode": "mock" if crew.MOCK_MODE else "crewai",
                 "product": PRODUCT_NAME, "workspace": ws, "syncedAt": now_iso()},
        "queue": rows("queue", ws, "ORDER BY ts DESC"),
        "angles": rows("angles", ws, "ORDER BY ts DESC"),
        "competitor": rows("competitor", ws, "ORDER BY ts DESC LIMIT 100"),
    }


@app.post("/api/approve")
async def api_approve(req: Request, ws: str = Depends(get_workspace),
                      _role: str = Depends(require_editor)):
    body = await req.json()
    qid = body.get("id")
    if not qid:
        raise HTTPException(400, "thiếu id")
    auto_rejected = []
    cid = None
    with closing(db()) as conn, conn:
        q = conn.execute(
            "SELECT * FROM queue WHERE id=? AND workspace_id=?", (qid, ws)).fetchone()
        if not q:
            raise HTTPException(404, "không tìm thấy queue item trong workspace")
        conn.execute(
            "UPDATE queue SET status='approved' WHERE id=? AND workspace_id=?", (qid, ws))
        qd = dict(q)
        conn.execute(
            "INSERT OR REPLACE INTO angles"
            "(workspace_id,id,hook,p1,p2,p3,p4,p5,p6,qcScore,status,result,date,fromQueue,ts,approach,platform) "
            "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (ws, q["id"], q["hook"], q["p1"], q["p2"], q["p3"], q["p4"], q["p5"], q["p6"],
             q["qc"], "draft", None, now_iso()[:10], 1, now_iso(),
             qd.get("approach"), qd.get("platform") or "TikTok"),
        )
        # Module D: chọn 1 variant → tự reject 2 variant còn lại cùng cluster
        cid = q["cluster_id"] if "cluster_id" in q.keys() else None
        if cid:
            sibs = conn.execute(
                "SELECT id FROM queue WHERE cluster_id=? AND workspace_id=? AND id!=? AND status='pending'",
                (cid, ws, qid)).fetchall()
            for s in sibs:
                conn.execute(
                    "UPDATE queue SET status='rejected', "
                    "reason='Auto-reject: đã chọn variant khác trong cụm' "
                    "WHERE id=? AND workspace_id=?", (s["id"], ws))
                auto_rejected.append(s["id"])
    master_log(ws, "approve", qid, {"cluster": cid})
    for rid in auto_rejected:
        master_log(ws, "reject", rid, {"auto": True, "cluster": cid})
    return {"ok": True, "id": qid, "workspace": ws, "auto_rejected": auto_rejected}


@app.post("/api/reject")
async def api_reject(req: Request, ws: str = Depends(get_workspace),
                     _role: str = Depends(require_editor)):
    body = await req.json()
    qid = body.get("id")
    reason = body.get("reason", "")
    if not qid:
        raise HTTPException(400, "thiếu id")
    with closing(db()) as conn, conn:
        conn.execute(
            "UPDATE queue SET status='rejected', reason=? WHERE id=? AND workspace_id=?",
            (reason, qid, ws))
    master_log(ws, "reject", qid, {"reason": reason})
    return {"ok": True, "id": qid, "workspace": ws}


@app.post("/api/generate")
async def api_generate(req: Request, ws: str = Depends(get_workspace),
                       _role: str = Depends(require_editor)):
    """Nút 'Generate Queue' trên dashboard → sinh angle thật qua backend (đúng workspace)."""
    body = await req.json() if await req.body() else {}
    count = int(body.get("count") or 5)
    pains = _extract_pains(body.get("pains")) or None
    # P2 + Multi-platform + Auto-learning: context riêng nhóm
    wsrow = ws_get(ws) or {}
    product = wsrow.get("product_name") or PRODUCT_NAME
    collection = wsrow.get("chroma_collection") or None
    platform = body.get("platform") or wsrow.get("platform") or "TikTok"
    bias = learning_summary(ws)["bias"]
    result = crew.generate_angles_from_pains(
        pains=pains, product=product, count=count, collection=collection,
        bias_persona=bias["persona"], platform=platform)
    items = [brief_to_queue_item(b, product) for b in result["angles"]]
    queued = insert_queue_items(items, ws)
    return {"ok": True, "queued": queued, "mode": result["mode"], "workspace": ws,
            "product": product, "platform": platform, "bias": bias, "items": items}


@app.get("/api/workspaces")
async def api_list_workspaces():
    """Danh sách workspace đã đăng ký (cho admin/allowlist)."""
    with closing(db()) as conn:
        return [dict(r) for r in conn.execute(
            "SELECT * FROM workspaces ORDER BY created_ts").fetchall()]


@app.post("/api/workspace")
async def api_register_workspace(req: Request, ws: str = Depends(get_workspace_raw)):
    """
    Dashboard gọi khi mở/chuyển nhóm → đăng ký ws + product của nhóm vào hub.
    Nhờ vậy P2 (AI context theo nhóm) và allowlist (P1) mới có dữ liệu.
    """
    body = await req.json() if await req.body() else {}
    row = ws_upsert(
        ws,
        name=body.get("name"),
        product_name=body.get("product") or body.get("product_name"),
        product_desc=body.get("product_desc"),
        chroma_collection=body.get("chroma_collection"),
        platform=body.get("platform"),
    )
    logger.info(f"/api/workspace ← đăng ký '{ws}' (product={row.get('product_name')})")
    return {"ok": True, "workspace": row}


@app.post("/api/clone-angle")
async def api_clone_angle(req: Request, ws: str = Depends(get_workspace),
                          _role: str = Depends(require_admin)):
    """
    Admin clone 1 angle (đang Win ở nhóm khác) sang workspace đích (header X-Workspace-ID = đích).
    Tạo queue item mới 'pending' để nhóm đích review & chạy thử.
    """
    body = await req.json()
    a = body.get("angle") or body.get("item") or {}
    item = {
        "id": crew_uid(),
        "hook": a.get("hook", ""),
        "p1": a.get("p1", ""), "p2": a.get("p2", ""), "p3": a.get("p3", ""),
        "p4": a.get("p4", ""), "p5": a.get("p5", ""), "p6": a.get("p6", "Link trong bio"),
        "qc": a.get("qc") or a.get("qcScore") or 8,
        "status": "pending", "reason": "",
        "painSource": a.get("painSource", ""),
        "sourceTag": body.get("sourceTag") or "Clone chéo team",
        "source": "clone", "ts": now_iso(),
        "raw": json.dumps(a, ensure_ascii=False),
    }
    n = insert_queue_items([item], ws)
    master_log(ws, "clone", item["id"], {"from": body.get("fromTeam"), "hook": item["hook"][:60]})
    logger.info(f"/api/clone-angle → clone sang '{ws}': {item['hook'][:50]}")
    return {"ok": True, "queued": n, "workspace": ws, "id": item["id"]}


# ──────────────────────────────────────────────
# MODULE A — endpoint nhận log-event từ Frontend (fire-and-forget)
# ──────────────────────────────────────────────
@app.post("/api/log-event")
async def api_log_event(req: Request, ws: str = Depends(get_workspace)):
    """FE bắn mọi thao tác đổi trạng thái về đây → master_log (audit + N8N)."""
    body = await req.json() if await req.body() else {}
    master_log(ws, body.get("eventType", "unknown"),
               body.get("angleId"), body.get("payload"))
    return {"ok": True}


# ──────────────────────────────────────────────
# MODULE B + C — SSE stream sinh A/B Matrix (3 variants/cluster)
# ──────────────────────────────────────────────
@app.post("/api/generate-stream")
async def api_generate_stream(req: Request, ws: str = Depends(get_workspace),
                              _role: str = Depends(require_editor)):
    body = await req.json() if await req.body() else {}
    count = int(body.get("count") or 3)
    wsrow = ws_get(ws) or {}
    product = wsrow.get("product_name") or PRODUCT_NAME
    collection = wsrow.get("chroma_collection") or None
    platform = body.get("platform") or wsrow.get("platform") or "TikTok"
    bias = learning_summary(ws)["bias"]   # Auto-learning: hướng đang thắng

    async def event_stream():
        # ── SSE: mỗi frame là 1 dòng "data: {json}\n\n" ──
        def sse(obj):
            return f"data: {json.dumps(obj, ensure_ascii=False)}\n\n"

        # Phát trạng thái "suy nghĩ" của AI để FE cập nhật nút bấm realtime
        yield sse({"stage": "analyze", "message": "Đang phân tích Pain..."})
        await asyncio.sleep(0.5)
        yield sse({"stage": "hook", "message": "Đang viết Hook..."})
        await asyncio.sleep(0.5)
        yield sse({"stage": "variants",
                   "message": "Đang dựng 3 biến thể Logical / Emotional / Curiosity..."})

        # Sinh matrix ngoài event-loop (không block) — A/B 3 variants/cluster
        # Auto-learning: bias_approach đẩy hướng thắng lên đầu · Multi-platform: platform
        matrix = await asyncio.to_thread(
            crew.generate_variant_matrix, None, product, count, collection,
            bias["approach"], platform)

        out = []
        for cl in matrix["clusters"]:
            cid = crew_uid()
            items = []
            for v in cl["variants"]:
                it = brief_to_queue_item(v, product)
                it["cluster_id"] = cid          # gộp 3 variant vào 1 cụm
                it["approach"] = v.get("approach")
                items.append(it)
            insert_queue_items(items, ws)        # lưu đúng workspace
            out.append({"cluster_id": cid, "pain": cl.get("pain"), "items": items})

        # Lưu vết event sinh AI (Module A)
        master_log(ws, "ai_generate", None,
                   {"clusters": len(out), "variants": matrix["total_variants"]})

        await asyncio.sleep(0.3)
        yield sse({"stage": "done", "message": "Hoàn tất!",
                   "clusters": out, "total": matrix["total_variants"],
                   "mode": matrix["mode"], "workspace": ws})

    # text/event-stream + tắt buffering để FE nhận từng frame ngay
    return StreamingResponse(event_stream(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache",
                                      "X-Accel-Buffering": "no"})


# ──────────────────────────────────────────────
# P4 — FAIR-SHARE JOB QUEUE cho CrewAI
# Nhiều nhóm cùng xin tạo angle → worker xử lý XOAY VÒNG theo workspace
# (không FIFO) để không nhóm nào bị treo chờ quá lâu.
# ──────────────────────────────────────────────
_serve_seq = 0                 # bộ đếm thứ tự phục vụ (round-robin, không dùng clock)
_ws_last_served = {}           # ws -> seq lần cuối được phục vụ (-1 = chưa bao giờ)
_jobs_lock = threading.Lock()
_worker_started = False


def _pick_fair(pending: list, last_served: dict):
    """
    Chọn job kế tiếp theo fair-share: ưu tiên ws được phục vụ lâu nhất (seq nhỏ nhất),
    tie-break theo job id (FIFO trong cùng nhóm). pending: list dict có 'id','workspace_id'.
    Hàm THUẦN (dễ test) — trả về job được chọn hoặc None.
    """
    if not pending:
        return None
    return min(pending, key=lambda j: (last_served.get(j["workspace_id"], -1), j["id"]))


def enqueue_job(ws: str, count: int = 5, kind: str = "generate") -> int:
    with closing(db()) as conn, conn:
        cur = conn.execute(
            "INSERT INTO jobs(workspace_id,kind,count,status,created_ts) VALUES(?,?,?, 'pending', ?)",
            (ws, kind, count, now_iso()))
        return cur.lastrowid


def _run_job(job: dict):
    """Chạy 1 job generate cho đúng workspace của nó (giống /api/generate)."""
    ws = job["workspace_id"]
    wsrow = ws_get(ws) or {}
    product = wsrow.get("product_name") or PRODUCT_NAME
    collection = wsrow.get("chroma_collection") or None
    result = crew.generate_angles_from_pains(
        pains=None, product=product, count=job["count"], collection=collection)
    items = [brief_to_queue_item(b, product) for b in result["angles"]]
    return insert_queue_items(items, ws)


def _worker_tick() -> bool:
    """Xử lý 1 job theo fair-share. Trả True nếu có job được chạy."""
    global _serve_seq
    with _jobs_lock:
        pending = rows_all("jobs", "WHERE status='pending' ORDER BY id ASC")
        job = _pick_fair(pending, _ws_last_served)
        if not job:
            return False
        with closing(db()) as conn, conn:
            conn.execute("UPDATE jobs SET status='running', started_ts=? WHERE id=?",
                         (now_iso(), job["id"]))
    # Chạy ngoài lock (generation có thể lâu khi dùng CrewAI thật)
    try:
        n = _run_job(job)
        with closing(db()) as conn, conn:
            conn.execute("UPDATE jobs SET status='done', result_count=?, done_ts=? WHERE id=?",
                         (n, now_iso(), job["id"]))
        status = f"done ({n} angles)"
    except Exception as e:
        with closing(db()) as conn, conn:
            conn.execute("UPDATE jobs SET status='error', error=?, done_ts=? WHERE id=?",
                         (str(e), now_iso(), job["id"]))
        status = f"error: {e}"
    with _jobs_lock:
        _serve_seq += 1
        _ws_last_served[job["workspace_id"]] = _serve_seq
    logger.info(f"[job {job['id']}] ws={job['workspace_id']} → {status}")
    return True


def _worker_loop():
    while True:
        try:
            if not _worker_tick():
                time.sleep(2)   # rảnh → ngủ nhẹ
        except Exception as e:
            logger.error(f"worker lỗi: {e}")
            time.sleep(2)


def rows_all(table, tail="", params=()):
    """SELECT không lọc workspace (dùng nội bộ cho worker/admin)."""
    with closing(db()) as conn:
        return [dict(r) for r in conn.execute(
            f"SELECT * FROM {table} {tail}", params).fetchall()]


@app.post("/api/jobs")
async def api_enqueue_job(req: Request, ws: str = Depends(get_workspace),
                          _role: str = Depends(require_editor)):
    """Xếp 1 job generate vào hàng đợi fair-share (xử lý nền). Trả job_id."""
    body = await req.json() if await req.body() else {}
    count = int(body.get("count") or 5)
    job_id = enqueue_job(ws, count)
    logger.info(f"/api/jobs ← enqueue job {job_id} cho ws={ws} (count={count})")
    return {"ok": True, "job_id": job_id, "workspace": ws, "status": "pending"}


@app.get("/api/jobs")
async def api_list_jobs(ws: str = Depends(get_workspace)):
    """Danh sách job của workspace này."""
    return rows("jobs", ws, "ORDER BY id DESC LIMIT 50")


@app.get("/api/jobs/{job_id}")
async def api_job_status(job_id: int, ws: str = Depends(get_workspace)):
    with closing(db()) as conn:
        r = conn.execute(
            "SELECT * FROM jobs WHERE id=? AND workspace_id=?", (job_id, ws)).fetchone()
    if not r:
        raise HTTPException(404, "job không thuộc workspace này")
    return dict(r)


# ──────────────────────────────────────────────
# DASHBOARD
# ──────────────────────────────────────────────
@app.get("/", response_class=HTMLResponse)
async def dashboard():
    if not DASHBOARD_FILE.exists():
        return HTMLResponse(f"<h1>Không tìm thấy {DASHBOARD_FILE.name}</h1>", status_code=404)
    return HTMLResponse(DASHBOARD_FILE.read_text(encoding="utf-8"))


@app.on_event("startup")
def _startup():
    global _worker_started, _serve_seq
    init_db()
    # Khôi phục thứ tự fair-share từ job đã done (sau restart)
    for j in rows_all("jobs", "WHERE status='done' ORDER BY id ASC"):
        _serve_seq += 1
        _ws_last_served[j["workspace_id"]] = _serve_seq
    if not _worker_started:
        threading.Thread(target=_worker_loop, daemon=True, name="aios-job-worker").start()
        _worker_started = True
    logger.info("=" * 60)
    logger.info(f"AIOS Hub khởi động | mode={'MOCK' if crew.MOCK_MODE else 'CREWAI'} | product={PRODUCT_NAME}")
    logger.info(f"Dashboard: http://localhost:{os.getenv('AIOS_PORT', '8800')}/ | worker fair-share: ON")
    logger.info("=" * 60)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("aios_server:app", host="0.0.0.0",
                port=int(os.getenv("AIOS_PORT", "8800")), reload=False)

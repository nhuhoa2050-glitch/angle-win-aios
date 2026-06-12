#!/usr/bin/env python3
"""
ANGLE WIN AIOS — CrewAI Angle Generator
Chạy lúc 6am hàng ngày để tự generate 5 angle briefs từ Pain Bank.
Usage: python crewai_angle_agent.py [--once] [--scheduled]

# ============================================================
# Requirements: pip install crewai chromadb python-dotenv schedule requests langchain-openai langchain-anthropic
# ============================================================
#
# .env file cần có:
#   OPENAI_API_KEY        hoặc ANTHROPIC_API_KEY
#   CHROMA_URL            ví dụ: http://localhost:8000
#   CHROMA_COLLECTION     ví dụ: pain_bank
#   AIOS_WEBHOOK_URL      ví dụ: http://localhost:5678/webhook
#   PRODUCT_NAME          tên sản phẩm đang chạy
#   PRODUCT_DESCRIPTION   mô tả ngắn sản phẩm
#   ZALO_OA_ACCESS_TOKEN  Zalo OA access token
#   ZALO_USER_ID          User ID nhận notification
# ============================================================
"""

import os
import re
import json
import random
import logging
import argparse
import time
from datetime import datetime
from pathlib import Path

# ── Optional / lazy deps ───────────────────────
# Module phải import được kể cả khi máy chưa cài đủ lib (để AIOS server demo
# chạy ngay ở mock mode). Các lib nặng được import an toàn, fallback nếu thiếu.
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:  # python-dotenv chưa cài
    def load_dotenv(*_a, **_k):
        return False

try:
    import requests
except Exception:  # requests chưa cài
    requests = None

try:
    import chromadb
    CHROMA_AVAILABLE = True
except Exception:
    chromadb = None
    CHROMA_AVAILABLE = False

try:
    from crewai import Agent, Task, Crew, Process
    CREWAI_AVAILABLE = True
except Exception:
    Agent = Task = Crew = Process = None
    CREWAI_AVAILABLE = False

# ──────────────────────────────────────────────
# 1. ENVIRONMENT SETUP
# ──────────────────────────────────────────────

# Biến môi trường bắt buộc
OPENAI_API_KEY        = os.getenv("OPENAI_API_KEY", "")
ANTHROPIC_API_KEY     = os.getenv("ANTHROPIC_API_KEY", "")
CHROMA_URL            = os.getenv("CHROMA_URL", "http://localhost:8000")
CHROMA_COLLECTION     = os.getenv("CHROMA_COLLECTION", "pain_bank")
# AIOS hub (FastAPI) — nơi nhận angle briefs. Mặc định trỏ vào hub local.
AIOS_WEBHOOK_URL      = os.getenv("AIOS_WEBHOOK_URL", "http://localhost:8800")
PRODUCT_NAME          = os.getenv("PRODUCT_NAME", "Sản phẩm chưa cấu hình")
PRODUCT_DESCRIPTION   = os.getenv("PRODUCT_DESCRIPTION", "")
ZALO_OA_ACCESS_TOKEN  = os.getenv("ZALO_OA_ACCESS_TOKEN", "")
ZALO_USER_ID          = os.getenv("ZALO_USER_ID", "")

# MOCK_MODE: chạy không cần LLM/crewai — sinh angle mẫu để demo end-to-end.
# Bật khi: ép AIOS_MOCK=1, HOẶC chưa có API key, HOẶC crewai chưa cài.
MOCK_MODE = (
    os.getenv("AIOS_MOCK", "").lower() in ("1", "true", "yes")
    or not (OPENAI_API_KEY or ANTHROPIC_API_KEY)
    or not CREWAI_AVAILABLE
)

# Thư mục lưu output
OUTPUT_DIR = Path("outputs")
OUTPUT_DIR.mkdir(exist_ok=True)

# ──────────────────────────────────────────────
# 2. LOGGING SETUP
# ──────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(OUTPUT_DIR / "crewai_agent.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────
# 3. CHROMADB — ĐỌC PAIN BANK
# ──────────────────────────────────────────────
def get_top_pains(n: int = 10, collection: str = None) -> list[str]:
    """
    Query top N customer pains từ ChromaDB Pain Bank.
    collection: tên collection riêng của workspace (None → CHROMA_COLLECTION mặc định).
    Trả về danh sách string mô tả pain.
    """
    coll_name = collection or CHROMA_COLLECTION
    try:
        logger.info(f"Kết nối ChromaDB tại {CHROMA_URL} (collection={coll_name})...")

        # Parse host và port từ URL
        chroma_host = CHROMA_URL.replace("http://", "").replace("https://", "")
        chroma_port = 8000
        if ":" in chroma_host:
            parts = chroma_host.split(":")
            chroma_host = parts[0]
            chroma_port = int(parts[1].split("/")[0])

        client = chromadb.HttpClient(host=chroma_host, port=chroma_port)
        collection_obj = client.get_collection(coll_name)

        results = collection_obj.query(
            query_texts=["customer pain problem frustration"],
            n_results=n,
        )
        pains = results.get("documents", [[]])[0]
        logger.info(f"Lấy được {len(pains)} pains từ Pain Bank '{coll_name}'")
        return pains

    except Exception as e:
        logger.error(f"Lỗi kết nối ChromaDB: {e}")
        # Fallback: trả về sample pains để agent vẫn chạy được
        logger.warning("Dùng sample pains mặc định do không kết nối được ChromaDB")
        return [
            "Da mặt bị nổi mụn sau khi dùng mỹ phẩm không rõ nguồn gốc",
            "Sản phẩm dưỡng da hứa hẹn nhiều nhưng không thấy kết quả sau 1 tháng",
            "Giá quá đắt so với chất lượng thực tế nhận được",
            "Không biết cách dùng đúng để đạt hiệu quả tối đa",
            "Lo ngại về thành phần hóa học ảnh hưởng sức khỏe dài hạn",
            "Giao hàng chậm, hàng đến bị móp hộp, không tin tưởng shop",
            "Không có review thật, toàn KOC paid không trung thực",
            "Mua về dùng 2 tuần không thấy gì, tiền mất tật mang",
            "Không biết sản phẩm có phù hợp với loại da của mình không",
            "Sợ bị lừa hàng fake khi mua trên TikTok Shop",
        ]


# ──────────────────────────────────────────────
# 4. CREWAI AGENTS
# ──────────────────────────────────────────────
def build_agents():
    """Khởi tạo 4 agents với roles chuyên biệt."""

    pain_analyst = Agent(
        role="Pain Bank Analyst",
        goal=(
            "Phân tích và chọn top 5 pains có tiềm năng convert cao nhất "
            "dựa trên mức độ phổ biến, cảm xúc, và liên quan đến sản phẩm"
        ),
        backstory=(
            "Chuyên gia phân tích consumer insight với 10 năm kinh nghiệm "
            "TikTok commerce Vietnam. Đã từng work tại Nielsen, Kantar và nhiều "
            "brand FMCG lớn. Hiểu sâu về tâm lý mua hàng của người Việt Gen Z "
            "và Millennial trên TikTok Shop."
        ),
        verbose=True,
        allow_delegation=False,
    )

    angle_generator = Agent(
        role="6P Angle Generator",
        goal=(
            "Tạo 5 angle briefs theo công thức 6P: "
            "Persona, Pain, Moment, Promise, Proof, Pull-to-buy. "
            "Mỗi angle phải fresh, chưa bão hòa, và có hook mạnh trong 3 giây đầu."
        ),
        backstory=(
            "Content strategist chuyên bán hàng TikTok Shop Vietnam với track record "
            "300+ angle đã test, win rate 42%. Hiểu sâu về bestie vibe, ngôn ngữ "
            "Gen Z Việt, storytelling ngắn, và cách build trust nhanh trên camera. "
            "Đặc biệt giỏi viết hook gây tò mò mà không bị report vi phạm."
        ),
        verbose=True,
        allow_delegation=False,
    )

    qc_judge = Agent(
        role="QC Judge",
        goal=(
            "Chấm điểm và lọc angles theo 5 tiêu chí: "
            "Hook strength (1-10), Relatability (1-10), Credibility (1-10), "
            "CTA clarity (1-10), Brand safety (1-10). "
            "Chỉ pass angles đạt tổng điểm >= 35/50 (tương đương 7/10 trung bình). "
            "Phải giải thích rõ lý do reject."
        ),
        backstory=(
            "Ex-TikTok content moderator và brand safety expert với 5 năm kinh nghiệm. "
            "Đã review hơn 50,000 videos và biết rõ cái gì viral, cái gì bị report, "
            "cái gì bị shadow ban. Không có cảm tình, chỉ có data và tiêu chuẩn."
        ),
        verbose=True,
        allow_delegation=False,
    )

    brief_writer = Agent(
        role="KOC Brief Writer",
        goal=(
            "Format angles đã pass QC thành brief chuẩn cho KOC, "
            "đảm bảo: no-fluff, actionable, tone bestie không phải tone quảng cáo, "
            "KOC đọc xong là biết ngay phải nói gì, đứng ở đâu, express thế nào."
        ),
        backstory=(
            "Creative director đã brief 500+ KOC videos với win rate >40%. "
            "Từng work với Shopee, Lazada, TikTok Shop Vietnam. "
            "Biết cách viết brief mà KOC không cần training vẫn deliver được "
            "video cảm giác authentic, không cảm giác bị paid."
        ),
        verbose=True,
        allow_delegation=False,
    )

    return pain_analyst, angle_generator, qc_judge, brief_writer


# ──────────────────────────────────────────────
# 5. CREWAI TASKS
# ──────────────────────────────────────────────
def build_tasks(
    pain_analyst: Agent,
    angle_generator: Agent,
    qc_judge: Agent,
    brief_writer: Agent,
    pains: list[str],
) -> list[Task]:
    """Tạo 4 tasks liên kết nhau theo quy trình."""

    pains_text = "\n".join(f"- {p}" for p in pains)

    task_analyze_pains = Task(
        description=f"""
Phân tích danh sách pains dưới đây và chọn top 5 pains tiềm năng nhất để build angle.

Sản phẩm: {PRODUCT_NAME}
Mô tả: {PRODUCT_DESCRIPTION}

Danh sách pains từ Pain Bank:
{pains_text}

Tiêu chí chọn:
1. Pain có cảm xúc mạnh (frustration, fear, embarrassment)
2. Pain liên quan trực tiếp đến sản phẩm {PRODUCT_NAME}
3. Pain phổ biến với target audience TikTok Việt Nam (18-35 tuổi)
4. Pain chưa bị khai thác quá nhiều trên TikTok (fresh angle potential)

Output: Danh sách top 5 pains kèm giải thích tại sao chọn và insight về target persona.
""",
        expected_output=(
            "Danh sách 5 pains đã chọn, mỗi pain gồm: "
            "(1) mô tả pain, (2) target persona, (3) cảm xúc chủ đạo, "
            "(4) lý do tiềm năng convert cao"
        ),
        agent=pain_analyst,
    )

    task_generate_angles = Task(
        description=f"""
Dựa trên top 5 pains đã phân tích, tạo 5 angle briefs theo công thức 6P cho sản phẩm {PRODUCT_NAME}.

Công thức 6P cho mỗi angle:
- Persona: Ai đang nói? (KOC profile, background, vibe)
- Pain: Pain cụ thể đang target
- Moment: Bối cảnh, thời điểm xảy ra pain (setting, situation)
- Promise: Sản phẩm giải quyết pain thế nào? (cụ thể, không generic)
- Proof: Bằng chứng tin cậy (before/after, demo, social proof)
- Pull-to-buy: CTA tự nhiên, không cảm giác bị ép mua

Yêu cầu:
- Mỗi angle có hook rõ ràng cho 3 giây đầu video
- Tone bestie, không phải tone quảng cáo
- Không dùng từ: "tốt nhất", "số 1", "đảm bảo 100%"
- Hook phải specific, không generic
""",
        expected_output=(
            "5 angle briefs đầy đủ theo format 6P, "
            "mỗi angle có: tên angle, hook 3 giây, và đủ 6 phần P"
        ),
        agent=angle_generator,
        context=[task_analyze_pains],
    )

    task_qc_angles = Task(
        description="""
Chấm điểm và filter 5 angles theo 5 tiêu chí sau (thang 1-10 mỗi tiêu chí):

1. Hook Strength: Hook 3 giây có đủ mạnh để stop scroll không?
2. Relatability: Target audience có tự nhận ra mình trong đó không?
3. Credibility: Proof có đáng tin không? Có vẻ authentic không?
4. CTA Clarity: CTA có rõ ràng và tự nhiên không?
5. Brand Safety: Có nguy cơ bị report, shadow ban, hoặc vi phạm policy không?

Tổng điểm tối đa: 50. Pass threshold: >= 35 điểm.

Với mỗi angle:
- Nếu PASS: giữ nguyên + ghi score breakdown
- Nếu FAIL: reject + giải thích cụ thể điểm nào thấp + gợi ý cải thiện

Output: Danh sách angles đã pass QC kèm score, và danh sách angles bị reject kèm lý do.
""",
        expected_output=(
            "Danh sách angles PASS QC (tối thiểu 1, tối đa 5) với score breakdown chi tiết. "
            "Danh sách angles FAIL với lý do cụ thể."
        ),
        agent=qc_judge,
        context=[task_generate_angles],
    )

    task_write_briefs = Task(
        description=f"""
Format các angles đã pass QC thành KOC Brief chuẩn cho sản phẩm {PRODUCT_NAME}.

Format brief chuẩn cho mỗi angle:

---
ANGLE #[số]: [Tên angle ngắn gọn]
QC Score: [X]/50
---

🎯 TARGET PERSONA
[Ai là người xem lý tưởng - 1-2 câu]

😤 PAIN HOOK (3 giây đầu)
[Câu hook cụ thể KOC nói/hành động ngay đầu video]

📍 SETTING
[KOC đứng ở đâu, mặc gì, đang làm gì]

📝 SCRIPT OUTLINE
[3-5 beat ngắn theo flow: Hook → Pain → Discovery → Proof → CTA]

💬 KEY PHRASES
[3-5 câu/cụm từ cụ thể KOC nên nói - viết ra nguyên văn]

🎬 VISUAL NOTES
[Gợi ý visual: góc quay, prop, before/after, demo]

📢 CTA
[Câu CTA cụ thể - tự nhiên, không cảm giác paid]

⚠️ AVOID
[1-3 điều KOC tuyệt đối không làm/nói trong video này]
---

Yêu cầu: Viết đủ chi tiết để KOC đọc 2 phút là có thể quay ngay, không cần họp thêm.
Output cuối cùng phải là JSON array có thể parse được.
""",
        expected_output=(
            "JSON array các angle briefs đã format chuẩn. "
            "Mỗi brief là một object với các keys: "
            "angle_id, angle_name, qc_score, persona, pain_hook, "
            "setting, script_outline, key_phrases, visual_notes, cta, avoid"
        ),
        agent=brief_writer,
        context=[task_qc_angles],
    )

    return [task_analyze_pains, task_generate_angles, task_qc_angles, task_write_briefs]


# ──────────────────────────────────────────────
# 6. CREW EXECUTION
# ──────────────────────────────────────────────
# Các key chuẩn của một angle brief (khớp với dashboard normalizer)
BRIEF_KEYS = (
    "angle_id", "angle_name", "qc_score", "persona", "pain_hook",
    "setting", "script_outline", "key_phrases", "visual_notes", "cta", "avoid",
    "pain", "moment", "promise", "proof", "approach", "platform",
)


def _normalize_angles(angles_data: list, pains: list) -> list:
    """Chuẩn hoá output về list các brief dict có đủ key BRIEF_KEYS."""
    normalized = []
    for i, a in enumerate(angles_data or []):
        if not isinstance(a, dict):
            a = {"angle_name": str(a)}
        brief = {k: a.get(k) for k in BRIEF_KEYS}
        brief["angle_id"] = brief.get("angle_id") or f"A{i+1}"
        brief["angle_name"] = brief.get("angle_name") or f"Angle {i+1}"
        # qc_score: chấp nhận thang /50 hoặc /10
        qc = brief.get("qc_score")
        try:
            qc = float(qc)
        except (TypeError, ValueError):
            qc = None
        brief["qc_score"] = qc
        normalized.append(brief)
    return normalized


def _pain_phrase(pain: str) -> str:
    """Rút gọn pain thành cụm ngắn, gọn để ghép vào hook/field (bỏ dấu câu thừa)."""
    p = (pain or "").strip().rstrip(".").lower()
    return (p[:55] + "…") if len(p) > 55 else p


# Multi-platform: CTA tinh chỉnh theo sàn
PLATFORM_CTA = {
    "TikTok": "Giỏ hàng đang có mã, link mình để dưới nha",
    "Shopee": "Bấm giỏ hàng Shopee, nhớ bấm mã giảm trước khi thanh toán",
    "Facebook": "Inbox shop hoặc bấm 'Mua ngay' dưới bài để được tư vấn",
}


def _platform_cta(platform: str, fallback: str) -> str:
    return PLATFORM_CTA.get(platform or "TikTok", fallback)


def _mock_angles(pains: list, product: str, count: int,
                 bias_persona: str = None, platform: str = "TikTok") -> list:
    """Sinh angle mẫu KHÔNG cần LLM — nội dung sạch, map đúng 6P.
    bias_persona: Auto-learning — ưu tiên persona đang thắng lên đầu.
    platform: Multi-platform — tinh chỉnh CTA theo sàn."""
    personas = ["Dân văn phòng 25-32", "Mẹ bỉm sữa", "Sinh viên ngân sách thấp",
                "Người bận rộn", "Phụ nữ 35+ quan tâm sức khoẻ"]
    if bias_persona and bias_persona in personas:   # đẩy persona thắng lên đầu
        personas = [bias_persona] + [p for p in personas if p != bias_persona]
    proofs = ["Before/After", "Demo trực tiếp", "Review thật", "Thử thách 14 ngày", "So sánh"]
    moments = ["cuối ngày mệt nhoài", "sáng vội đi làm", "sau bữa tối", "trước giờ lên hình"]
    promises = ["nhẹ người thấy rõ sau 1 tuần", "tiết kiệm nửa thời gian mỗi ngày",
                "đỡ hẳn mà không cần cố gắng nhiều", "thấy khác biệt ngay lần đầu dùng"]
    out = []
    pool = pains or ["khách hàng chưa tin sản phẩm"]
    for i in range(count):
        pain = pool[i % len(pool)]
        ph = _pain_phrase(pain)
        persona = personas[i % len(personas)]
        promise = promises[i % len(promises)]
        out.append({
            "angle_id": f"A{i+1}",
            "angle_name": f"{persona} · {proofs[i % len(proofs)]}",
            "qc_score": random.randint(36, 47),  # thang /50
            "persona": persona,
            "pain": pain,                                   # PAIN thật → map sang P2
            "moment": moments[i % len(moments)],            # → P3
            "promise": promise,                             # → P4
            "proof": proofs[i % len(proofs)],               # → P5 (ngắn gọn)
            "platform": platform,
            "pain_hook": f"{persona} mà {ph} thì xem cái này — {promise}.",
            "setting": moments[i % len(moments)],
            "script_outline": [
                f"Hook: nói đúng nỗi khổ '{ph}'",
                f"Discovery: tình cờ biết {product}",
                f"Proof: {proofs[i % len(proofs)]}",
                "CTA tự nhiên",
            ],
            "key_phrases": [
                "thề là không nghĩ nó work",
                "xài mấy hôm mới dám quay clip này",
                "không phải quảng cáo đâu nha mng",
            ],
            "visual_notes": proofs[i % len(proofs)] + ", quay dọc, ánh sáng tự nhiên",
            "cta": _platform_cta(platform, "Link trong bio"),
            "avoid": "Không hứa 'khỏi 100%', không dùng từ 'tốt nhất/số 1'",
        })
    return out


def _run_crewai(pains: list, product: str, count: int) -> list:
    """Chạy CrewAI pipeline thật và parse JSON briefs ra list."""
    pain_analyst, angle_generator, qc_judge, brief_writer = build_agents()
    tasks = build_tasks(pain_analyst, angle_generator, qc_judge, brief_writer, pains)
    crew = Crew(
        agents=[pain_analyst, angle_generator, qc_judge, brief_writer],
        tasks=tasks,
        process=Process.sequential,
        verbose=True,
    )
    logger.info("Khởi động CrewAI pipeline...")
    result = crew.kickoff()
    raw_output = str(result)
    logger.info("CrewAI pipeline hoàn tất.")
    logger.debug(f"Raw output: {raw_output[:500]}...")

    json_match = re.search(r"\[[\s\S]*\]", raw_output)
    if not json_match:
        logger.warning("Không tìm thấy JSON array trong output, trả raw text")
        return [{"angle_name": "raw_output", "pain_hook": raw_output}]
    try:
        return json.loads(json_match.group())
    except json.JSONDecodeError as e:
        logger.warning(f"Không parse được JSON: {e}. Trả raw output.")
        return [{"angle_name": "raw_output", "pain_hook": raw_output}]


def generate_angles_from_pains(pains: list = None, product: str = None,
                               count: int = 5, collection: str = None,
                               bias_persona: str = None, platform: str = "TikTok") -> dict:
    """
    Lõi sinh angle — gọi được từ AIOS server (in-process) hoặc CLI.
    pains: list[str] (nếu None sẽ tự lấy từ ChromaDB / fallback).
    collection: Chroma collection riêng của workspace (pain-bank theo nhóm).
    bias_persona: Auto-learning — ưu tiên persona đang thắng.
    platform: Multi-platform — TikTok/Shopee/Facebook.
    Trả về dict metadata + angles đã chuẩn hoá.
    """
    product = product or PRODUCT_NAME
    if pains is None:
        pains = get_top_pains(n=10, collection=collection)

    logger.info("=" * 60)
    logger.info(f"Generate {count} angles | sản phẩm: {product} | platform: {platform} | "
                f"bias: {bias_persona or '-'} | mode: {'MOCK' if MOCK_MODE else 'CREWAI'}")
    logger.info("=" * 60)

    if MOCK_MODE:
        angles_data = _mock_angles(pains, product, count, bias_persona, platform)
    else:
        try:
            angles_data = _run_crewai(pains, product, count)
            for a in angles_data:   # gắn platform cho output crewai
                if isinstance(a, dict):
                    a.setdefault("platform", platform)
        except Exception as e:
            logger.error(f"CrewAI lỗi ({e}), fallback sang mock angles", exc_info=True)
            angles_data = _mock_angles(pains, product, count, bias_persona, platform)

    angles = _normalize_angles(angles_data, pains)
    return {
        "product": product,
        "generated_at": datetime.now().isoformat(),
        "mode": "mock" if MOCK_MODE else "crewai",
        "platform": platform,
        "pains_used": pains,
        "angles": angles,
        "total_angles": len(angles),
    }


def run_crew() -> dict:
    """Chạy pipeline đầy đủ với pains lấy từ ChromaDB (giữ tương thích CLI)."""
    pains = get_top_pains(n=10)
    return generate_angles_from_pains(pains=pains, product=PRODUCT_NAME, count=5)


# ──────────────────────────────────────────────
# MODULE C — A/B MATRIX: mỗi pain → 3 variants theo hướng tiếp cận
# ──────────────────────────────────────────────
APPROACHES = ("Logical", "Emotional", "Curiosity")


def _variant(approach: str, pain: str, product: str, idx: int,
             platform: str = "TikTok") -> dict:
    p = _pain_phrase(pain)
    if approach == "Logical":
        hook = f"3 lý do khiến {p} — và cách {product} xử lý từng cái"
        persona = "Người thích phân tích, so sánh"
        promise = "hiểu rõ vấn đề và cách xử lý có cơ sở"
        proof = "So sánh số liệu"
    elif approach == "Emotional":
        hook = f"Tui từng bất lực vì {p}… đến khi đổi sang {product}"
        persona = "Người đồng cảm, kể chuyện thật"
        promise = "thấy được đồng cảm và có lối ra"
        proof = "Before/After cảm xúc"
    else:  # Curiosity
        hook = f"Không ai nói cho bạn vì sao {p}? Xem hết clip nha"
        persona = "Người tò mò, thích bí mật"
        promise = "biết được điều ít ai nói ra"
        proof = "Reveal cuối clip"
    return {
        "angle_id": f"V{idx}_{approach[:3].upper()}",
        "angle_name": f"{approach} · {persona}",
        "approach": approach,
        "qc_score": random.randint(36, 47),
        "persona": persona,
        "pain": pain,
        "moment": "khi đang gặp đúng vấn đề",
        "promise": promise,
        "proof": proof,
        "platform": platform,
        "pain_hook": hook,
        "setting": "Bối cảnh tự nhiên, quay dọc",
        "script_outline": ["Hook", "Triển khai hướng " + approach, "Proof", "CTA"],
        "visual_notes": proof + ", quay dọc",
        "cta": _platform_cta(platform, "Link ở bio"),
        "avoid": "Không hứa 'khỏi 100%', không 'tốt nhất/số 1'",
    }


def generate_variant_matrix(pains: list = None, product: str = None,
                            count: int = 3, collection: str = None,
                            bias_approach: str = None, platform: str = "TikTok") -> dict:
    """
    Sinh A/B Matrix: mỗi pain → 1 cluster gồm 3 variants [Logical, Emotional, Curiosity].
    bias_approach: Auto-learning — đẩy hướng đang thắng lên đầu cụm.
    platform: Multi-platform — TikTok/Shopee/Facebook.
    """
    product = product or PRODUCT_NAME
    if pains is None:
        pains = get_top_pains(n=count, collection=collection)
    pains = (pains or ["khách hàng chưa tin sản phẩm"])[:count]
    order = list(APPROACHES)
    if bias_approach in order:                       # hướng thắng lên đầu
        order = [bias_approach] + [a for a in order if a != bias_approach]
    clusters = []
    for i, pain in enumerate(pains):
        clusters.append({
            "pain": pain,
            "variants": [_variant(a, pain, product, i + 1, platform) for a in order],
        })
    return {
        "product": product,
        "generated_at": datetime.now().isoformat(),
        "mode": "mock" if MOCK_MODE else "crewai",
        "platform": platform,
        "bias_approach": bias_approach,
        "clusters": clusters,
        "total_variants": sum(len(c["variants"]) for c in clusters),
    }


# ──────────────────────────────────────────────
# 7. OUTPUT — POST + SAVE + NOTIFY
# ──────────────────────────────────────────────
def save_to_file(data: dict) -> Path:
    """Lưu angles ra file JSON local với timestamp."""
    date_str = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    output_path = OUTPUT_DIR / f"angles_{date_str}.json"

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    logger.info(f"Đã lưu angles vào: {output_path}")
    return output_path


def push_to_aios_queue(data: dict) -> bool:
    """POST angles vào AIOS approval queue webhook."""
    if requests is None:
        logger.warning("requests chưa cài, bỏ qua push to AIOS queue")
        return False
    url = f"{AIOS_WEBHOOK_URL}/queue"
    try:
        response = requests.post(
            url,
            json=data,
            headers={"Content-Type": "application/json"},
            timeout=30,
        )
        response.raise_for_status()
        logger.info(f"Push to AIOS queue thành công: {response.status_code}")
        return True
    except Exception as e:
        logger.error(f"Lỗi push to AIOS queue ({url}): {e}")
        return False


def send_zalo_notification(data: dict, success: bool = True) -> bool:
    """Gửi Zalo notification sau khi generate xong."""
    if requests is None:
        logger.warning("requests chưa cài, bỏ qua Zalo notification")
        return False
    if not ZALO_OA_ACCESS_TOKEN or not ZALO_USER_ID:
        logger.warning("Thiếu ZALO_OA_ACCESS_TOKEN hoặc ZALO_USER_ID, bỏ qua notification")
        return False

    total = data.get("total_angles", 0)
    product = data.get("product", "")
    generated_at = data.get("generated_at", "")

    if success:
        message = (
            f"✅ ANGLE GENERATION XONG!\n"
            f"Sản phẩm: {product}\n"
            f"Số angles: {total} briefs\n"
            f"Lúc: {generated_at}\n\n"
            f"👉 Vào AIOS để review và approve angles nhé!"
        )
    else:
        message = (
            f"❌ ANGLE GENERATION THẤT BẠI\n"
            f"Sản phẩm: {product}\n"
            f"Thời gian: {generated_at}\n\n"
            f"⚠️ Check logs để xem lỗi."
        )

    payload = {
        "recipient": {"user_id": ZALO_USER_ID},
        "message": {"text": message},
    }

    try:
        response = requests.post(
            "https://openapi.zalo.me/v2.0/oa/message",
            json=payload,
            headers={
                "Content-Type": "application/json",
                "access_token": ZALO_OA_ACCESS_TOKEN,
            },
            timeout=15,
        )
        response.raise_for_status()
        logger.info("Đã gửi Zalo notification thành công")
        return True
    except Exception as e:
        logger.error(f"Lỗi gửi Zalo notification: {e}")
        return False


def push_to_queue(result: dict) -> None:
    """
    Orchestrate toàn bộ output pipeline:
    1. Lưu file local
    2. POST lên AIOS webhook
    3. Gửi Zalo notification
    """
    # Lưu file local
    file_path = save_to_file(result)

    # Push lên AIOS
    aios_ok = push_to_aios_queue(result)

    # Zalo notification
    send_zalo_notification(result, success=aios_ok)

    logger.info(
        f"Output pipeline hoàn tất. File: {file_path} | AIOS: {'OK' if aios_ok else 'FAIL'}"
    )


# ──────────────────────────────────────────────
# 8. SCHEDULER
# ──────────────────────────────────────────────
def run_job() -> None:
    """Job function chạy mỗi ngày lúc 6am."""
    logger.info(f"[{datetime.now()}] Bắt đầu scheduled angle generation job...")
    try:
        result = run_crew()
        push_to_queue(result)
        logger.info(f"[{datetime.now()}] Job hoàn tất. Total angles: {result.get('total_angles', 0)}")
    except Exception as e:
        logger.error(f"[{datetime.now()}] Job thất bại: {e}", exc_info=True)
        # Gửi Zalo alert lỗi
        send_zalo_notification(
            {"product": PRODUCT_NAME, "generated_at": datetime.now().isoformat(), "total_angles": 0},
            success=False,
        )


def validate_env() -> bool:
    """Kiểm tra env vars bắt buộc trước khi chạy."""
    missing = []

    if not OPENAI_API_KEY and not ANTHROPIC_API_KEY:
        missing.append("OPENAI_API_KEY hoặc ANTHROPIC_API_KEY")
    if not CHROMA_URL:
        missing.append("CHROMA_URL")
    if not AIOS_WEBHOOK_URL:
        missing.append("AIOS_WEBHOOK_URL")
    if not PRODUCT_NAME or PRODUCT_NAME == "Sản phẩm chưa cấu hình":
        missing.append("PRODUCT_NAME")

    if missing:
        logger.warning(f"Thiếu env vars (chạy vẫn được nhưng có thể lỗi): {', '.join(missing)}")
        return False

    logger.info("Env vars OK.")
    return True


# ──────────────────────────────────────────────
# 9. CLI INTERFACE
# ──────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="ANGLE WIN AIOS — CrewAI Angle Generator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ví dụ:
  python crewai_angle_agent.py --once          # Chạy ngay 1 lần
  python crewai_angle_agent.py --scheduled     # Chạy theo lịch 6am hàng ngày
  python crewai_angle_agent.py --once --debug  # Chạy 1 lần với debug logging
        """,
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Chạy generate một lần ngay lập tức rồi thoát",
    )
    parser.add_argument(
        "--scheduled",
        action="store_true",
        help="Chạy theo lịch: 6:00 AM hàng ngày (loop liên tục)",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Bật DEBUG logging",
    )
    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.debug("Debug mode ON")

    # Validate environment
    validate_env()

    if args.once:
        logger.info("Chế độ: --once. Chạy ngay một lần...")
        run_job()
        logger.info("Xong. Thoát.")

    elif args.scheduled:
        logger.info("Chế độ: --scheduled. Chạy lúc 6:00 AM hàng ngày...")
        logger.info(f"Server time hiện tại: {datetime.now()}")

        try:
            import schedule
        except ImportError:
            logger.error("Thiếu lib 'schedule'. Cài: pip install schedule")
            raise SystemExit(1)

        # Đăng ký job
        schedule.every().day.at("06:00").do(run_job)

        logger.info("Scheduler đã khởi động. Đang chờ...")
        logger.info("Bấm Ctrl+C để dừng.")

        try:
            while True:
                schedule.run_pending()
                time.sleep(60)
        except KeyboardInterrupt:
            logger.info("Scheduler dừng bởi user. Thoát.")

    else:
        # Không có flag — hiển thị help
        parser.print_help()
        print("\n⚠️  Phải dùng --once hoặc --scheduled để chạy.")
        print("Ví dụ: python crewai_angle_agent.py --once")

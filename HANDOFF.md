# 🔄 HANDOFF — Angle Win AIOS

> Copy toàn bộ file này vào đầu phiên Claude mới để brief lại nhanh.
> Cập nhật lần cuối: **2026-06-12** — xong: multi-tenant, P1 bảo mật, P2 AI-context/Chroma-per-ws, P3 offline outbox,
> P4 fair-share, Export CSV, date-range, **Autonomous A/B/C/D** (Master Log + SSE + A/B Matrix + Cluster Approval),
> **Auto-learning + Multi-platform + RBAC**, fix chất lượng mock 6P, **PDF hướng dẫn**, và **bản v3 offline-first** (fix + wire role/platform).

---

## 0. HAI BẢN SONG SONG (đọc trước)
- **v2 + hub** (`angle-win-aios-v2.html` + `aios_server.py`): mạnh nhất về backend (SQLite multi-tenant, RBAC server-side,
  Auto-learning, A/B Matrix SSE, fair-share, master event log). Chạy: `bash run.sh` → http://localhost:8800/ (cần Python). **Chỉ localhost.**
- **v3** (`angle-win-aios-v3.html`): single-file **offline-first** (localStorage), có **tab Research** + tích hợp **n8n LLM gateway**,
  deploy **GitHub Pages** → có link public. Đã port role/platform/A-B của v2 + wire chạy offline.
  Deploy repo: `nhuhoa2050-glitch.github.io/angle-win-aios/angle-win-aios-v3.html`. README đã đổi sang mô tả v3.
  Phục vụ test local phiên này: `python3 -m http.server 9000` → http://localhost:9000/angle-win-aios-v3.html

---

## 1. Bối cảnh (Context)

**Dự án:** Hệ thống quản lý & test "angle" quảng cáo TikTok Shop cho **nhiều nhóm**.
Yêu cầu cốt lõi: **dữ liệu mỗi nhóm độc lập, không lỗi chồng chéo.**

**Kiến trúc đã ghép end-to-end (qua hub trung tâm) — multi-tenant đầy đủ:**
- `aios_server.py` — **hub FastAPI + SQLite** (`aios.db`). Phục vụ dashboard `/`; webhook
  `/queue /win /flop /competitor-data` (+ `require_token`); `/generate-angles`; API
  `/api/state|approve|reject|generate|health|verdicts|workspace(s)|jobs`. Bảng: queue, angles,
  competitor, events, **workspaces**, **jobs** — tất cả có `workspace_id`. Có **worker fair-share** (daemon thread).
- `crewai_angle_agent.py` — pipeline 4-agent 6P, import được (`generate_angles_from_pains(..., collection)`)
  + CLI + **MOCK_MODE** (tự bật khi thiếu API key / chưa cài crewai). `get_top_pains(n, collection)` theo nhóm.
- `angle-win-aios-v2.html` — dashboard + lớp `AIOS`/`aiosSync`: gửi `X-Workspace-ID`/`X-AIOS-Token`,
  đăng ký ws (`aiosRegisterWs`), guard `requireWs`, **outbox offline** (localStorage replay). Tab **Realtime WIN/FLOP**:
  chart combo gộp Giờ/Ngày/Tuần/Tháng + **date-range** + **Export CSV**.
- `n8n-angle-win-workflow.json` — tên **Novix**, 3 cron; 5 node gọi hub gửi `X-Workspace-ID` + `X-AIOS-Token`.

**Trạng thái chạy:** demo MOCK chạy ngay — `bash run.sh` → http://localhost:8800/. Mọi endpoint + JS đã smoke-test OK.
Bật bảo mật/đa nhóm thật: `AIOS_TOKEN=... AIOS_STRICT_WS=1`; bật LLM thật: `requirements-agent.txt` + API key, bỏ trống `AIOS_MOCK`.

**Đã xong gần nhất:** P4 fair-share job queue + Export CSV + date-range chart. **Roadmap (P1→P4 + tùy chọn) đã hoàn thành toàn bộ.**

---

## 2. Nhiệm vụ (Task) — việc cần làm tiếp

- [x] **Multi-tenant thật ở backend** ✅ (2026-06-11): 4 bảng có cột `workspace_id`
      (queue/angles PK ghép `(workspace_id,id)`); DI `get_workspace` đọc header `X-Workspace-ID`;
      mọi SQL gắn `workspace_id=?`. Dashboard gửi header từ `currentWS.id` (helper `aiosHeaders`).
      Đã test cô lập 2 nhóm + guard cross-tenant (404). Migration ALTER COLUMN cho DB cũ.
- [x] **n8n gửi `X-Workspace-ID`** ✅ (2026-06-11): 5 node HTTP gọi hub (win/flop/queue/competitor/generate-angles)
      đã thêm header `X-Workspace-ID = {{ $env.WS_ID }}`. Set biến `WS_ID` trong n8n cho từng nhóm
      (mỗi nhóm 1 bản workflow với WS_ID riêng).
- [x] **n8n gắn tên Novix** ✅ (2026-06-11): workflow name "Novix — Angle Win AIOS", 3 sub-flow notes
      "Novix · Flow 1/2/3", tag "Novix".
- [x] **P1 — Bảo mật multi-tenant** ✅ (2026-06-11):
      • Bảng `workspaces` (id,name,product_name,product_desc,chroma_collection) + seed 'default'.
      • `require_token` (header `X-AIOS-Token` = env `AIOS_TOKEN`) trên 5 webhook; n8n đã gửi token.
      • `get_workspace` strict allowlist (env `AIOS_STRICT_WS=1` → ws lạ 403); dev mode auto-register.
      • Endpoint `POST /api/workspace` (đăng ký, dùng get_workspace_raw để bootstrap) + `GET /api/workspaces`.
      • Dashboard: `aiosRegisterWs()` đăng ký ws+product khi sync; `requireWs()` guard generate/approve/reject.
      • Test: 401 no/bad token, 200 đúng token, 403 ws lạ (strict), đăng ký→200, guard OK.
- [x] **P2 — AI context theo nhóm** ✅ (2026-06-11): `/generate-angles` & `/api/generate` nạp
      `product_name` từ bảng `workspaces` theo ws (thay vì PRODUCT_NAME toàn cục). Test: team_skin → "Serum Trắng Da X".
- [x] **P2-deep — Chroma collection theo nhóm** ✅ (2026-06-11): `get_top_pains(n, collection)` +
      `generate_angles_from_pains(..., collection)`; server truyền `wsrow.chroma_collection` ở cả
      `/generate-angles` và `/api/generate`. Mock chưa có Chroma server nên fallback sample (log xác nhận collection).
- [x] **P3 — Fallback offline (outbox)** ✅ (2026-06-11): dashboard `aiosPush` offline/lỗi → xếp vào
      `localStorage['aios_outbox']` kèm `wsId`; `aiosOutboxFlush()` replay khi online (gọi trong `aiosSync`);
      badge hiện "⏳N chờ sync". Dùng localStorage thay IndexedDB cho gọn (đủ cho mutation nhỏ).
- [x] **Export CSV feed verdict** ✅ (2026-06-11): nút trên tab Realtime → `exportVerdictsCSV()` tải CSV (BOM UTF-8) từ feed hiện tại.
- [x] **Date-range cho chart Realtime** ✅ (2026-06-11): ô từ–đến ngày → `/api/verdicts?from_ts&to_ts` lọc cả feed lẫn timeline.
- [x] **P4 — Fair-share job queue** ✅ (2026-06-11): bảng `jobs`; `POST/GET /api/jobs`, `GET /api/jobs/{id}`;
      worker thread daemon xử lý XOAY VÒNG theo ws (`_pick_fair` thuần, test: A1 B4 A2 B5 A3); generate đồng bộ
      cũ vẫn giữ nguyên. Khôi phục thứ tự fair-share sau restart từ jobs done.
- [x] **UI/UX nâng cao** ✅ (2026-06-12):
      • **Workspace color theme** — `applyWsTheme()` nhuộm viền card/input + sidebar/ws-bar theo `currentWS.dot`
        (chống thao tác nhầm nhóm). Gọi trong `renderWorkspaceBar`.
      • **Khóa Realtime khi !currentWS** — `renderRealtime` ẩn stats/chart/feed (`#rt-stats|rt-chartcard|rt-feedcard`),
        hiện `#rt-lock` "Vui lòng chọn Team…", dừng polling.
      • **Clone chéo Team** — Admin View thêm bảng WIN angle + nút Clone; `cloneAngleToTeam()` ghi localStorage nhóm đích
        + gọi `POST /api/clone-angle` (header X-Workspace-ID=đích → queue pending). Test: clone sang beta OK.
- [x] **Autonomous A/B/C/D** ✅ (2026-06-12):
      • **A — Master Event Logging**: BE `master_log()` (events + `_forward_to_n8n` fire-and-forget thread → `N8N_MASTER_WEBHOOK`);
        `POST /api/log-event`; FE `aiosLogEvent()` non-block wire vào approve/reject/test/win/flop/generate/clone.
        Lưu ý: master_log đóng gói payload lồng → `parse()` trong /api/verdicts đã đọc cả tầng `payload`.
      • **B — SSE**: `POST /api/generate-stream` (text/event-stream) analyze→hook→variants→done; FE đọc reader cập nhật nút `#btn-generate`.
      • **C — A/B Matrix**: `crew.generate_variant_matrix` → mỗi pain 1 cluster 3 variant [Logical, Emotional, Curiosity].
      • **D — Cluster Approval**: queue thêm `cluster_id`/`approach`; FE render cụm; `approveVariant`+`api_approve` chọn 1 → auto-reject 2 + log.
      Env mới: `N8N_MASTER_WEBHOOK`. Test: SSE 6 variants/2 cluster, approve→auto-reject 2, N8N stub nhận event kèm ws.
- [x] **Auto-learning + Multi-platform + RBAC** ✅ (2026-06-12):
      • **Auto-learning**: `learning_summary(ws)` tính win-rate theo approach/persona từ `angles` (đã lưu `approach`/`platform`);
        `GET /api/insights`; generate dùng `bias_persona`/`bias_approach` (đẩy hướng thắng lên đầu). FE: panel "AI Insights" trên tab Approval.
        Test: 4 mẫu (2 Emotional win/2 Logical flop) → bias=Emotional → generate sau Emotional lên đầu cụm.
      • **Multi-platform**: cột `platform` (queue/angles/workspaces); `crew.PLATFORM_CTA` (TikTok/Shopee/Facebook);
        generate nhận platform; FE selector + badge platform trên thẻ queue. Test: Shopee → CTA "Bấm giỏ hàng Shopee…".
      • **RBAC**: header `X-User-Role` (viewer<editor<admin); `require_editor` (generate/approve/reject/jobs/stream),
        `require_admin` (clone). FE: switcher vai trò, `requireEditor/requireAdmin` guard, CSS `role-viewer` ẩn nút + banner Chỉ xem.
        Test: viewer generate→403, editor→200, viewer xem state→200.
      Bỏ qua đợt này: Postgres+Redis (hạ tầng, không test được local).
- [x] **Fix chất lượng mock 6P** ✅ (2026-06-12): `_mock_angles`/`_variant` map đúng field (P2=pain thật, P4=promise,
      P5=proof gọn), bỏ "Mock Angle #1"; thêm `pain/moment/promise/proof/platform` vào `BRIEF_KEYS`; bỏ `[X ngày]` trong HOOK_TEMPLATES.
- [x] **PDF hướng dẫn** ✅ (2026-06-12): `Huong-dan-Angle-Win-AIOS.pdf` (6 trang, xuất bằng Chrome headless từ `huong-dan-angle-win-aios.html`).
- [x] **v3 — fix lỗi hiển thị + audit** ✅ (2026-06-12):
      • `</script>` đặt sai dòng 3376 → dời về cuối (JS không còn render thành text/mất dashboard).
      • `researchRun()` đọc `#rs-shopee-urls` (đã bỏ) → dùng `urlMgrGetAllUrls()`.
      • Hoàn tất patch `_origRenderResearch` (nối `urlMgrRender`). Xoá `researchShopee` cũ (dead code trùng).
      • **Wire role/platform offline**: thêm `PLATFORM_CTA` + `aiosToast`; `generateQueue` offline dùng CTA + badge theo
        `currentPlatform`; CSS `body.role-viewer #main .btn-*` ẩn nút hành động mọi tab; toast phản hồi khi đổi role/platform.
- [ ] **Việc tiếp theo gợi ý:**
  - [ ] Deploy v3 đã vá lên GitHub Pages (git add/commit/push) — XEM mục "Lệnh deploy" cuối file.
  - [ ] (Tùy chọn) Gộp tính năng backend mạnh của v2 (Auto-learning thật, A/B SSE) vào v3 offline để bản public đủ.
  - [ ] (Tùy chọn) Cập nhật PDF: chuyển Auto-learning/Multi-platform/RBAC từ "Tương lai" → "Đã xong".
  - [ ] (Tùy chọn) Rà các tab khác của v3 (B1–B4, Kaizen, Nghiệm Thu) tìm hàm/ID hụt.
  - [ ] (P4 hạ tầng — đã hoãn) Postgres + Redis khi scale.
- [ ] (Tùy chọn) Tắt MOCK, nối CrewAI thật để test pipeline LLM.

---

## Lệnh deploy v3 (GitHub Pages)
```bash
cd /Volumes/hoamau/Mkt-rubylinh
git add angle-win-aios-v3.html
git commit -m "fix+polish v3: script close, research URL-manager, role/platform offline"
git push
# đợi ~1 phút → hard refresh (Cmd/Ctrl+Shift+R):
# https://nhuhoa2050-glitch.github.io/angle-win-aios/angle-win-aios-v3.html
```

---

## 3. Định dạng đầu ra (Format)

- Cung cấp **code hoàn chỉnh**, không giải thích dài dòng.
- Thêm **comment vào những chỗ quan trọng** (tiếng Việt).
- Sửa file tại chỗ, báo ngắn gọn file nào đổi + đã test gì.

---

## Phụ lục — chạy nhanh

```bash
cd /Volumes/hoamau/Mkt-rubylinh
bash run.sh                      # hub + dashboard (mock), http://localhost:8800/
# bật LLM thật:
pip install -r requirements-agent.txt && cp .env.example .env   # điền API key, để trống AIOS_MOCK
```

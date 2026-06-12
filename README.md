# ⚡ Angle Win AIOS v3.0

Hệ thống tự động hoá tìm angle win cho TikTok content — dành cho đội ngũ marketing multi-team.

## Deploy

**GitHub Pages** — sau khi push, app sẽ live tại:
```
https://<your-username>.github.io/<repo-name>/angle-win-aios-v3.html
```

## Setup GitHub Pages

1. Push repo lên GitHub
2. Vào **Settings → Pages**
3. Source: **Deploy from a branch** → `main` / `/ (root)`
4. Hoặc dùng GitHub Actions (`.github/workflows/deploy.yml` đã có sẵn)

## Cấu trúc file

```
angle-win-aios-v3.html   ← App chính (single-file, offline-first)
index.html               ← Redirect về v3
.github/workflows/
  deploy.yml             ← Auto deploy GitHub Pages
```

## Data

Toàn bộ data lưu trong `localStorage` của browser, namespace theo workspace:
- `aios_ws_{id}` — workspace data (pains, angles, kaizen…)
- `aios_events_ws_{id}` — event log (append-only)
- `aios_workspaces` — danh sách workspace

## n8n Integration

- LLM Gateway: `https://srv-lhzd2.auto.123host.asia/webhook/ceenor-llm?key=CeenorAIOS2026`
- Pain Bank Collector: `/webhook/aios-chatwoot-pain`
- Analytics Chat: `/webhook/aios-analytics-chat`
- Research Scrape: `/webhook/aios-research-scrape`

## Thiết lập nhanh

```bash
git init
git add .
git commit -m "feat: AIOS v3 initial deploy"
git remote add origin https://github.com/<username>/<repo>.git
git push -u origin main
```

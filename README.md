# stock-quant-dashboard

Python 自動化更新量化資料，並把結果發布到 GitHub Pages。

## 架構

- `Python pipeline`：抓資料、算指標、寫入 SQLite
- `SQLite`：本地 SQL 資料庫（`data/quant.db`）
- `docs/`：GitHub Pages 靜態前端
- `GitHub Actions`：定時執行 pipeline 並部署 Pages
- 市場資料來源：Yahoo Finance（`yfinance`）

## 本地啟動

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
python main.py --once
```

執行後會產生：

- SQLite DB：`data/quant.db`
- 前端資料：`docs/data/latest.json`

## 部署到 GitHub Pages

1. push 到 GitHub
2. 到 repo 設定啟用 Pages（Build and deployment 選 GitHub Actions）
3. workflow：`.github/workflows/deploy-dashboard.yml`
   - 每個平日 UTC 10:10 自動跑一次
   - 也可手動 `Run workflow`

## 主要檔案

- 入口：`main.py`
- 編排：`quant_dashboard/pipeline.py`
- DB：`quant_dashboard/db.py`
- 前端：`docs/index.html`
- 定時部署：`.github/workflows/deploy-dashboard.yml`

## 之後可做

1. 在 `quant_dashboard/jobs/run_quant.py` 加入更多因子與策略
2. 在 `docs/index.html` 加上圖表與篩選
3. 若要多人長期保存，改為 PostgreSQL（Supabase / Neon）

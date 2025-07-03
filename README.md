# 🐾 Peiplay Discord Bot

匿名語音配對 Discord Bot，支援延遲啟動、自動配對移動、匿名評價、管理員統計查詢等功能，並搭配 PostgreSQL 儲存紀錄與 Flask API 整合網站使用。

---

## 🚀 功能特色

- ✅ 延遲啟動語音頻道（指定時間開啟）
- ✅ 顯示動物頻道名稱（如 🐱 貓咪頻道）
- ✅ 自動移動配對雙方到語音頻道
- ✅ 匿名文字區（5 分鐘後自動刪除）
- ✅ 評價 Modal 表單（匿名 1～5 星＋留言）
- ✅ 管理區收到配對與評價資訊
- ✅ /mystats 查詢自己配對統計
- ✅ /stats @user 管理員可查詢他人統計
- ✅ /report 舉報指令
- ✅ 支援延長頻道時長功能
- ✅ Discord ID 網站 API 接入（Flask）

---

## 🛠 安裝與使用

### 1. Clone 專案

```bash
git clone https://github.com/louis03000/peiplay-discord-bot.git
cd peiplay-discord-bot
# 📈 Stock News Bot - Google Sheets & ntfy

An autonomous bot that scans financial news sites (local and global) based on custom keywords, updating a Google Sheet in real-time with mobile notifications.

## 🚀 Key Features
- **Multi-language Scanning:** Automatic search in Hebrew and English (including auto-translation of keywords).
- **Smart Sorting:** Articles are filtered by priority (User-defined sites > International News > Local News) with a limit of 10 relevant articles per keyword.
- **Google Sheets Interface:** Manage site lists and keywords directly from the spreadsheet.
- **ntfy Notifications:** Receive mobile alerts upon completion, detailing which stocks/keywords were found.
- **Visual Feedback:** Header colors change during execution (Light Red while running, Light Green when finished).
- **Cloud Optimized:** Designed for Render.com with external Cron-job support.

## 📋 Spreadsheet Setup
The bot expects a Google Sheet named `news_for_invest` with the following tabs:
1. **אתרים לחיפוש** (Sites to Search): Column A - Website URLs (e.g., ynet, Globes, etc.).
2. **מילות מפתח** (Keywords): Column A - Hebrew, Column B - English (The bot fills missing translations).
3. **תוצאות החיפוש**: The results sheet (Date, Keyword, URL, Site, Title).

## ⚙️ Installation & Local Setup

1. Clone the repository:
   ```bash
   git clone [https://github.com/your-username/stock-news-bot.git](https://github.com/your-username/stock-news-bot.git)
   cd stock-news-bot
import os
import json
import requests
import feedparser
import base64
import pytz
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
from urllib.parse import urljoin, urlparse, quote
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from deep_translator import GoogleTranslator

# =========================
# הגדרות
# =========================
load_dotenv()

NTFY_TOPIC = os.environ.get("NTFY_TOPIC_env")
SHEET_NAME = os.environ.get("SHEET_NAME_env")
SHEET_LINK = os.environ.get("SHEET_LINK_env")

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

IL_TIMEZONE = pytz.timezone("Asia/Jerusalem")

POS_WORDS = [
    "זינוק","עלייה","רווח","שיא","חיובי","הצלחה",
    "jump","surge","gain","profit","beat"
]

NEG_WORDS = [
    "נפילה","ירידה","הפסד","קריסה","שלילי",
    "drop","fall","loss","crash","miss"
]

# =========================
# עזר
# =========================
def get_il_time():
    return datetime.now(IL_TIMEZONE).strftime("%Y-%m-%d %H:%M")

def normalize_url(url):
    p = urlparse(url)
    return f"{p.netloc}{p.path}".lower().rstrip("/")

def extract_domain(url):
    return urlparse(url).netloc.replace("www.","").split(".")[0]

def send_notification(msg):
    if not NTFY_TOPIC:
        return
    requests.post(
        f"https://ntfy.sh/{NTFY_TOPIC}",
        data=msg.encode("utf-8"),
        headers={"Title": "חדשות חדשות"}
    )

def get_sheet_client():
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]

    creds_json = os.environ.get("GOOGLE_CREDS_JSON")

    if not creds_json:
        raise RuntimeError("GOOGLE_CREDS_JSON environment variable is missing")

    creds_dict = json.loads(creds_json)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(
        creds_dict, scope
    )

    return gspread.authorize(creds)

def update_header_color(worksheet, color_type, index):
    if color_type == "red":
        color = {'red': 1.0, 'green': 0.8, 'blue': 0.8} 
    else:
        color = {'red': 0.8, 'green': 1.0, 'blue': 0.8}
    try:
        worksheet.format(index,
        {"backgroundColor": color, "textFormat": {"bold": True}})
    except: pass

# =========================
# סריקת HTML אמיתית
# =========================
def extract_articles_from_html(url):
    res = requests.get(url, headers=HEADERS, timeout=10)
    soup = BeautifulSoup(res.text, "html.parser")

    articles = []
    for tag in soup.find_all(["article","h2","h3"]):
        a = tag.find("a", href=True)
        if not a:
            continue

        title = a.get_text(" ", strip=True)
        link = urljoin(url, a["href"])

        if len(title) < 12:
            continue
        if not link.startswith("http"):
            continue

        articles.append((title, link))

    return articles[:30]

# =========================
# בדיקת מילות מפתח
# =========================
def keyword_match(text, keywords):
    t = text.lower()
    for he,en in keywords:
        if he and he.lower() in t:
            return he
        if en and en.lower() in t:
            return en
    return None

# =========================
# סריקת אתר בודד
# =========================
def scrape_single_site(site_data, keywords):
    url, row_idx = site_data
    found = []
    status = "OK"

    try:
        # RSS
        rss_map = {
            "ynet": "https://www.ynet.co.il/Integration/StoryRss2.xml",
            "globes": "https://www.globes.co.il/webservice/rss/rss.aspx?BID=2",
            "calcalist": "https://www.calcalist.co.il/GeneralRSS/0,16335,L-8,00.xml",
            "themarker": "https://www.themarker.com/srv/tm-market-rss"
        }

        domain = extract_domain(url)
        rss_url = rss_map.get(domain)

        if rss_url:
            feed = feedparser.parse(rss_url)
            for e in feed.entries[:20]:
                kw = keyword_match(e.title, keywords)
                if kw:
                    found.append({
                        "Date": get_il_time(),
                        "Keyword": kw,
                        "Article URL": e.link,
                        "Site URL": domain,
                        "Title": e.title,
                        "Is_User_Site": True
                    })

        # HTML תמיד
        html_articles = extract_articles_from_html(url)
        for title, link in html_articles:
            kw = keyword_match(title, keywords)
            if kw:
                found.append({
                    "Date": get_il_time(),
                    "Keyword": kw,
                    "Article URL": link,
                    "Site URL": domain,
                    "Title": title,
                    "Is_User_Site": True
                })

    except Exception as e:
        status = "Error"

    if not found and status == "OK":
        status = "Active (no matches)"

    return found, status, row_idx

# =========================
# ניתוח מקומי (בלי AI)
# =========================
def local_sentiment(keyword, df):
    subset = df[df["Keyword"] == keyword]
    text = " ".join(subset["Title"].astype(str)).lower()

    pos = sum(w in text for w in POS_WORDS)
    neg = sum(w in text for w in NEG_WORDS)
    user_hits = (subset["Sort_Priority"] == 1).sum()

    score = pos - neg + user_hits

    if score >= 4:
        return "Strong Buy", "ריבוי אזכורים חיוביים ממקורות מרכזיים"
    if score >= 2:
        return "Buy", "נטייה חיובית בסיקור"
    if score <= -3:
        return "Strong Sell", "סיקור שלילי חזק"
    if score <= -1:
        return "Sell", "נטייה שלילית"
    return "Hold", "סיקור מעורב או חלש"

# =========================
# MAIN
# =========================
def main():
    gc = get_sheet_client()
    sh = gc.open(SHEET_NAME)

    ws_kw = sh.worksheet("מילות מפתח")
    ws_sites = sh.worksheet("אתרים לחיפוש")
    ws_log = sh.worksheet("תוצאות החיפוש")
    ws_dec = sh.worksheet("החלטות")

    update_header_color(ws_kw, "red", "B")
    update_header_color(ws_sites, "red", "B")
    update_header_color(ws_log, "red", "E")
    update_header_color(ws_dec, "red", "E")

    try:

        # מילות מפתח
        keywords = []
        for r in ws_kw.get_all_values()[1:]:
            if r and (r[0] or r[1]):
                keywords.append((r[0], r[1]))

        # אתרים
        sites_data = [
            (row[0], idx)
            for idx,row in enumerate(ws_sites.get_all_values()[1:], start=2)
            if row and row[0].startswith("http")
        ]

        all_articles = []
        site_status = {}

        with ThreadPoolExecutor(max_workers=8) as ex:
            futures = [ex.submit(scrape_single_site, s, keywords) for s in sites_data]
            for f in as_completed(futures):
                arts, status, idx = f.result()
                all_articles.extend(arts)
                site_status[idx] = status

        # עדכון סטטוס אתרים
        for idx,st in site_status.items():
            ws_sites.update(f"C{idx}", st)

        df = pd.DataFrame(all_articles)
        if df.empty:
            return

        df["normalized"] = df["Article URL"].apply(normalize_url)
        df["Sort_Priority"] = 1

        # החלטות
        ws_dec.clear()
        ws_dec.append_row(["תאריך","מילת מפתח","המלצה","הסבר","כמות כתבות"])

        for kw in df["Keyword"].unique():
            rec, expl = local_sentiment(kw, df)
            ws_dec.append_row([
                get_il_time(),
                kw,
                rec,
                expl,
                int((df["Keyword"] == kw).sum())
            ])

        # לוג
        ws_log.clear()
        ws_log.append_row(["תאריך","מילת מפתח","קישור","אתר","כותרת"])
        for _,r in df.iterrows():
            ws_log.append_row([
                r["Date"], r["Keyword"], r["Article URL"], r["Site URL"], r["Title"]
            ])

        send_notification("נמצאו כתבות חדשות")

    finally:
        update_header_color(ws_kw, "green", "B")
        update_header_color(ws_sites, "green", "B")
        update_header_color(ws_log, "green", "E")
        update_header_color(ws_dec, "green", "E")

if __name__ == "__main__":
    main()

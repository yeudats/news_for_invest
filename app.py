import os
import json
from dotenv import load_dotenv
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import requests
from bs4 import BeautifulSoup
import feedparser
from deep_translator import GoogleTranslator
import pandas as pd
from datetime import datetime
import pytz
from urllib.parse import quote, urljoin, urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import base64
from google import genai
import time

# --- הגדרות ---
load_dotenv()

NTFY_TOPIC = os.environ.get("NTFY_TOPIC_env")
SHEET_NAME = os.environ.get("SHEET_NAME_env")
SHEET_LINK = os.environ.get("SHEET_LINK_env")
GOOGLE_API_KEY = os.environ.get("GOOGLE_API_KEY")

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'
}

IL_TIMEZONE = pytz.timezone('Asia/Jerusalem')

if GOOGLE_API_KEY:
    client = genai.Client(api_key=GOOGLE_API_KEY, http_options={'api_version': 'v1'})
else:
    print("WARNING: GOOGLE_API_KEY is missing!")

# --- פונקציות עזר ---

def get_il_time():
    return datetime.now(IL_TIMEZONE).strftime("%Y-%m-%d %H:%M")

def extract_domain_name(url):
    try:
        if not url: return ""
        parsed = urlparse(url)
        domain = parsed.netloc.replace("www.", "").split('.')[0]
        return domain.lower()
    except: return ""

def extract_site_name(url, title=None, is_google_news=False):
    if is_google_news and title:
        parts = title.rsplit(' - ', 1)
        if len(parts) > 1: return parts[1].strip()
    try:
        domain = urlparse(url).netloc
        return domain.replace("www.", "")
    except: return "Unknown"

def clean_title_google_news(title):
    parts = title.rsplit(' - ', 1)
    return parts[0].strip() if len(parts) > 1 else title

def normalize_url(url):
    try:
        parsed = urlparse(url)
        clean = f"{parsed.netloc}{parsed.path}".lower().replace("www.", "")
        return clean.strip('/')
    except: return url

def send_notification(message):
    try:
        url = f"https://ntfy.sh/{NTFY_TOPIC}"
        headers = {"Title": "New Articles Found", "Priority": "3"}
        requests.post(url, data=message.encode('utf-8'), headers=headers, timeout=10)
    except: pass

def get_sheet_client():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds_json = os.environ.get("GOOGLE_CREDS_JSON")
    if creds_json:
        return gspread.authorize(ServiceAccountCredentials.from_json_keyfile_dict(json.loads(creds_json), scope))
    return gspread.authorize(ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope))

def contains_hebrew(text):
    return any("\u0590" <= c <= "\u05FF" for c in text)

def update_header_color(worksheet, color_type, header_length):
    color = {'red': 1.0, 'green': 0.8, 'blue': 0.8} if color_type == "red" else {'red': 0.8, 'green': 1.0, 'blue': 0.8}
    try: worksheet.format(f"A1:{header_length}1", {"backgroundColor": color, "textFormat": {"bold": True}})
    except: pass

def check_keyword_in_article_body(article_url, keywords):
    try:
        response = requests.get(article_url, headers=HEADERS, timeout=5)
        if response.status_code != 200: return False, ""
        text = BeautifulSoup(response.content, 'html.parser').get_text(" ", strip=True).lower()
        for he, en in keywords:
            if (he and he.lower() in text) or (en and en.lower() in text):
                return True, (he if he else en)
    except: pass
    return False, ""

def scrape_single_site(site_data, keywords):
    url, row_idx = site_data
    found, status = [], "OK"
    try:
        rss_url = url
        if "ynet" in url: rss_url = "https://www.ynet.co.il/Integration/StoryRss2.xml"
        elif "globes" in url: rss_url = "https://www.globes.co.il/webservice/rss/rss.aspx?BID=2"
        
        response = requests.get(rss_url, headers=HEADERS, timeout=10)
        if response.status_code == 403: return [], "Blocked (403)", row_idx
        if response.status_code != 200: return [], f"Error {response.status_code}", row_idx

        cur_time = get_il_time()
        if "xml" in response.headers.get('Content-Type', '') or rss_url.endswith('xml'):
            feed = feedparser.parse(response.content)
            for entry in feed.entries[:20]:
                match, kw = False, ""
                for he, en in keywords:
                    if (he and he.lower() in entry.title.lower()) or (en and en.lower() in entry.title.lower()):
                        match, kw = True, (he if he else en); break
                if not match: match, kw = check_keyword_in_article_body(entry.link, keywords)
                if match:
                    found.append({'Date': cur_time, 'Keyword': kw, 'Article URL': entry.link, 'Site URL': extract_site_name(url), 'Title': entry.title, 'Is_User_Site': True})
        else:
            soup = BeautifulSoup(response.content, 'html.parser')
            for a in soup.find_all('a', href=True)[:30]:
                link = urljoin(url, a['href'])
                match, kw = check_keyword_in_article_body(link, keywords)
                if match:
                    found.append({'Date': cur_time, 'Keyword': kw, 'Article URL': link, 'Site URL': extract_site_name(url), 'Title': a.get_text(" ", strip=True), 'Is_User_Site': True})
    except Exception as e: status = f"Err: {str(e)[:15]}"
    return found, status, row_idx

# --- ניתוח מאוחד בבקשה אחת ---
def analyze_all_keywords_at_once(data_dict):
    if not GOOGLE_API_KEY or not data_dict: return None
    
    # בניית פרומפט מאוחד
    prompt = "You are a financial analyst. Analyze the following news for multiple subjects. For each subject, provide a recommendation (Buy/Sell/Hold/Strong Buy/Strong Sell) and a short 1-2 sentence explanation in Hebrew.\n\n"
    
    for kw, articles in data_dict.items():
        prompt += f"### Subject: {kw}\n"
        for i, a in enumerate(articles[:15], 1):
            prompt += f"{i}. {a['Title']} (Source: {a['Site URL']})\n"
        prompt += "\n"

    prompt += "\nOutput MUST be a valid JSON list of objects: [{\"keyword\": \"...\", \"recommendation\": \"...\", \"explanation\": \"...\"}]"

    try:
        response = client.models.generate_content(model="gemini-1.5-flash", contents=prompt)
        text = response.text.strip()
        if "```json" in text: text = text.split("```json")[1].split("```")[0]
        return json.loads(text)
    except Exception as e:
        return f"Error: {str(e)}"

# --- תהליך ראשי ---
def background_process():
    print("Starting process...")
    client = get_sheet_client()
    sh = client.open(SHEET_NAME)
    ws_kwd, ws_sites, ws_log, ws_decisions = sh.worksheet("מילות מפתח"), sh.worksheet("אתרים לחיפוש"), sh.worksheet("תוצאות החיפוש"), sh.worksheet("החלטות")

    # 1. טעינת מילות מפתח
    k_vals = ws_kwd.get_all_values()[1:]
    keywords = []
    for row in k_vals:
        if any(row): keywords.append((row[0], row[1]))

    # 2. סריקת אתרים שהמשתמש נתן (עדיפות 1)
    print("Scraping user sites...")
    priority_sites = [(r[0], i) for i, r in enumerate(ws_sites.get_all_values()[1:], 2) if r and r[0].startswith('http')]
    user_domains = {extract_domain_name(url) for url, _ in priority_sites}
    
    new_articles, statuses = [], []
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(scrape_single_site, s, keywords) for s in priority_sites]
        for f in as_completed(futures):
            arts, stat, idx = f.result()
            new_articles.extend(arts)
            statuses.append({'range': f'C{idx}', 'values': [[stat]]})
    
    if statuses: ws_sites.batch_update(statuses)

    # 3. סריקת Google News (עדיפות נמוכה יותר)
    print("Scraping Google News...")
    cur_time = get_il_time()
    for he, en in keywords:
        q = he if he else en
        try:
            feed = feedparser.parse(f"[https://news.google.com/rss/search?q=](https://news.google.com/rss/search?q=){quote(q)}&hl=he&gl=IL&ceid=IL:he")
            for entry in feed.entries[:10]:
                src = extract_site_name(entry.link, entry.title, True)
                new_articles.append({'Date': cur_time, 'Keyword': he, 'Article URL': entry.link, 'Site URL': src, 'Title': clean_title_google_news(entry.title), 'Is_User_Site': (extract_domain_name(src) in user_domains)})
        except: pass

    # 4. עיבוד ומיון
    df = pd.DataFrame(new_articles).drop_duplicates(subset=['Article URL'])
    if df.empty: return print("No articles found.")

    def get_priority(row):
        if row['Is_User_Site']: return 1
        return 2

    df['Priority'] = df.apply(get_priority, axis=1)
    df = df.sort_values(by=['Keyword', 'Priority', 'Date'], ascending=[True, True, False])

    # 5. ניתוח ג'ימיני מאוחד
    print("Starting Gemini Bulk Analysis...")
    analysis_input = {kw: group.to_dict('records') for kw, group in df.groupby('Keyword')}
    results = analyze_all_keywords_at_once(analysis_input)

    # 6. עדכון גיליון החלטות
    ws_decisions.clear()
    if isinstance(results, str) and "Error" in results:
        ws_decisions.append_row(["שגיאת מערכת / חסימה", results])
    elif results:
        ws_decisions.append_row(["תאריך ושעה", "מילת מפתח", "המלצה", "הסבר", "כמות כתבות"])
        for res in results:
            kw = res.get('keyword')
            count = len(df[df['Keyword'] == kw])
            ws_decisions.append_row([cur_time, kw, res.get('recommendation'), res.get('explanation'), count])

    # 7. עדכון לוג תוצאות
    final_rows = [["תאריך ושעה", "מילת מפתח", "קישור לכתבה", "שם האתר", "כותרת"]]
    for kw, group in df.groupby('Keyword'):
        for _, r in group.head(15).iterrows():
            final_rows.append([r['Date'], r['Keyword'], r['Article URL'], r['Site URL'], r['Title']])
        final_rows.append([""]*5)

    ws_log.clear()
    ws_log.update(final_rows)
    print("Done.")

if __name__ == "__main__":
    background_process()
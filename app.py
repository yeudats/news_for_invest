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
from flask import Flask
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import base64

app = Flask(__name__)

# --- הגדרות ---
load_dotenv()
NTFY_TOPIC = os.environ.get("NTFY_TOPIC_env")
SHEET_NAME = os.environ.get("SHEET_NAME_env")
SHEET_LINK = os.environ.get("SHEET_LINK_env")
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'
}

IL_TIMEZONE = pytz.timezone('Asia/Jerusalem')

# --- פונקציות עזר ---

def get_il_time():
    return datetime.now(IL_TIMEZONE).strftime("%Y-%m-%d %H:%M")

def normalize_url(url):
    """
    מנקה את הכתובת כדי למנוע כפילויות.
    מסיר פרמטרים (אחרי סימן שאלה) ומסיר http/www כדי להשוות נטו את הכתובת.
    """
    if not url: return ""
    try:
        parsed = urlparse(url)
        # בניית הכתובת מחדש ללא query parameters
        clean = f"{parsed.netloc}{parsed.path}"
        # הסרת קידומות נפוצות לנרמול
        clean = clean.lower().replace("www.", "").replace("https://", "").replace("http://", "")
        # הסרת לוכסן בסוף אם יש
        if clean.endswith('/'): clean = clean[:-1]
        return clean
    except:
        return url

def send_notification(message):
    try:
        url = f"https://ntfy.sh/{NTFY_TOPIC}"
        title = "כתבות חדשות"
        encoded_title = f"=?utf-8?b?{base64.b64encode(title.encode('utf-8')).decode('utf-8')}?="
        headers = {
            "Title": encoded_title,
            "Click": SHEET_LINK,
            "Tags": "newspaper",
            "Priority": "3"
        }
        requests.post(url, data=message.encode('utf-8'), headers=headers, timeout=10)
    except Exception as e:
        print(f"Notification Error: {e}")

def get_sheet_client():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds_json = os.environ.get("GOOGLE_CREDS_JSON")
    if creds_json:
        info = json.loads(creds_json)
        creds = ServiceAccountCredentials.from_json_keyfile_dict(info, scope)
    else:
        creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)
    return gspread.authorize(creds)

def contains_hebrew(text):
    return any("\u0590" <= c <= "\u05FF" for c in text)

def update_header_color(worksheet, color_type, header_length):
    color = {'red': 1.0, 'green': 0.8, 'blue': 0.8} if color_type == "red" else {'red': 0.8, 'green': 1.0, 'blue': 0.8}
    try:
        worksheet.format(f"A1:{header_length}1", {"backgroundColor": color, "textFormat": {"bold": True}})
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
    found = []
    status = "OK"
    try:
        rss_url = url
        # רשימת RSS מהירה
        if not "xml" in url and not "rss" in url:
            if "ynet" in url: rss_url = "https://www.ynet.co.il/Integration/StoryRss2.xml"
            elif "globes" in url: rss_url = "https://www.globes.co.il/webservice/rss/rss.aspx?BID=2"
            elif "calcalist" in url: rss_url = "https://www.calcalist.co.il/GeneralRSS/0,16335,L-8,00.xml"
            elif "themarker" in url: rss_url = "https://www.themarker.com/srv/tm-market-rss"
            elif "bizportal" in url: rss_url = "https://www.bizportal.co.il/forumpages/rss/general"

        response = requests.get(rss_url, headers=HEADERS, timeout=10)
        if response.status_code in [403, 401]: return [], "Blocked", row_idx

        current_time_str = get_il_time()

        if "xml" in response.headers.get('Content-Type', '') or rss_url.endswith('xml'):
            feed = feedparser.parse(response.content)
            for entry in feed.entries[:30]:
                t, l = entry.title, entry.link
                match, kw = False, ""
                for he, en in keywords:
                    if (he and he.lower() in t.lower()) or (en and en.lower() in t.lower()):
                        match, kw = True, (he if he else en); break
                if not match: match, kw = check_keyword_in_article_body(l, keywords)
                if match:
                    found.append({'Date': current_time_str, 'Keyword': kw, 'Article URL': l, 'Site URL': url, 'Title': t})
        else:
            soup = BeautifulSoup(response.content, 'html.parser')
            links = soup.find_all('a', href=True)[:30]
            for a in links:
                t = a.get_text(" ", strip=True)
                l = urljoin(url, a['href'])
                if len(t) < 5: continue
                match, kw = check_keyword_in_article_body(l, keywords)
                if match:
                    found.append({'Date': current_time_str, 'Keyword': kw, 'Article URL': l, 'Site URL': url, 'Title': t})
    except Exception as e:
        status = f"Error: {str(e)[:10]}"
    return found, status, row_idx

def background_process():
    print("Starting background process...")
    client = get_sheet_client()
    sh = client.open(SHEET_NAME)
    
    ws_kwd = sh.worksheet("מילות מפתח")
    ws_sites = sh.worksheet("אתרים לחיפוש")
    ws_log = sh.worksheet("תוצאות החיפוש")

    update_header_color(ws_kwd, "red", "B")
    update_header_color(ws_sites, "red", "B")
    update_header_color(ws_log, "red", "E")

    # --- 1. טעינת היסטוריה (כתבות ישנות) ---
    existing_data = ws_log.get_all_values()
    df_old = pd.DataFrame()
    col_map = {"תאריך ושעה": "Date", "מילת מפתח": "Keyword", "קישור לכתבה": "Article URL", "קישור לאתר": "Site URL", "כותרת": "Title"}
    
    if len(existing_data) > 1:
        headers_row = existing_data[0]
        data_rows = [r for r in existing_data[1:] if r and len(r) > 2 and r[2]] # סינון שורות ריקות
        if data_rows:
            temp_df = pd.DataFrame(data_rows, columns=headers_row).rename(columns=col_map)
            # בחירת רק העמודות הרלוונטיות למקרה של סטיות
            needed = list(col_map.values())
            if all(c in temp_df.columns for c in needed):
                df_old = temp_df[needed].copy()
                # יצירת עמודת נרמול להשוואה
                df_old['normalized_url'] = df_old['Article URL'].apply(normalize_url)

    # רשימת כתובות שכבר קיימות בהיסטוריה (לצורך סימון "לא חדש")
    old_urls_set = set(df_old['normalized_url'].tolist()) if not df_old.empty else set()

    # --- 2. עדכון מילות מפתח ---
    k_vals = ws_kwd.get_all_values()
    keywords = []
    updates = []
    trans_en = GoogleTranslator(source='auto', target='en')
    trans_he = GoogleTranslator(source='auto', target='iw')

    for i, row in enumerate(k_vals[1:], 2):
        val_a = row[0].strip() if len(row) > 0 else ""
        val_b = row[1].strip() if len(row) > 1 else ""
        if not val_a and not val_b: continue

        final_he, final_en = "", ""
        if contains_hebrew(val_a): final_he = val_a
        elif val_a: final_en = val_a
        
        if contains_hebrew(val_b): final_he = val_b
        elif val_b: final_en = val_b
            
        if final_he and not final_en: final_en = trans_en.translate(final_he)
        elif final_en and not final_he:
            t = trans_he.translate(final_en)
            final_he = t if t.lower() != final_en.lower() else final_en

        keywords.append((final_he, final_en))
        if val_a != final_he or val_b != final_en:
            updates.append({'range': f'A{i}:B{i}', 'values': [[final_he, final_en]]})

    if updates: ws_kwd.batch_update(updates)

    # --- 3. סריקה חדשה ---
    new_articles = []
    priority_sites = [r[0] for r in ws_sites.get_all_values()[1:] if r and r[0].startswith('http')]
    sites_data = [(url, i) for i, url in enumerate(priority_sites, 2)]
    
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(scrape_single_site, s, keywords): s for s in sites_data}
        for future in as_completed(futures):
            arts, _, _ = future.result()
            new_articles.extend(arts)

    # Google News
    cur_time = get_il_time()
    for loc in [{'l': 'en', 'g': 'US', 'c': 'US:en', 'lbl': 'Global News'}, 
                {'l': 'he', 'g': 'IL', 'c': 'IL:he', 'lbl': 'Local News'}]:
        for he, en in keywords:
            q = en if loc['l'] == 'en' else he
            try:
                rss = f"https://news.google.com/rss/search?q={quote(q)}&hl={loc['l']}&gl={loc['g']}&ceid={loc['c']}"
                feed = feedparser.parse(rss)
                for entry in feed.entries[:10]:
                    new_articles.append({
                        'Date': cur_time, 'Keyword': he if he else en,
                        'Article URL': entry.link, 'Site URL': loc['lbl'], 'Title': entry.title
                    })
            except: pass

    # --- 4. עיבוד ומיון סופי ---
    df_new = pd.DataFrame(new_articles)
    
    if not df_new.empty:
        # תרגום כותרות
        translator = GoogleTranslator(source='en', target='iw')
        for idx, row in df_new.iterrows():
             if any(c.isalpha() and c.isascii() for c in row['Title']):
                try: df_new.at[idx, 'Title'] = translator.translate(row['Title'])
                except: pass
        
        # הוספת עמודת נרמול לחדשים
        df_new['normalized_url'] = df_new['Article URL'].apply(normalize_url)

    # איחוד כל הנתונים (חדש + ישן)
    df_combined = pd.concat([df_new, df_old], ignore_index=True) if not df_old.empty else df_new
    
    if not df_combined.empty:
        # הסרת כפילויות לפי הכתובת המנורמלת
        # אנו ממיינים קודם כדי לוודא שאם יש כפילות, נשמור את הגרסה שסימנו לה תאריך עדכני (למרות שזה לא קריטי אם זה אותו לינק)
        df_combined = df_combined.drop_duplicates(subset=['normalized_url'], keep='first')

        # --- לוגיקת המיון המתקדמת ---
        
        def calculate_priority(row):
            norm_url = row['normalized_url']
            url_full = row['Article URL']
            site_label = row['Site URL']
            
            # בדיקה: האם זו כתבה שקיימת בהיסטוריה?
            # אם היא הייתה ב-old_urls_set, היא נחשבת ישנה (Priority 4), גם אם מצאנו אותה שוב עכשיו.
            is_history = norm_url in old_urls_set
            
            if is_history:
                return 4 # כתבות ישנות לתחתית
            
            # אם זו כתבה חדשה באמת (לא בהיסטוריה):
            if any(ps in url_full for ps in priority_sites): return 1 # אתרי משתמש
            if site_label == 'Global News': return 2 # חו"ל
            return 3 # הארץ (או כל ברירת מחדל אחרת לחדש)

        df_combined['Sort_Priority'] = df_combined.apply(calculate_priority, axis=1)

        # מיון:
        # 1. לפי מילת מפתח
        # 2. לפי עדיפות (1,2,3 - חדשים למעלה, 4 - ישנים למטה)
        # 3. בתוך כל קבוצה - לפי תאריך (החדש ביותר ראשון)
        df_combined = df_combined.sort_values(
            by=['Keyword', 'Sort_Priority', 'Date'], 
            ascending=[True, True, False]
        )

        # בניית הגליון הסופי (עד 20 תוצאות)
        final_rows = [["תאריך ושעה", "מילת מפתח", "קישור לכתבה", "קישור לאתר", "כותרת"]]
        truly_new_keywords = set()

        grouped = df_combined.groupby('Keyword', sort=False) # sort=False שומר על סדר המיון שעשינו למעלה
        for kw, group in grouped:
            top_20 = group.head(20)
            
            # בדיקה עבור התראה: האם ב-20 הכתבות המוצגות יש משהו חדש באמת?
            # משהו חדש = Priority פחות מ-4 (כלומר 1, 2 או 3)
            if any(top_20['Sort_Priority'] < 4):
                truly_new_keywords.add(kw)

            for _, row in top_20.iterrows():
                final_rows.append([
                    row['Date'], row['Keyword'], row['Article URL'], row['Site URL'], row['Title']
                ])
            
            final_rows.append([""] * 5)
            final_rows.append([""] * 5)

        ws_log.clear()
        ws_log.update(final_rows)
        
        if truly_new_keywords:
            kws_str = ", ".join(list(truly_new_keywords))
            send_notification(f"כתבות חדשות עבור: {kws_str}")
        else:
            print("No truly new articles (priority 1-3) found.")

    update_header_color(ws_kwd, "green", "B")
    update_header_color(ws_sites, "green", "B")
    update_header_color(ws_log, "green", "E")
    print("Done.")

@app.route('/run-tasks')
def trigger_bot():
    thread = threading.Thread(target=background_process)
    thread.start()
    return "Job Started", 200

@app.route('/')
def home(): return "Bot is Alive", 200

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=int(os.environ.get("PORT", 5000)))
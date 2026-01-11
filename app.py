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
import pytz  # ספרייה לניהול אזורי זמן
from urllib.parse import quote, urljoin
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

# הגדרת אזור זמן לישראל
IL_TIMEZONE = pytz.timezone('Asia/Jerusalem')

# --- פונקציות עזר ---

def get_il_time():
    """מחזיר מחרוזת זמן נוכחי בישראל"""
    return datetime.now(IL_TIMEZONE).strftime("%Y-%m-%d %H:%M")

def send_notification(message):
    try:
        url = f"https://ntfy.sh/{NTFY_TOPIC}"
        
        title = "כתבות חדשות"
        encoded_title = f"=?utf-8?b?{base64.b64encode(title.encode('utf-8')).decode('utf-8')}?="
        
        headers = {
            "Title": encoded_title,
            "Click": SHEET_LINK,
            "Tags": "newspaper",
            "Priority": "3" # החזרתי לדיפולט (או 2 אם אתה מעדיף שקט)
        }
        
        response = requests.post(
            url, 
            data=message.encode('utf-8'), 
            headers=headers, 
            timeout=10
        )
        print(f"Notification sent! Status: {response.status_code}")
        
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
        worksheet.format(f"A1:{header_length}1", {
            "backgroundColor": color,
            "textFormat": {"bold": True}
        })
    except Exception as e:
        print(f"Color formatting error: {e}")

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
        if not "xml" in url and not "rss" in url:
            if "ynet" in url: rss_url = "https://www.ynet.co.il/Integration/StoryRss2.xml"
            elif "globes" in url: rss_url = "https://www.globes.co.il/webservice/rss/rss.aspx?BID=2"
            elif "calcalist" in url: rss_url = "https://www.calcalist.co.il/GeneralRSS/0,16335,L-8,00.xml"
            elif "themarker" in url: rss_url = "https://www.themarker.com/srv/tm-market-rss"
            elif "bizportal" in url: rss_url = "https://www.bizportal.co.il/forumpages/rss/general"

        response = requests.get(rss_url, headers=HEADERS, timeout=10)
        
        if response.status_code in [403, 401]:
            return [], "Blocked", row_idx

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
                    found.append({'Date': current_time_str, 
                                  'Keyword': kw, 'Article URL': l, 'Site URL': url, 'Title': t})
        
        else:
            soup = BeautifulSoup(response.content, 'html.parser')
            links = soup.find_all('a', href=True)[:30]
            for a in links:
                t = a.get_text(" ", strip=True)
                l = urljoin(url, a['href'])
                if len(t) < 5: continue
                match, kw = check_keyword_in_article_body(l, keywords)
                if match:
                    found.append({'Date': current_time_str, 
                                  'Keyword': kw, 'Article URL': l, 'Site URL': url, 'Title': t})
                    
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

    # --- 1. קריאת היסטוריה (כדי לשמור על כתבות ישנות) ---
    existing_data = ws_log.get_all_values()
    df_old = pd.DataFrame()
    
    # מיפוי עמודות עברית לאנגלית לעבודה פנימית
    col_map = {
        "תאריך ושעה": "Date",
        "מילת מפתח": "Keyword",
        "קישור לכתבה": "Article URL",
        "קישור לאתר": "Site URL",
        "כותרת": "Title"
    }
    
    if len(existing_data) > 1:
        # לוקחים את הכותרות מהשורה הראשונה ואת הדאטה מהשאר
        headers_row = existing_data[0]
        data_rows = existing_data[1:]
        
        # סינון שורות ריקות (כמו הרווחים שאנחנו מוסיפים)
        cleaned_rows = [r for r in data_rows if r and r[2]] # בודק שיש לינק
        
        if cleaned_rows:
            temp_df = pd.DataFrame(cleaned_rows, columns=headers_row)
            # שינוי שמות עמודות לאנגלית לצורך עיבוד
            temp_df = temp_df.rename(columns=col_map)
            # שומרים רק את העמודות הרלוונטיות
            needed_cols = list(col_map.values())
            # מוודאים שכל העמודות קיימות (למקרה של שינויים ידניים)
            if all(col in temp_df.columns for col in needed_cols):
                df_old = temp_df[needed_cols]

    # --- 2. מילות מפתח ---
    k_vals = ws_kwd.get_all_values()
    keywords = []
    updates = []
    translator_to_en = GoogleTranslator(source='auto', target='en')
    translator_to_he = GoogleTranslator(source='auto', target='iw')

    for i, row in enumerate(k_vals[1:], 2):
        val_a = row[0].strip() if len(row) > 0 else ""
        val_b = row[1].strip() if len(row) > 1 else ""
        if not val_a and not val_b: continue

        final_he, final_en = "", ""
        if contains_hebrew(val_a): final_he = val_a
        elif val_a: final_en = val_a
        
        if contains_hebrew(val_b): final_he = val_b
        elif val_b: final_en = val_b
            
        if final_he and not final_en:
            final_en = translator_to_en.translate(final_he)
        elif final_en and not final_he:
            translated = translator_to_he.translate(final_en)
            final_he = translated if translated.lower() != final_en.lower() else final_en

        keywords.append((final_he, final_en))
        if val_a != final_he or val_b != final_en:
            updates.append({'range': f'A{i}:B{i}', 'values': [[final_he, final_en]]})

    if updates: ws_kwd.batch_update(updates)

    # --- 3. סריקה חדשה ---
    new_articles_list = []
    priority_sites = [r[0] for r in ws_sites.get_all_values()[1:] if r and r[0].startswith('http')]
    sites_data = [(url, i) for i, url in enumerate(priority_sites, 2)]
    
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(scrape_single_site, s, keywords): s for s in sites_data}
        for future in as_completed(futures):
            arts, status, row_idx = future.result()
            new_articles_list.extend(arts)

    # Google News
    current_time_str = get_il_time()
    for loc in [{'l': 'en', 'g': 'US', 'c': 'US:en', 'label': 'Global News'}, 
                {'l': 'he', 'g': 'IL', 'c': 'IL:he', 'label': 'Local News'}]:
        for he, en in keywords:
            q = en if loc['l'] == 'en' else he
            try:
                rss = f"https://news.google.com/rss/search?q={quote(q)}&hl={loc['l']}&gl={loc['g']}&ceid={loc['c']}"
                feed = feedparser.parse(rss)
                for entry in feed.entries[:10]:
                    new_articles_list.append({
                        'Date': current_time_str,
                        'Keyword': he if he else en,
                        'Article URL': entry.link,
                        'Site URL': loc['label'],
                        'Title': entry.title
                    })
            except: pass

    # --- 4. עיבוד ומיזוג נתונים ---
    
    # אם יש כתבות חדשות, נתרגם כותרות
    if new_articles_list:
        translator = GoogleTranslator(source='en', target='iw')
        for art in new_articles_list:
             if any(c.isalpha() and c.isascii() for c in art['Title']):
                try: art['Title'] = translator.translate(art['Title'])
                except: pass

    df_new = pd.DataFrame(new_articles_list)
    
    # בדיקה מה באמת חדש (לצורך התראה)
    truly_new_keywords = set()
    if not df_new.empty:
        # אם אין היסטוריה בכלל, הכל חדש
        if df_old.empty:
            truly_new_keywords = set(df_new['Keyword'].unique())
        else:
            # מציאת כתבות שה-URL שלהן לא נמצא ב-Old
            existing_urls = set(df_old['Article URL'])
            new_items_df = df_new[~df_new['Article URL'].isin(existing_urls)]
            truly_new_keywords = set(new_items_df['Keyword'].unique())

    # מיזוג: חדש + ישן
    df_combined = pd.concat([df_new, df_old]) if not df_old.empty else df_new
    
    if not df_combined.empty:
        # הסרת כפילויות לפי URL (נשמור את העדכן ביותר אם יש חפיפה, למרות ש-URL זהה לא אמור להשתנות)
        df_combined = df_combined.drop_duplicates(subset=['Article URL'], keep='first')
        
        # פונקציית עדיפות
        def get_site_priority(row):
            url = row['Article URL']
            site_label = row['Site URL']
            if any(ps in url for ps in priority_sites): return 1
            if site_label == 'Global News': return 2
            return 3

        df_combined['Priority'] = df_combined.apply(get_site_priority, axis=1)
        
        # מיון: קודם לפי מילת מפתח, אחר כך עדיפות אתר, אחר כך תאריך (הכי חדש למעלה)
        # שים לב: המיון הוא קריטי כדי שה-head(20) יקח את הכי רלוונטיים/חדשים
        df_combined = df_combined.sort_values(by=['Keyword', 'Priority', 'Date'], ascending=[True, True, False])

        # בניית הרשימה הסופית
        # שינוי שמות עמודות חזרה לעברית
        rev_col_map = {v: k for k, v in col_map.items()}
        final_rows = [list(col_map.keys())] # כותרות
        
        grouped = df_combined.groupby('Keyword')
        for kw, group in grouped:
            # לוקחים עד 20 כתבות לכל מילת מפתח (שילוב של חדש וישן)
            top_20 = group.head(20)
            
            for _, row in top_20.iterrows():
                final_rows.append([
                    row['Date'], 
                    row['Keyword'], 
                    row['Article URL'], 
                    row['Site URL'], 
                    row['Title']
                ])
            
            # רווחים
            final_rows.append([""] * 5)
            final_rows.append([""] * 5)

        # כתיבה לגליון
        ws_log.clear()
        ws_log.update(final_rows)
        
        # שליחת התראה רק על מה שבאמת חדש
        if truly_new_keywords:
            found_kws = ", ".join(list(truly_new_keywords))
            send_notification(f"נמצאו כתבות חדשות עבור: {found_kws}")
        else:
            print("No truly new articles found, skipping notification.")

    update_header_color(ws_kwd, "green", "B")
    update_header_color(ws_sites, "green", "B")
    update_header_color(ws_log, "green", "E")

    print("Background process finished.")

@app.route('/run-tasks')
def trigger_bot():
    thread = threading.Thread(target=background_process)
    thread.start()
    return "Job Started in Background", 200

@app.route('/')
def home(): return "Bot is Alive", 200

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=int(os.environ.get("PORT", 5000)))
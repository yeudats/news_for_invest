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
from urllib.parse import quote, urljoin
from flask import Flask
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

app = Flask(__name__)

# --- הגדרות ---
load_dotenv()
NTFY_TOPIC = os.environ.get("NTFY_TOPIC_env")
SHEET_NAME = os.environ.get("SHEET_NAME_env")
SHEET_LINK = os.environ.get("SHEET_LINK_env")
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'
}

# --- פונקציות עזר ---

def send_notification(message):
    try:
        safe_title = quote("כתבות חדשות") 
        url = f"https://ntfy.sh/{NTFY_TOPIC}"
        
        headers = {
            "Title": safe_title,
            "Click": SHEET_LINK,
            "Tags": "chart_with_upwards_trend,newspaper",
            "Priority": "default"
        }
        
        # גוף ההודעה חייב להיות מקודד ל-utf-8
        requests.post(url, data=message.encode('utf-8'), headers=headers, timeout=5)
    except Exception as e:
        print(f"Notification Error: {e}")

def get_sheet_client():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    
    # בדיקה: האם אנחנו ברנדר (משתנה סביבה) או מקומי (קובץ)
    creds_json = os.environ.get("GOOGLE_CREDS_JSON")
    
    if creds_json:
        # קריאה מתוך המשתנה שהגדרנו ב-Render
        info = json.loads(creds_json)
        creds = ServiceAccountCredentials.from_json_keyfile_dict(info, scope)
    else:
        # למקרה שאתה עדיין מריץ מקומית עם הקובץ
        creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)
        
    return gspread.authorize(creds)

def contains_hebrew(text):
    """בדיקה האם הטקסט מכיל תווים בעברית"""
    return any("\u0590" <= c <= "\u05FF" for c in text)

def translate_text(text, dest_lang):
    try:
        return GoogleTranslator(source='auto', target=dest_lang).translate(text)
    except: return text

def update_header_color(worksheet, color_type):
    """משנה את צבע הרקע של הכותרת (שורה 1)"""
    # אדום בהיר: {'red': 1.0, 'green': 0.8, 'blue': 0.8}
    # ירוק בהיר: {'red': 0.8, 'green': 1.0, 'blue': 0.8}
    color = {'red': 1.0, 'green': 0.8, 'blue': 0.8} if color_type == "red" else {'red': 0.8, 'green': 1.0, 'blue': 0.8}
    try:
        worksheet.format("A1:E1", {
            "backgroundColor": color,
            "textFormat": {"bold": True}
        })
    except Exception as e:
        print(f"Color formatting error: {e}")

def check_keyword_in_article_body(article_url, keywords):
    """Deep Search - נכנס לכתבה, חיפוש מהיר עם Timeout קצר"""
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
    """הפונקציה שתרוץ במקביל לכל אתר"""
    url, row_idx = site_data
    found = []
    status = "OK"
    
    try:
        # ניסיון למצוא RSS דינמי
        rss_url = url
        if not "xml" in url and not "rss" in url:
             # רשימה מקוצרת לאתרים נפוצים
            if "ynet" in url: rss_url = "https://www.ynet.co.il/Integration/StoryRss2.xml"
            elif "globes" in url: rss_url = "https://www.globes.co.il/webservice/rss/rss.aspx?BID=2"
            elif "calcalist" in url: rss_url = "https://www.calcalist.co.il/GeneralRSS/0,16335,L-8,00.xml"
            elif "themarker" in url: rss_url = "https://www.themarker.com/srv/tm-market-rss"
            elif "bizportal" in url: rss_url = "https://www.bizportal.co.il/forumpages/rss/general"

        response = requests.get(rss_url, headers=HEADERS, timeout=10)
        
        # זיהוי חסימה
        if response.status_code in [403, 401]:
            return [], "Blocked", row_idx

        # RSS Parsing
        if "xml" in response.headers.get('Content-Type', '') or rss_url.endswith('xml'):
            feed = feedparser.parse(response.content)
            for entry in feed.entries[:30]:
                t, l = entry.title, entry.link
                match, kw = False, ""
                # בדיקה בכותרת
                for he, en in keywords:
                    if (he and he.lower() in t.lower()) or (en and en.lower() in t.lower()):
                        match, kw = True, (he if he else en); break
                # בדיקה בתוכן (Deep Search)
                if not match: match, kw = check_keyword_in_article_body(l, keywords)
                
                if match:
                    found.append({'Date': datetime.now().strftime("%Y-%m-%d %H:%M"), 
                                  'Keyword': kw, 'Article URL': l, 'Site URL': url, 'Title': t})
        
        # HTML Parsing (אם זה לא RSS)
        else:
            soup = BeautifulSoup(response.content, 'html.parser')
            links = soup.find_all('a', href=True)[:30]
            for a in links:
                t = a.get_text(" ", strip=True)
                l = urljoin(url, a['href'])
                if len(t) < 5: continue
                match, kw = check_keyword_in_article_body(l, keywords)
                if match:
                    found.append({'Date': datetime.now().strftime("%Y-%m-%d %H:%M"), 
                                  'Keyword': kw, 'Article URL': l, 'Site URL': url, 'Title': t})
                    
    except Exception as e:
        status = f"Error: {str(e)[:10]}"
    
    return found, status, row_idx

def update_header_color(worksheet, color_type):
    """משנה את צבע הרקע של הכותרת (שורה 1)"""
    # אדום בהיר: {'red': 1.0, 'green': 0.8, 'blue': 0.8}
    # ירוק בהיר: {'red': 0.8, 'green': 1.0, 'blue': 0.8}
    color = {'red': 1.0, 'green': 0.8, 'blue': 0.8} if color_type == "red" else {'red': 0.8, 'green': 1.0, 'blue': 0.8}
    try:
        worksheet.format("A1:E1", {
            "backgroundColor": color,
            "textFormat": {"bold": True}
        })
    except Exception as e:
        print(f"Color formatting error: {e}")

def background_process():
    print("Starting background process...")
    client = get_sheet_client()
    sh = client.open(SHEET_NAME)
    
    ws_kwd = sh.worksheet("מילות מפתח")
    ws_sites = sh.worksheet("אתרים לחיפוש")
    ws_log = sh.worksheet("תוצאות החיפוש")

    # שינוי צבע לאדום בהיר (ריצה התחילה)
    update_header_color(ws_kwd, "red")
    update_header_color(ws_sites, "red")
    update_header_color(ws_log, "red")

    # --- 1. טיפול במילות מפתח ---
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
            # פתרון ל-NVIDIA: אם התרגום מחזיר את אותה מילה או נכשל
            translated = translator_to_he.translate(final_en)
            final_he = translated if translated.lower() != final_en.lower() else final_en

        keywords.append((final_he, final_en))
        if val_a != final_he or val_b != final_en:
            updates.append({'range': f'A{i}:B{i}', 'values': [[final_he, final_en]]})

    if updates: ws_kwd.batch_update(updates)

    # --- 2. סריקה ---
    all_articles = []
    # שמירת רשימת האתרים "שלך" לצורך עדיפות במיון
    priority_sites = [r[0] for r in ws_sites.get_all_values()[1:] if r and r[0].startswith('http')]
    
    sites_data = [(url, i) for i, url in enumerate(priority_sites, 2)]
    
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(scrape_single_site, s, keywords): s for s in sites_data}
        for future in as_completed(futures):
            arts, status, row_idx = future.result()
            all_articles.extend(arts)

    # --- 3. Google News (חו"ל ואז הארץ) ---
    for loc in [{'l': 'en', 'g': 'US', 'c': 'US:en', 'label': 'Global News'}, 
                {'l': 'he', 'g': 'IL', 'c': 'IL:he', 'label': 'Local News'}]:
        for he, en in keywords:
            q = en if loc['l'] == 'en' else he
            try:
                rss = f"https://news.google.com/rss/search?q={quote(q)}&hl={loc['l']}&gl={loc['g']}&ceid={loc['c']}"
                feed = feedparser.parse(rss)
                for entry in feed.entries[:10]:
                    all_articles.append({
                        'Date': datetime.now().strftime("%Y-%m-%d %H:%M"),
                        'Keyword': he if he else en,
                        'Article URL': entry.link,
                        'Site URL': loc['label'],
                        'Title': entry.title
                    })
            except: pass

    # --- 4. מיון ובניית הגליון (לפי בקשתך) ---
    if all_articles:
        df = pd.DataFrame(all_articles).drop_duplicates(subset=['Article URL'])
        
        # פונקציית עזר לדירוג עדיפות אתר
        def get_site_priority(row):
            url = row['Article URL']
            site_label = row['Site URL']
            if any(ps in url for ps in priority_sites): return 1 # אתרים שלי - ראשון
            if site_label == 'Global News': return 2             # חו"ל - שני
            return 3                                            # הארץ - שלישי

        df['Priority'] = df.apply(get_site_priority, axis=1)
        # מיון: מילת מפתח -> עדיפות אתר -> שם אתר
        df = df.sort_values(by=['Keyword', 'Priority', 'Site URL'])

        # בניית הרשימה הסופית עם רווחים
        final_rows = [["Date", "Keyword", "Article URL", "Site URL", "Title"]] # כותרות
        
        grouped = df.groupby('Keyword')
        for kw, group in grouped:
            # לוקחים רק 10 כתבות לכל מילת מפתח
            top_10 = group.head(10)
            for _, row in top_10.iterrows():
                final_rows.append([row['Date'], row['Keyword'], row['Article URL'], row['Site URL'], row['Title']])
            
            # הוספת 2 שורות ריקות אחרי כל קבוצת מילת מפתח
            final_rows.append([""] * 5)
            final_rows.append([""] * 5)

        # עדכון הגליון
        ws_log.clear()
        ws_log.update(final_rows)
        
        # הודעה
        found_kws = ", ".join(df['Keyword'].unique().tolist())
        send_notification(f"מילות המפתח שנמצאו: {found_kws}")

    # סיום: שינוי צבע לירוק בהיר
    update_header_color(ws_kwd, "green")
    update_header_color(ws_sites, "green")
    update_header_color(ws_log, "green")
    
    print("Background process finished.")

@app.route('/run-tasks')
def trigger_bot():
    # מפעיל את הבוט ב-Thread נפרד ומחזיר תשובה מיידית ל-Cron Job
    thread = threading.Thread(target=background_process)
    thread.start()
    return "Job Started in Background", 200

@app.route('/')
def home(): return "Bot is Alive", 200

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=int(os.environ.get("PORT", 5000)))
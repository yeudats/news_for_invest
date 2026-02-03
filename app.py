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
import cloudscraper # ספרייה לעקיפת חסימות 403

# --- הגדרות ---
load_dotenv()

NTFY_TOPIC = os.environ.get("NTFY_TOPIC_env")
SHEET_NAME = os.environ.get("SHEET_NAME_env")
SHEET_LINK = os.environ.get("SHEET_LINK_env")

# הגדרת סקרייפר לעקיפת חסימות
scraper = cloudscraper.create_scraper(
    browser={
        'browser': 'chrome',
        'platform': 'windows',
        'desktop': True
    }
)

IL_TIMEZONE = pytz.timezone('Asia/Jerusalem')

# --- מילון סנטימנט פיננסי ---
POSITIVE_KEYWORDS = [
    # Hebrew
    "זינוק", "מזנקת", "מזנק", "עליות", "עלה", "רווח", "שיא", "חיובי", "צמיחה", "הצלחה", 
    "רכישה", "הסכם", "אישור", "הנפקה", "דיבידנד", "המלצה", "קניה", "שורי", "מתחזק", 
    "מומנטום", "הכנסות שיא", "מעל הצפי", "מכה את התחזיות", "אקזיט", "התאוששות",
    # English
    "surge", "jump", "soar", "rally", "gain", "profit", "record", "growth", "positive",
    "bullish", "buy", "outperform", "beat", "revenue", "deal", "approval", "merger",
    "acquisition", "dividend", "recovery", "upgrades", "strong"
]

NEGATIVE_KEYWORDS = [
    # Hebrew
    "צניחה", "קורסת", "נופלת", "ירידות", "ירד", "הפסד", "שפל", "שלילי", "אזהרה", "מיתון",
    "משבר", "תביעה", "חקירה", "פיטורים", "סגירה", "פשיטת רגל", "דובי", "נחלש", "סיכון",
    "מתחת לצפי", "אכזבה", "מחיקה", "חובות", "קריסה",
    # English
    "plunge", "crash", "drop", "fall", "loss", "negative", "bearish", "sell", "underperform",
    "miss", "warn", "recession", "crisis", "lawsuit", "investigation", "layoff", "bankruptcy",
    "debt", "risk", "weak", "down", "slump"
]

# --- פונקציות עזר ---

def get_il_time():
    return datetime.now(IL_TIMEZONE).strftime("%d.%m.%Y  %H:%M")

def extract_domain_name(url):
    try:
        if not url: return ""
        parsed = urlparse(url)
        domain = parsed.netloc
        domain = domain.replace("www.", "").split('.')[0]
        return domain.lower()
    except:
        return ""

def extract_site_name(url, title=None, is_google_news=False):
    if is_google_news and title:
        parts = title.rsplit(' - ', 1)
        if len(parts) > 1:
            return parts[1].strip()
    
    try:
        parsed = urlparse(url)
        domain = parsed.netloc
        if domain.startswith("www."):
            domain = domain[4:]
        return domain
    except:
        return "Unknown Source"

def clean_title_google_news(title):
    parts = title.rsplit(' - ', 1)
    if len(parts) > 1:
        return parts[0].strip()
    return title

def normalize_url(url):
    if not url: return ""
    try:
        parsed = urlparse(url)
        clean = f"{parsed.netloc}{parsed.path}"
        clean = clean.lower().replace("www.", "").replace("https://", "").replace("http://", "")
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

def update_header_color(worksheet, color_type, header_length):
    color = {'red': 1.0, 'green': 0.8, 'blue': 0.8} if color_type == "red" else {'red': 0.8, 'green': 1.0, 'blue': 0.8}
    try:
        worksheet.format(f"A1:{header_length}1", {"backgroundColor": color, "textFormat": {"bold": True}})
    except: pass

def check_keyword_in_article_body(article_url, keywords):
    try:
        # שימוש ב-scraper במקום requests
        response = scraper.get(article_url, timeout=10)
        if response.status_code != 200: return False, ""
        text = BeautifulSoup(response.content, 'html.parser').get_text(" ", strip=True).lower()
        
        for kw in keywords:
            if kw.lower() in text:
                return True, kw
    except: pass
    return False, ""

def scrape_single_site(site_data, keywords):
    url, row_idx = site_data
    found = []
    status = "OK"
    try:
        rss_url = url
        # ניסיונות זיהוי RSS נפוצים
        if not "xml" in url and not "rss" in url:
            if "ynet" in url: rss_url = "https://www.ynet.co.il/Integration/StoryRss2.xml"
            elif "globes" in url: rss_url = "https://www.globes.co.il/webservice/rss/rss.aspx?BID=2"
            elif "calcalist" in url: rss_url = "https://www.calcalist.co.il/GeneralRSS/0,16335,L-8,00.xml"
            elif "themarker" in url: rss_url = "https://www.themarker.com/srv/tm-market-rss"
            elif "bizportal" in url: rss_url = "https://www.bizportal.co.il/forumpages/rss/general"

        # שימוש ב-scraper כדי לעקוף 403
        response = scraper.get(rss_url, timeout=15)
        
        if response.status_code != 200:
            return [], f"Error {response.status_code}", row_idx

        current_time_str = get_il_time()
        is_rss = "xml" in response.headers.get('Content-Type', '') or rss_url.endswith('xml')

        if is_rss:
            feed = feedparser.parse(response.content)
            if not feed.entries: status = "No RSS Entries"
            for entry in feed.entries[:30]:
                t, l = entry.title, entry.link
                match, matched_kw = False, ""
                
                # בדיקה בכותרת
                for kw in keywords:
                    if kw.lower() in t.lower():
                        match, matched_kw = True, kw
                        break
                
                # בדיקה בגוף הכתבה
                if not match: 
                    match, matched_kw = check_keyword_in_article_body(l, keywords)
                
                if match:
                    site_name = extract_site_name(url)
                    found.append({
                        'Date': current_time_str,
                        'Keyword': matched_kw, 
                        'Article URL': l, 
                        'Site URL': site_name, 
                        'Title': t,
                        'Is_User_Site': True
                    })
        else:
            # HTML רגיל
            soup = BeautifulSoup(response.content, 'html.parser')
            links = soup.find_all('a', href=True)[:30]
            if not links: status = "No Links Found"
            for a in links:
                t = a.get_text(" ", strip=True)
                l = urljoin(url, a['href'])
                if len(t) < 10: continue
                
                # לולאה פשוטה על רשימת המילים
                for kw in keywords:
                    if kw.lower() in t.lower():
                        found.append({
                            'Date': current_time_str,
                            'Keyword': kw,
                            'Article URL': l,
                            'Site URL': extract_site_name(url),
                            'Title': t,
                            'Is_User_Site': True
                        })
                        break
    except Exception as e:
        status = f"Err: {str(e)[:15]}"

    if status == "OK" and not found:
        status = "Active (no matches)"

    return found, status, row_idx

# --- מנוע לוגי (ללא שינוי) ---
def analyze_sentiment_logic(grouped_articles):
    results = {}
    
    for kw, articles in grouped_articles.items():
        score = 0
        reasons = []
        relevant_articles = articles[:10]
        
        for article in relevant_articles:
            title = article['Title'].lower()
            for word in POSITIVE_KEYWORDS:
                if word in title:
                    score += 1
                    if word not in str(reasons): reasons.append(f"{word}+")
            for word in NEGATIVE_KEYWORDS:
                if word in title:
                    score -= 1
                    if word not in str(reasons): reasons.append(f"{word}-")

        recommendation = "לעמוד"
        sentiment_he = "מעורב/ללא כיוון ברור"
        
        if score >= 3:
            recommendation = "לקנות בחוזקה"
            sentiment_he = "חיובי מאוד"
        elif score >= 1:
            recommendation = "לקנות"
            sentiment_he = "חיובי"
        elif score <= -3:
            recommendation = "למכור בחוזקה"
            sentiment_he = "שלילי מאוד"
        elif score <= -1:
            recommendation = "למכור"
            sentiment_he = "שלילי"
            
        explanation = f"ציון: {score}. מילים: {', '.join(reasons[:5])}" if reasons else "לא נמצאו מילות מפתח מובהקות"

        results[kw] = {
            "recommendation": recommendation,
            "explanation": f"{sentiment_he}. {explanation}",
            "count": len(articles)
        }
        
    return results

def background_process():
    print("Starting process (Updated Single-Lang Mode)...")
    client = get_sheet_client()
    sh = client.open(SHEET_NAME)
    
    ws_kwd = sh.worksheet("מילות מפתח")
    ws_sites = sh.worksheet("אתרים לחיפוש")
    ws_log = sh.worksheet("תוצאות החיפוש")
    grouped_for_logic = {}
    try:
        ws_decisions = sh.worksheet("החלטות")
    except:
        ws_decisions = sh.add_worksheet(title="החלטות", rows=1000, cols=5)
        ws_decisions.append_row(["תאריך ושעה", "מילת מפתח", "המלצה", "הסבר להמלצה", "כמות כתבות"])

    update_header_color(ws_kwd, "red", "B")
    update_header_color(ws_sites, "red", "B")
    update_header_color(ws_log, "red", "E")
    update_header_color(ws_decisions, "red", "E")

    # --- 1. טעינת מילות מפתח (פשוטה, ללא תרגום) ---
    print("Loading keywords...")
    k_vals = ws_kwd.get_all_values()
    keywords = []
    # קריאת עמודה A בלבד, החל משורה 2
    for row in k_vals[1:]:
        if row and row[0].strip():
            keywords.append(row[0].strip())
    
    # הסרת כפילויות
    keywords = list(set(keywords))
    print(f"Keywords to search: {keywords}")

    # --- 2. טעינת היסטוריה וניקוי מילים שנמחקו ---
    print("Loading history & Cleaning deleted keywords...")
    existing_data = ws_log.get_all_values()
    df_old = pd.DataFrame()
    col_map = {"תאריך ושעה": "Date", "מילת מפתח": "Keyword", "קישור לכתבה": "Article URL", "שם האתר": "Site URL", "כותרת": "Title"}
    url_to_original_date = {} 

    if len(existing_data) > 1:
        headers_row = existing_data[0]
        if len(headers_row) > 3: headers_row[3] = "שם האתר"
        data_rows = [r for r in existing_data[1:] if r and len(r) > 2 and r[2]]
        if data_rows:
            temp_df = pd.DataFrame(data_rows, columns=headers_row).rename(columns=col_map)
            needed = list(col_map.values())
            if all(c in temp_df.columns for c in needed):
                df_old = temp_df[needed].copy()
                
                # --- סינון: השאר רק שורות שהמילה שלהן קיימת ברשימה החדשה ---
                before_count = len(df_old)
                df_old = df_old[df_old['Keyword'].isin(keywords)]
                print(f"Removed {before_count - len(df_old)} rows of deleted keywords.")

                df_old['normalized_url'] = df_old['Article URL'].apply(normalize_url)
                for _, row in df_old.iterrows():
                    url_to_original_date[row['normalized_url']] = row['Date']

    old_urls_set = set(df_old['normalized_url'].tolist()) if not df_old.empty else set()

    # --- 3. סריקה חדשה (שימוש ב-Scraper) ---
    print(f"Scraping sites...")
    new_articles = []
    priority_sites_raw = [r[0] for r in ws_sites.get_all_values()[1:] if r and r[0].startswith('http')]
    user_domains = {extract_domain_name(url) for url in priority_sites_raw}
    
    sites_data = [(url, i) for i, url in enumerate(priority_sites_raw, 2)]
    site_statuses = {}

    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(scrape_single_site, s, keywords): s for s in sites_data}
        for future in as_completed(futures):
            arts, status, ridx = future.result()
            new_articles.extend(arts)
            site_statuses[ridx] = status
    
    # עדכון סטטוסים בגיליון אתרים
    if site_statuses:
        status_updates = []
        for ridx, stat in site_statuses.items():
            status_updates.append({'range': f'B{ridx}', 'values': [[stat]]})
        try: ws_sites.batch_update(status_updates)
        except: pass

    # Google News Loop (פשוט יותר - לפי המילה המדויקת)
    cur_time = get_il_time()
    # נחפש גם באזור US וגם ב-IL כדי לכסות את כל האפשרויות, עם המילה המדויקת
    for loc in [{'l': 'en', 'g': 'US', 'c': 'US:en', 'lbl': 'Global'}, 
                {'l': 'he', 'g': 'IL', 'c': 'IL:he', 'lbl': 'Local'}]:
        for kw in keywords:
            try:
                rss = f"https://news.google.com/rss/search?q={quote(kw)}&hl={loc['l']}&gl={loc['g']}&ceid={loc['c']}"
                feed = feedparser.parse(rss)
                for entry in feed.entries[:10]:
                    real_source = extract_site_name(entry.link, entry.title, is_google_news=True)
                    clean_title_text = clean_title_google_news(entry.title)
                    new_articles.append({
                        'Date': cur_time, 
                        'Keyword': kw,
                        'Article URL': entry.link, 
                        'Site URL': real_source,
                        'Title': clean_title_text,
                        'Is_User_Site': False, 
                        'Region': loc['lbl']
                    })
            except: pass

    # --- 4. מיזוג ומיון ---
    df_new = pd.DataFrame(new_articles)
    if not df_new.empty:
        df_new['normalized_url'] = df_new['Article URL'].apply(normalize_url)
        # תרגום כותרות לאנגלית לעברית (אופציונלי - לנוחות המשתמש)
        translator = GoogleTranslator(source='en', target='iw')
        for idx, row in df_new.iterrows():
             if any(c.isalpha() and c.isascii() for c in row['Title']):
                try: df_new.at[idx, 'Title'] = translator.translate(row['Title'])
                except: pass
        
        def fix_date_if_exists(row):
            if row['normalized_url'] in url_to_original_date:
                return url_to_original_date[row['normalized_url']]
            return row['Date']
        df_new['Date'] = df_new.apply(fix_date_if_exists, axis=1)

    df_combined = pd.concat([df_new, df_old], ignore_index=True) if not df_old.empty else df_new
    
    if not df_combined.empty:
        df_combined = df_combined.drop_duplicates(subset=['normalized_url'], keep='first')

        def calculate_priority(row):
            norm_url = row['normalized_url']
            if norm_url in old_urls_set: return 4
            article_domain = extract_domain_name(row['Site URL'])
            if row.get('Is_User_Site', False) or article_domain in user_domains: return 1
            for ud in user_domains:
                if ud and ud in row['Article URL'].lower(): return 1
            if row.get('Region', '') == 'Global': return 2
            return 3 

        df_combined['Sort_Priority'] = df_combined.apply(calculate_priority, axis=1)
        df_combined = df_combined.sort_values(by=['Keyword', 'Sort_Priority', 'Date'], ascending=[True, True, False])
        
        for kw, group in df_combined.groupby("Keyword"):
            grouped_for_logic[kw] = group[['Title', 'Site URL']].to_dict('records')

    # --- 5. הפעלת מנוע לוגי ---
    if grouped_for_logic:
        print("Analyzing sentiment using logic rules...")
        analysis_result = analyze_sentiment_logic(grouped_for_logic)

        ws_decisions.clear()
        ws_decisions.append_row(["תאריך ושעה", "מילת מפתח", "המלצה", "הסבר להמלצה", "כמות כתבות"])

        now = get_il_time()
        rows = []
        for kw, data in analysis_result.items():
            rows.append([
                now,
                kw,
                data.get("recommendation", "Neutral"),
                data.get("explanation", ""),
                data.get("count", 0)
            ])
        ws_decisions.append_rows(rows)
    
    # --- 6. כתיבת לוג סופי ---
    if not df_combined.empty:
        final_rows = [["תאריך ושעה", "מילת מפתח", "קישור לכתבה", "שם האתר", "כותרת"]]
        truly_new_keywords = set()

        grouped = df_combined.groupby('Keyword', sort=False)
        for kw, group in grouped:
            top_20 = group.head(20)
            if any(top_20['Sort_Priority'] < 4): truly_new_keywords.add(kw)
            for _, row in top_20.iterrows():
                final_rows.append([row['Date'], row['Keyword'], row['Article URL'], row['Site URL'], row['Title']])
            final_rows.append([""] * 5)
            final_rows.append([""] * 5)

        ws_log.clear()
        ws_log.update(final_rows)
        
        if truly_new_keywords:
            kws_str = ", ".join(list(truly_new_keywords))
            send_notification(f"חדש: {kws_str}")

    update_header_color(ws_kwd, "green", "B")
    update_header_color(ws_sites, "green", "B")
    update_header_color(ws_log, "green", "E")
    update_header_color(ws_decisions, "green", "E")
    print("Done (Single-Lang Mode).")

if __name__ == "__main__":
    background_process()
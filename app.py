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

# הגדרת ג'ימיני
if GOOGLE_API_KEY:
    client = genai.Client(
        api_key=GOOGLE_API_KEY,
        http_options={'api_version': 'v1'}
        )
else:
    print("WARNING: GOOGLE_API_KEY is missing!")

# --- פונקציות עזר ---

def get_il_time():
    return datetime.now(IL_TIMEZONE).strftime("%Y-%m-%d %H:%M")

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
                    site_name = extract_site_name(url)
                    found.append({
                        'Date': current_time_str,
                        'Keyword': kw, 
                        'Article URL': l, 
                        'Site URL': site_name, 
                        'Title': t,
                        'Is_User_Site': True
                    })
        else:
            soup = BeautifulSoup(response.content, 'html.parser')
            links = soup.find_all('a', href=True)[:30]
            for a in links:
                t = a.get_text(" ", strip=True)
                l = urljoin(url, a['href'])
                if len(t) < 5: continue
                match, kw = check_keyword_in_article_body(l, keywords)
                if match:
                    site_name = extract_site_name(url)
                    found.append({
                        'Date': current_time_str, 
                        'Keyword': kw, 
                        'Article URL': l, 
                        'Site URL': site_name, 
                        'Title': t,
                        'Is_User_Site': True
                    })
    except Exception as e:
        status = f"Error: {str(e)[:10]}"
    return found, status, row_idx

# --- פונקציית ניתוח עם Gemini ---
def analyze_market_sentiment(keyword, articles):
    if not GOOGLE_API_KEY:
        print("Error: No API Key for Gemini.")
        return None

    try:
        # מגבלה: נשלח מקסימום 50 כותרות כדי לא להעמיס יותר מדי על הפרומפט
        articles_to_send = articles[:50] 
        articles_text = "\n".join([f"- {a['Title']} (Source: {a['Site URL']}, Date: {a.get('Date', 'N/A')})" for a in articles_to_send])
        
        prompt = f"""
        You are an expert financial analyst. 
        Analyze the following news headlines regarding the company/topic: "{keyword}".
        Total articles found: {len(articles)}. Displaying titles for analysis below:

        Headlines:
        {articles_text}
        
        Based on these headlines (and the sentiment of the text), decide on a stock recommendation (Buy, Sell, Hold, Strong Buy, Strong Sell).
        If the news are neutral or old, lean towards 'Hold'.
        Provide a short explanation (max 3 sentences) in Hebrew.
        
        Output JSON:
        {{
            "recommendation": "YOUR_RECOMMENDATION",
            "explanation": "YOUR_EXPLANATION_IN_HEBREW"
        }}
        """
        
        response = client.models.generate_content(
                model="models/gemini-2.0-flash", 
                contents=prompt
            )
        
        text_resp = response.text.strip()
        if text_resp.startswith("```json"):
            text_resp = text_resp[7:]
        if text_resp.endswith("```"):
            text_resp = text_resp[:-3]
            
        data = json.loads(text_resp)
        return data
    except Exception as e:
        print(f"Gemini Critical Error for {keyword}: {e}")
        return None

def background_process():
    print("Starting process...")
    client = get_sheet_client()
    sh = client.open(SHEET_NAME)
    
    ws_kwd = sh.worksheet("מילות מפתח")
    ws_sites = sh.worksheet("אתרים לחיפוש")
    ws_log = sh.worksheet("תוצאות החיפוש")
    
    try:
        ws_decisions = sh.worksheet("החלטות")
    except:
        ws_decisions = sh.add_worksheet(title="החלטות", rows=1000, cols=5)
        ws_decisions.append_row(["תאריך ושעה", "מילת מפתח", "המלצה", "הסבר", "כמות כתבות"])

    update_header_color(ws_kwd, "red", "B")
    update_header_color(ws_sites, "red", "B")
    update_header_color(ws_log, "red", "E")
    update_header_color(ws_decisions, "red", "E")

    # --- 1. טעינת היסטוריה ---
    print("Loading history...")
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
                df_old['normalized_url'] = df_old['Article URL'].apply(normalize_url)
                for _, row in df_old.iterrows():
                    url_to_original_date[row['normalized_url']] = row['Date']

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
    print(f"Scraping sites for {len(keywords)} keywords...")
    new_articles = []
    priority_sites = [r[0] for r in ws_sites.get_all_values()[1:] if r and r[0].startswith('http')]
    sites_data = [(url, i) for i, url in enumerate(priority_sites, 2)]
    
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(scrape_single_site, s, keywords): s for s in sites_data}
        for future in as_completed(futures):
            arts, _, _ = future.result()
            new_articles.extend(arts)

    # Google News Loop
    cur_time = get_il_time()
    for loc in [{'l': 'en', 'g': 'US', 'c': 'US:en', 'lbl': 'Global'}, 
                {'l': 'he', 'g': 'IL', 'c': 'IL:he', 'lbl': 'Local'}]:
        for he, en in keywords:
            q = en if loc['l'] == 'en' else he
            try:
                rss = f"https://news.google.com/rss/search?q={quote(q)}&hl={loc['l']}&gl={loc['g']}&ceid={loc['c']}"
                feed = feedparser.parse(rss)
                for entry in feed.entries[:10]:
                    real_source = extract_site_name(entry.link, entry.title, is_google_news=True)
                    clean_title_text = clean_title_google_news(entry.title)
                    new_articles.append({
                        'Date': cur_time, 
                        'Keyword': he if he else en,
                        'Article URL': entry.link, 
                        'Site URL': real_source,
                        'Title': clean_title_text,
                        'Is_User_Site': False,
                        'Region': loc['lbl']
                    })
            except: pass

    print(f"Total articles found (before filter): {len(new_articles)}")

    # --- 4. מיזוג ומיון ראשוני ---
    print("Processing main log...")
    df_new = pd.DataFrame(new_articles)
    if not df_new.empty:
        df_new['normalized_url'] = df_new['Article URL'].apply(normalize_url)
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
            if row.get('Is_User_Site', False): return 1
            if any(ps in row['Article URL'] for ps in priority_sites): return 1
            region = row.get('Region', '')
            if region == 'Global': return 2
            return 3 

        df_combined['Sort_Priority'] = df_combined.apply(calculate_priority, axis=1)
        df_combined = df_combined.sort_values(by=['Keyword', 'Sort_Priority', 'Date'], ascending=[True, True, False])


    # --- 5. ניתוח עם ג'ימיני (מבוצע על כל הכתבות, לא רק על החדשות) ---
    # נבצע ניתוח רק למילות מפתח שבהן יש כתבות חדשות בסריקה הזו, כדי לחסוך API
    keywords_with_new_data = set(df_new['Keyword'].unique()) if not df_new.empty else set()
    
    if GOOGLE_API_KEY and not df_combined.empty:
        print("Starting Gemini analysis for updated keywords...")
        
        # קריאת החלטות קיימות
        existing_decisions = ws_decisions.get_all_values()
        decision_header = ["תאריך ושעה", "מילת מפתח", "המלצה", "הסבר", "כמות כתבות"]
        
        # יצירת מילון: מפתח=מילת מפתח, ערך=שורה
        # כך נוכל לדרוס ישנים
        decision_map = {}
        if len(existing_decisions) > 1:
            for r in existing_decisions[1:]:
                if r and len(r) >= 2:
                    decision_map[r[1]] = r # r[1] is Keyword

        grouped_combined = df_combined.groupby('Keyword')
        
        for kw, group in grouped_combined:
            # מנתחים רק אם יש כתבות חדשות למילה הזו, או שהמילה עדיין לא קיימת בהחלטות
            if kw in keywords_with_new_data or kw not in decision_map:
                articles_list = group[['Title', 'Site URL', 'Date']].to_dict('records')
                print(f"Analyzing '{kw}' with {len(articles_list)} TOTAL articles...")
                
                analysis = analyze_market_sentiment(kw, articles_list)
                
                if analysis:
                    print(f"--> Decision for {kw}: {analysis.get('recommendation')}")
                    # עדכון המילון
                    decision_map[kw] = [
                        cur_time,
                        kw,
                        analysis.get('recommendation', 'N/A'),
                        analysis.get('explanation', ''),
                        len(articles_list)
                    ]
                    # השהיה כדי למנוע חסימת API
                    time.sleep(4)
                else:
                    print(f"--> Failed to analyze {kw}")
            else:
                pass # מדלגים על ניתוח כי אין מידע חדש

        # כתיבה חזרה לגיליון החלטות (מוחק הכל וכותב מחדש את המילון המעודכן)
        if decision_map:
            print("Updating Decisions sheet...")
            rows_to_write = [decision_header] + list(decision_map.values())
            ws_decisions.clear()
            ws_decisions.update(rows_to_write)
    
    # --- 6. כתיבת לוג סופי (הצגת 20 עליונות) ---
    if not df_combined.empty:
        final_rows = [["תאריך ושעה", "מילת מפתח", "קישור לכתבה", "שם האתר", "כותרת"]]
        truly_new_keywords = set()

        grouped = df_combined.groupby('Keyword', sort=False)
        for kw, group in grouped:
            top_20 = group.head(20) # רק לתצוגה במיין לוג
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
        else:
            print("No new priority articles.")

    update_header_color(ws_kwd, "green", "B")
    update_header_color(ws_sites, "green", "B")
    update_header_color(ws_log, "green", "E")
    update_header_color(ws_decisions, "green", "E")
    print("Done.")

if __name__ == "__main__":
    background_process()
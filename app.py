import time
import streamlit as st
import sqlite3
import pandas as pd
import google.generativeai as genai
import requests
import hashlib
from bs4 import BeautifulSoup
import re
import json
from PIL import Image
from main import (
    fetch_and_store_apps,
    patch_app_info_localization,
    patch_app_store_version_localization,
    fetch_app_info,
    fetch_app_info_localizations,
    fetch_app_store_versions,
    fetch_app_store_version_localizations,
    fetch_and_store_single_app,
    upload_screenshots_dashboard,
    fetch_screenshots,
    sync_db_to_github
)

FIELD_LIMITS = {
    "name":               30,
    "subtitle":           30,
    "description":        4000,
    "promotional_text":   170,
    "whats_new":          4000,
    "privacy_policy_url": 2000,
    "privacy_choices_url":2000,
    "marketing_url":      2000,
    "support_url":        2000,
    "keywords":           100,
}

locale_names = {
    "AR-SA": "Arabic (Saudi Arabia)",
    "CA":    "Catalan",
    "CS":    "Czech",
    "DA":    "Danish",
    "DE-DE": "German (Germany)",
    "EL":    "Greek",
    "EN-AU": "English (Australia)",
    "EN-CA": "English (Canada)",
    "EN-GB": "English (United Kingdom)",
    "EN-US": "English (United States)",
    "ES-ES": "Spanish (Spain)",
    "ES-MX": "Spanish (Mexico)",
    "FI":    "Finnish",
    "FR-CA": "French (Canada)",
    "FR-FR": "French (France)",
    "HE":    "Hebrew",
    "HI":    "Hindi",
    "HR":    "Croatian",
    "HU":    "Hungarian",
    "ID":    "Indonesian",
    "IT":    "Italian",
    "JA":    "Japanese",
    "KO":    "Korean",
    "MS":    "Malay",
    "NL-NL": "Dutch (Netherlands)",
    "NO":    "Norwegian",
    "PL":    "Polish",
    "PT-BR": "Portuguese (Brazil)",
    "PT-PT": "Portuguese (Portugal)",
    "RO":    "Romanian",
    "RU":    "Russian",
    "SK":    "Slovak",
    "SV":    "Swedish",
    "TH":    "Thai",
    "TR":    "Turkish",
    "UK":    "Ukrainian",
    "VI":    "Vietnamese",
    "ZH-HANS": "Chinese (Simplified)",
    "ZH-HANT": "Chinese (Traditional)",
    "BN":     "Bengali",                    # Bangladesh, India
    "FA":     "Persian (Farsi)",            # Iran
    "GU":     "Gujarati",                   # India
    "KN":     "Kannada",                    # India
    "ML":     "Malayalam",                  # India
    "MR":     "Marathi",                    # India
    "PA":     "Punjabi",                    # India, Pakistan
    "TA":     "Tamil",                      # India, Sri Lanka
    "TE":     "Telugu",                     # India
    "UR":     "Urdu",                       # Pakistan, India
    "IW":     "Hebrew (Legacy)",            # Old code (same as HE)
    "NB":     "Norwegian Bokmål"            # More specific than NO
}

# === DISPLAY TYPES PER PLATFORM ===
DISPLAY_TYPES = {
    "IOS": [
        "APP_IPHONE_65",
        "APP_IPHONE_69",
        "APP_IPAD_PRO_3GEN_129"
    ],
    "MAC_OS": [
        "APP_DESKTOP"
    ]
}

# === VALID SIZES ===
VALID_SIZES = {
    "APP_IPHONE_65": [
        (1242, 2688), (2688, 1242),
        (1284, 2778), (2778, 1284)
    ],
    "APP_IPHONE_69": [
        (1260, 2736), (2736, 1260),
        (1320, 2868), (2868, 1320),
        (1290, 2796), (2796, 1290)
    ],
    "APP_IPAD_PRO_3GEN_129": [
        (2064, 2752), (2752, 2064),
        (2048, 2732), (2732, 2048)
    ],
    "APP_DESKTOP": [
        (1280, 800), (1440, 900),
        (2560, 1600), (2880, 1800)
    ]
}

# -------------------------------
# Gemini AI Setup
# -------------------------------
GEMINI_API_KEY = "AIzaSyCuk67UDCg3Rdn4i_ARvmR44t_rAfz54tM"
gemini_model = None
if GEMINI_API_KEY:
    genai.configure(api_key=GEMINI_API_KEY)
    gemini_model = genai.GenerativeModel(
        'gemini-2.5-flash-lite',
        generation_config={"temperature": 0.0}
    )

# -------------------------------
# Password Hashing
# -------------------------------
def hash_password(password: str) -> str:
    return hashlib.sha256(password.encode()).hexdigest()

# -------------------------------
# Database Connection
# -------------------------------
def get_db_connection():
    return sqlite3.connect("app_store_data.db", timeout=30)

# -------------------------------
# Initialize Database
# -------------------------------
def initialize_database():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Stores
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS stores (
            store_id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            issuer_id TEXT NOT NULL,
            key_id TEXT NOT NULL,
            private_key TEXT NOT NULL
        )
    """)
    # Apps
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS apps (
            app_id TEXT PRIMARY KEY,
            store_id INTEGER,
            name TEXT,
            FOREIGN KEY (store_id) REFERENCES stores (store_id)
        )
    """)
    # App Info Localizations
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS app_info_localizations (
            localization_id TEXT PRIMARY KEY,
            app_id TEXT,
            store_id INTEGER,
            locale TEXT,
            name TEXT,
            subtitle TEXT,
            privacy_policy_url TEXT,
            privacy_choices_url TEXT,
            FOREIGN KEY (app_id) REFERENCES apps (app_id),
            FOREIGN KEY (store_id) REFERENCES stores (store_id)
        )
    """)
    # App Versions
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS app_versions (
            version_id TEXT PRIMARY KEY,
            app_id TEXT,
            store_id INTEGER,
            platform TEXT,
            FOREIGN KEY (app_id) REFERENCES apps (app_id),
            FOREIGN KEY (store_id) REFERENCES stores (store_id)
        )
    """)
    # App Version Localizations
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS app_version_localizations (
            localization_id TEXT PRIMARY KEY,
            version_id TEXT,
            app_id TEXT,
            store_id INTEGER,
            locale TEXT,
            description TEXT,
            keywords TEXT,
            marketing_url TEXT,
            promotional_text TEXT,
            support_url TEXT,
            whats_new TEXT,
            platform TEXT,
            FOREIGN KEY (version_id) REFERENCES app_versions (version_id),
            FOREIGN KEY (app_id) REFERENCES apps (app_id),
            FOREIGN KEY (store_id) REFERENCES stores (store_id)
        )
    """)
    # Screenshots
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS app_screenshots (
            id TEXT PRIMARY KEY,
            app_id TEXT,
            store_id INTEGER,
            localization_id TEXT,
            locale TEXT,
            display_type TEXT,
            url TEXT,
            width INTEGER,
            height INTEGER,
            platform TEXT
        )
    """)
    # Users
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            password TEXT NOT NULL,
            is_admin INTEGER DEFAULT 0
        )
    """)
    # User-Store Assignment
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS user_stores (
            user_id INTEGER,
            store_id INTEGER,
            FOREIGN KEY (user_id) REFERENCES users (id),
            FOREIGN KEY (store_id) REFERENCES stores (store_id),
            PRIMARY KEY (user_id, store_id)
        )
    """)

    conn.commit()
    conn.close()
    st.success("Database initialized successfully!")

# -------------------------------
# Create Default Admin
# -------------------------------
def create_default_admin():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM users WHERE is_admin = 1")
    if not cursor.fetchone():
        cursor.execute(
            "INSERT INTO users (username, password, is_admin) VALUES (?, ?, ?)",
            ("Admin", hash_password("admin123"), 1)
        )
        conn.commit()
    conn.close()

# -------------------------------
# Login
# -------------------------------
def login():
    if 'authenticated' not in st.session_state:
        st.session_state.authenticated = False
        st.session_state.user = None
        st.session_state.is_admin = False

    if not st.session_state.authenticated:
        st.markdown(
    """
    <h2 style='text-align:center; margin-bottom: 25px;'>
        🔒 App Store Metadata Manager
    </h2>
    """,
    unsafe_allow_html=True
)
        with st.container():
            col1, col2, col3 = st.columns([2, 2, 2])
            with col2:
                with st.form("login_form", clear_on_submit=True):
                    st.markdown("### Login")
                    username = st.text_input("Username")
                    password = st.text_input("Password", type="password")
                    submit = st.form_submit_button("Login")

                    if submit:
                        conn = get_db_connection()
                        cursor = conn.cursor()
                        cursor.execute(
                            "SELECT id, password, is_admin FROM users WHERE username = ?",
                            (username,)
                        )
                        user = cursor.fetchone()
                        conn.close()

                        if user and hash_password(password) == user[1]:
                            st.session_state.authenticated = True
                            st.session_state.user = {"id": user[0], "username": username}
                            st.session_state.is_admin = user[2] == 1
                            st.success(f"Welcome, {username}!")
                            time.sleep(1)
                            st.rerun()
                        else:
                            st.error("Invalid username or password")
        st.stop()

# -------------------------------
# Check Database
# -------------------------------
def check_database_exists():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='stores'")
    exists = cursor.fetchone() is not None
    conn.close()
    return exists

# -------------------------------
# iTunes Search
# -------------------------------
def search_itunes_apps(term, country, entity):
    if not term.strip():
        return []
    url = "https://itunes.apple.com/search"
    params = {"term": term, "country": country, "entity": entity, "limit": 200}
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        return response.json().get("results", [])
    except Exception as e:
        st.error(f"Search failed: {e}")
        return []

# -------------------------------
# Scrape App Store Page
# -------------------------------
def scrape_appstore_page(track_view_url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    try:
        response = requests.get(track_view_url, headers=headers, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        subtitle_elem = soup.find('h2', class_='product-header__subtitle') or soup.find('div', {'data-testid': 'product-subtitle'})
        subtitle = subtitle_elem.get_text(strip=True) if subtitle_elem else 'No subtitle available'
        return {'subtitle': subtitle}
    except Exception as e:
        st.error(f"Scraping failed for {track_view_url}: {e}")
        return None
    
# -------------------------------
# Load Data
# -------------------------------
def load_app_data(app_id, store_id):
    conn = get_db_connection()
    df = pd.read_sql_query("SELECT * FROM apps WHERE app_id = ? AND store_id = ?", conn, params=(app_id, store_id))
    conn.close()
    return df.iloc[0] if not df.empty else None

def load_app_info_localizations(app_id, store_id):
    conn = get_db_connection()
    df = pd.read_sql_query("SELECT * FROM app_info_localizations WHERE app_id = ? AND store_id = ?", conn, params=(app_id, store_id))
    conn.close()
    return df.drop_duplicates(subset=['localization_id'])

def load_version_localizations(app_id, store_id, platform=None):
    conn = get_db_connection()
    query = "SELECT * FROM app_version_localizations WHERE app_id = ? AND store_id = ?"
    params = [app_id, store_id]
    if platform:
        query += " AND platform = ?"
        params.append(platform)
    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    return df.drop_duplicates(subset=['localization_id'])

def load_screenshots(app_id, store_id, platform=None):
    conn = get_db_connection()
    query = "SELECT localization_id, locale, display_type, url, width, height, platform FROM app_screenshots WHERE app_id = ? AND store_id = ?"
    params = [app_id, store_id]
    if platform:
        query += " AND platform = ?"
        params.append(platform)
    query += " ORDER BY locale, display_type"
    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    return df

def get_apps_list(store_id):
    conn = get_db_connection()
    df = pd.read_sql_query("SELECT app_id, COALESCE(name, 'Unnamed App') AS name FROM apps WHERE store_id = ? ORDER BY name", conn, params=(store_id,))
    conn.close()
    return df

# -------------------------------
# Get Stores
# -------------------------------
def get_stores():
    conn = get_db_connection()
    cursor = conn.cursor()
    if st.session_state.get('is_admin', False):
        cursor.execute("SELECT * FROM stores ORDER BY name")
    else:
        cursor.execute("""
            SELECT s.* FROM stores s
            JOIN user_stores us ON s.store_id = us.store_id
            WHERE us.user_id = ?
            ORDER BY s.name
        """, (st.session_state.user['id'],))
    columns = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(cursor.fetchall(), columns=columns)
    conn.close()
    return df

# -------------------------------
# Store CRUD
# -------------------------------
def get_store_credentials(store_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT issuer_id, key_id, private_key FROM stores WHERE store_id = ?", (store_id,))
    result = cursor.fetchone()
    conn.close()
    return result if result else (None, None, None)

def add_store(name, issuer_id, key_id, private_key):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO stores (name, issuer_id, key_id, private_key) VALUES (?, ?, ?, ?)",
        (name, issuer_id, key_id, private_key)
    )
    store_id = cursor.lastrowid
    conn.commit()
    conn.close()
    return store_id

def delete_store(store_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM stores WHERE store_id = ?", (store_id,))
    cursor.execute("DELETE FROM user_stores WHERE store_id = ?", (store_id,))
    conn.commit()
    conn.close()

# -------------------------------
# User Management
# -------------------------------
def delete_user(user_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM user_stores WHERE user_id = ?", (user_id,))
    cursor.execute("DELETE FROM users WHERE id = ? AND is_admin = 0", (user_id,))
    deleted = cursor.rowcount > 0
    conn.commit()
    conn.close()
    return deleted

def remove_user_store_access(user_id, store_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM user_stores WHERE user_id = ? AND store_id = ?", (user_id, store_id))
    removed = cursor.rowcount > 0
    conn.commit()
    conn.close()
    return removed

# -------------------------------
# Sync & Attribute Functions
# -------------------------------
def update_db_attribute(table, localization_id, attribute, value, store_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        f"UPDATE {table} SET {attribute} = ? WHERE localization_id = ? AND store_id = ?",
        (value, localization_id, store_id)
    )
    conn.commit()
    conn.close()

# -------------------------------
# NEW: Sync Attribute Data (Delete old, fetch & insert latest from Apple)
# -------------------------------
def sync_attribute_data(attr, app_id, store_id, issuer_id, key_id, private_key, platform=None):
    print(f"[SYNC ATTR] Syncing '{attr}' for app {app_id}, platform: {platform}")
    success = True

    if attr in ['name', 'subtitle', 'privacy_policy_url', 'privacy_choices_url']:
        # App Info Attributes (no platform)
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM app_info_localizations WHERE app_id = ? AND store_id = ?", (app_id, store_id))
            conn.commit()
            print(f"[SYNC ATTR] Deleted old app_info_localizations for {app_id}")

        app_info_data = fetch_app_info(app_id, issuer_id, key_id, private_key)
        if app_info_data and "data" in app_info_data and app_info_data["data"]:
            app_info_index = 1 if len(app_info_data["data"]) > 1 else 0
            app_info_id = app_info_data["data"][app_info_index].get("id")
            loc_data = fetch_app_info_localizations(app_info_id, issuer_id, key_id, private_key)
            if loc_data and "data" in loc_data:
                with get_db_connection() as conn:
                    cursor = conn.cursor()
                    for loc in loc_data["data"]:
                        cursor.execute(
                            """
                            INSERT OR REPLACE INTO app_info_localizations 
                            (localization_id, app_id, store_id, locale, name, subtitle, privacy_policy_url, privacy_choices_url) 
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            (
                                loc["id"],
                                app_id,
                                store_id,
                                loc["attributes"].get("locale"),
                                loc["attributes"].get("name"),
                                loc["attributes"].get("subtitle"),
                                loc["attributes"].get("privacyPolicyUrl"),
                                loc["attributes"].get("privacyChoicesUrl")
                            )
                        )
                    conn.commit()
                print(f"[SYNC ATTR] Inserted new app_info_localizations for {app_id}")
            else:
                success = False
        else:
            success = False

    elif attr in ['description', 'keywords', 'marketing_url', 'promotional_text', 'support_url', 'whats_new']:
        # Version Attributes (platform-specific)
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM app_version_localizations WHERE app_id = ? AND store_id = ? AND platform = ?", (app_id, store_id, platform))
            cursor.execute("DELETE FROM app_versions WHERE app_id = ? AND store_id = ? AND platform = ?", (app_id, store_id, platform))
            conn.commit()
            print(f"[SYNC ATTR] Deleted old app_versions & localizations for {app_id} ({platform})")

        versions_data = fetch_app_store_versions(app_id, issuer_id, key_id, private_key, platform=platform)
        if versions_data and "data" in versions_data:
            with get_db_connection() as conn:
                cursor = conn.cursor()
                for version in versions_data["data"]:
                    version_id = version["id"]
                    plat = version["attributes"].get("platform")
                    cursor.execute(
                        "INSERT OR REPLACE INTO app_versions (version_id, app_id, store_id, platform) VALUES (?, ?, ?, ?)",
                        (version_id, app_id, store_id, plat)
                    )

                    loc_data = fetch_app_store_version_localizations(version_id, issuer_id, key_id, private_key)
                    if loc_data and "data" in loc_data:
                        for loc in loc_data["data"]:
                            cursor.execute(
                                """
                                INSERT OR REPLACE INTO app_version_localizations 
                                (localization_id, version_id, app_id, store_id, locale, description, keywords, 
                                marketing_url, promotional_text, support_url, whats_new, platform) 
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                """,
                                (
                                    loc["id"],
                                    version_id,
                                    app_id,
                                    store_id,
                                    loc["attributes"].get("locale"),
                                    loc["attributes"].get("description"),
                                    loc["attributes"].get("keywords"),
                                    loc["attributes"].get("marketingUrl"),
                                    loc["attributes"].get("promotionalText"),
                                    loc["attributes"].get("supportUrl"),
                                    loc["attributes"].get("whatsNew"),
                                    plat
                                )
                            )
                        conn.commit()
                    else:
                        success = False
                print(f"[SYNC ATTR] Inserted new app_versions & localizations for {app_id} ({platform})")
        else:
            success = False

    elif attr == 'screenshots':
        # Screenshots (already handles delete in fetch_screenshots)
        fetch_screenshots(app_id, store_id, issuer_id, key_id, private_key, platform=platform)

    return success

def get_attribute_data(attribute, app_id, store_id, platform=None):
    if attribute in ['name', 'subtitle', 'privacy_policy_url', 'privacy_choices_url']:
        table = 'app_info_localizations'
        query = f"SELECT localization_id, locale, {attribute} FROM {table} WHERE app_id = ? AND store_id = ?"
        params = (app_id, store_id)
    else:
        table = 'app_version_localizations'
        query = f"SELECT localization_id, locale, {attribute} FROM {table} WHERE app_id = ? AND store_id = ?"
        params = [app_id, store_id]
        if platform:
            query += " AND platform = ?"
            params.append(platform)
    conn = get_db_connection()
    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    return df, table

def get_locales(app_id, store_id):
    conn = get_db_connection()
    query = """
        SELECT DISTINCT locale FROM app_info_localizations WHERE app_id = ? AND store_id = ?
        UNION
        SELECT DISTINCT locale FROM app_version_localizations WHERE app_id = ? AND store_id = ?
    """
    df = pd.read_sql_query(query, conn, params=(app_id, store_id, app_id, store_id))
    conn.close()
    return df['locale'].tolist()

def call_translation_api_for_origin(user_text, src_lang):
    try:
        if src_lang:
            url = "https://translation-api-772439504210.us-central1.run.app/translate_to_origin"
            payload = {'user_inp': user_text, 'src_lang': src_lang}
            headers = {"X-Api-Key": "E64FUZgN4AGZ8yZr"}
            response = requests.post(url, data=payload, headers=headers, timeout=30)
            response.raise_for_status()
            return response.json().get("translated_text", user_text)
    except Exception as e:
        st.error(f"Translation failed ({src_lang}): {str(e)}")
        print(f"Translation API error: {str(e)}")
        return user_text

def translate_text(text, locale):
    if not text.strip():
        return text

    # ←←← ADD / REPLACE THIS MAPPING ←←←
    locale_map = {
        "RU":      "ru",
        "UK":      "uk",
        "FRFR":    "fr",   # FR-FR → fr
        "FRCA":    "fr",   # FR-CA → fr
        "ESES":    "es",   # ES-ES → es
        "ESMX":    "es",   # ES-MX → es
        "ARSA":    "ar",    # Arabic
        "CA":      "ca",    # Catalan
        "CS":      "cs",    # Czech
        "DA":      "da",    # Danish
        "DEDE":    "de",    # German
        "EL":      "el",    # Greek
        "ENAU":    "en",   
        "ENCA":    "en",
        "ENGB":    "en",
        "ENUS":    "en",
        "FI":      "fi",    # Finnish
        "HE":      "he",    # Hebrew
        "HI":      "hi",    # Hindi
        "HR":      "hr",    # Croatian
        "HU":      "hu",    # Hungarian
        "ID":      "id",    # Indonesian
        "IT":      "it",    # Italian
        "JA":      "ja",    # Japanese
        "KO":      "ko",    # Korean
        "MS":      "ms",    # Malay
        "NLNL":    "nl",    # Dutch
        "NO":      "no",    # Norwegian
        "PL":      "pl",    # Polish
        "PTBR":    "pt",    # Portuguese Brazil
        "PTPT":    "pt",    # Portuguese Portugal (same code)
        "RO":      "ro",    # Romanian
        "SK":      "sk",    # Slovak
        "SV":      "sv",    # Swedish
        "TH":      "th",    # Thai
        "TR":      "tr",    # Turkish
        "VI":      "vi",    # Vietnamese
        "ZHHANS":  "zh-CN",    # Simplified Chinese
        "ZHHANT":  "zh-TW",    # Traditional Chinese
    }
    # ←←← END OF MAPPING ←←←

    target = locale.upper().replace("-", "")        # e.g. "fr-fr" → "FRFR"
    src_lang = locale_map.get(target, target.lower())   # fallback = lowercase code

    return call_translation_api_for_origin(text, src_lang)

def translate_text_with_gemini(text, locale):
    if not gemini_model or not text.strip():
        return text
    try:
        prompt = f"{text}\n\nTranslate to {locale}.\n Only provide the translated text."
        response = gemini_model.generate_content(prompt)
        return response.text.strip()
    except Exception as e:
        st.error(f"Translation failed: {str(e)}")
        return text 
# -------------------------------
# Main Dashboard
# -------------------------------
def main():
    st.set_page_config(page_title="App Metadata Dashboard", page_icon="📊", layout="wide")
    st.title("📱 App Metadata Dashboard")

    if not check_database_exists():
        initialize_database()
    create_default_admin()

    login()

    # --- Logout Button ---
    if st.sidebar.button("Logout", key="logout_main"):
        st.session_state['confirm_logout'] = True
    if st.session_state.get('confirm_logout', False):
        st.sidebar.warning("Are you sure you want to log out?")
        col1, col2 = st.sidebar.columns(2)
        with col1:
            if st.button("Logout", key="confirm_yes_logout"):
                for key in ['authenticated', 'user', 'is_admin', 'selected_attribute']:
                    if key in st.session_state:
                        del st.session_state[key]
                if 'confirm_logout' in st.session_state:
                    del st.session_state['confirm_logout']
                st.success("You have been logged out successfully.")
                st.rerun()
        with col2:
            if st.button("Cancel", key="confirm_no_logout"):
                del st.session_state['confirm_logout']

    st.sidebar.success(f"Logged in as: **{st.session_state.user['username']}**")
    if st.session_state.is_admin:
        st.sidebar.success("You are **ADMIN**")

    # Add Store – ONLY ADMIN
    if st.session_state.is_admin:
        with st.sidebar.expander("➕ Add New Store"):
            name = st.text_input("Store Name")
            issuer_id_input = st.text_input("Issuer ID")
            key_id_input = st.text_input("Key ID")
            private_key_input = st.text_area("Private Key")
            if st.button("Add Store"):
                if name and issuer_id_input and key_id_input and private_key_input:
                    store_id = add_store(name, issuer_id_input, key_id_input, private_key_input)
                    with st.spinner("Fetching..."):
                        success = fetch_and_store_apps(store_id, issuer_id_input, key_id_input, private_key_input)
                        if success:
                            st.success("Store added and data fetched!")
                            st.session_state.selected_store_id = store_id
                            st.session_state.selected_app_id = None
                            st.session_state.last_store_name = name
                        else:
                            st.error("Store added, but data fetch failed.")
                    sync_db_to_github()
                    st.rerun()

    # Admin Panel
    if st.session_state.is_admin:
        with st.sidebar.expander("Admin Panel"):
            st.subheader("Create User")
            new_user = st.text_input("Username", key="admin_new_user")
            new_pass = st.text_input("Password", type="password", key="admin_new_pass")
            if st.button("Create"):
                if new_user and new_pass:
                    conn = get_db_connection()
                    cursor = conn.cursor()
                    try:
                        cursor.execute("INSERT INTO users (username, password, is_admin) VALUES (?, ?, ?)",
                                        (new_user, hash_password(new_pass), 0))
                        conn.commit()
                        st.success(f"User `{new_user}` created!")
                    except sqlite3.IntegrityError:
                        st.error("Username exists!")
                    conn.close()

            st.subheader("Assign Store")
            users_df = pd.read_sql_query("SELECT id, username FROM users WHERE is_admin = 0", get_db_connection())
            stores_df = get_stores()
            if not users_df.empty and not stores_df.empty:
                user_id = st.selectbox("User", users_df['id'], format_func=lambda x: users_df[users_df['id']==x]['username'].iloc[0])
                store_id = st.selectbox("Store", stores_df['store_id'], format_func=lambda x: stores_df[stores_df['store_id']==x]['name'].iloc[0])
                if st.button("Assign"):
                    conn = get_db_connection()
                    cursor = conn.cursor()
                    cursor.execute("INSERT OR IGNORE INTO user_stores (user_id, store_id) VALUES (?, ?)", (user_id, store_id))
                    conn.commit()
                    st.success("Assigned!")
                    conn.close()

            st.subheader("Remove Store Access")
            if not users_df.empty and not stores_df.empty:
                remove_user_id = st.selectbox("User", users_df['id'], format_func=lambda x: users_df[users_df['id']==x]['username'].iloc[0], key="remove_user_select")
                remove_store_id = st.selectbox("Store", stores_df['store_id'], format_func=lambda x: stores_df[stores_df['store_id']==x]['name'].iloc[0], key="remove_store_select")
                if st.button("Remove Access", key="remove_access_btn"):
                    if remove_user_store_access(remove_user_id, remove_store_id):
                        st.success("Store access removed!")
                    else:
                        st.warning("No access found to remove.")
                    st.rerun()

                if st.session_state.get('confirm_remove_access', False):
                    st.warning("Are you sure you want to remove this store access?")
                    col1, col2 = st.columns(2)
                    with col1:
                        if st.button("Confirm", key="confirm_remove_yes"):
                            if remove_user_store_access(remove_user_id, remove_store_id):
                                st.success("Store access removed successfully!")
                            st.rerun()
                    with col2:
                        if st.button("Cancel", key="confirm_remove_no"):
                            del st.session_state['confirm_remove_access']
                            st.rerun()

            st.subheader("Delete User")
            if not users_df.empty:
                delete_user_id = st.selectbox("User to Delete", users_df['id'], format_func=lambda x: users_df[users_df['id']==x]['username'].iloc[0], key="delete_user_select")
                col1, col2 = st.columns(2)
                with col1:
                    if st.button("Delete User", key="delete_user_btn"):
                        st.session_state['confirm_delete_user'] = delete_user_id
                with col2:
                    if st.session_state.get('confirm_delete_user') == delete_user_id:
                        st.warning("Are you sure? This will delete the user and all their store access!")
                        if st.button("Confirm Delete", key="confirm_delete_yes"):
                            if delete_user(user_id):
                                st.success("User deleted successfully!")
                                del st.session_state['confirm_delete_user']
                            else:
                                st.error("Failed to delete user (might be admin).")
                            st.rerun()
                        if st.button("Cancel", key="confirm_delete_no"):
                            del st.session_state['confirm_delete_user']
                            st.rerun()

    # -------------------------------
    # PERSISTENT STORE & APP SELECTION
    # -------------------------------
    if 'selected_store_id' not in st.session_state:
        st.session_state.selected_store_id = None
    if 'selected_app_id' not in st.session_state:
        st.session_state.selected_app_id = None

    stores_df = get_stores()
    if stores_df.empty:
        st.warning("No stores assigned!")
        return

    store_options = {row['name']: row['store_id'] for _, row in stores_df.iterrows()}
    store_names = list(store_options.keys())

    def on_store_change():
        st.session_state.selected_store_id = store_options[st.session_state.store_selectbox]
        st.session_state.selected_app_id = None  # Reset app on store change

    selected_store_name = st.sidebar.selectbox(
        "Select Store",
        store_names,
        index=store_names.index(st.session_state.get('last_store_name', store_names[0])) if st.session_state.get('last_store_name') in store_names else 0,
        key="store_selectbox",
        on_change=on_store_change
    )
    st.session_state.last_store_name = selected_store_name
    selected_store_id = st.session_state.selected_store_id = store_options[selected_store_name]

    # --- ADMIN: DELETE STORE ---
    if st.session_state.is_admin:
        if st.sidebar.button("🗑️", key="delete_current_store"):
            st.session_state['confirm_delete_store'] = selected_store_id
            st.session_state['confirm_delete_name'] = selected_store_name

        if st.session_state.get('confirm_delete_store') == selected_store_id:
            st.sidebar.warning(f"Are you sure you want to delete store '{selected_store_name}'?")
            col1, col2 = st.sidebar.columns(2)
            with col1:
                if st.button("Confirm", key="confirm_yes"):
                    delete_store(selected_store_id)
                    if 'confirm_delete_store' in st.session_state:
                        del st.session_state['confirm_delete_store']
                        del st.session_state['confirm_delete_name']
                        del st.session_state.last_store_name
                        st.session_state.selected_store_id = None
                    st.success(f"Store `{selected_store_name}` deleted!")
                    st.rerun()
            with col2:
                if st.button("Cancel", key="confirm_no"):
                    if 'confirm_delete_store' in st.session_state:
                        del st.session_state['confirm_delete_store']
                        del st.session_state['confirm_delete_name']
                    st.rerun()

    issuer_id, key_id, private_key = get_store_credentials(selected_store_id)

    if st.sidebar.button("🔄 Fetch Data for Store"):
        with st.spinner(f"Fetching {selected_store_name}"):
            success = fetch_and_store_apps(selected_store_id, issuer_id, key_id, private_key)
            if success:
                st.success("Data fetched!")
            else:
                st.error("Fetch failed.")
        sync_db_to_github()
        st.rerun()

    apps_df = get_apps_list(selected_store_id)
    if apps_df.empty:
        st.warning("No apps! Fetch data first.")
        return

    st.sidebar.header("📱 Search Apps")
    app_options = {row['name']: row['app_id'] for _, row in apps_df.iterrows()}
    app_names = list(app_options.keys())

    def on_app_change():
        st.session_state.selected_app_id = app_options[st.session_state.app_selectbox]

    default_app_index = 0
    if st.session_state.selected_app_id and st.session_state.selected_app_id in app_options.values():
        default_name = next((n for n, aid in app_options.items() if aid == st.session_state.selected_app_id), app_names[0])
        default_app_index = app_names.index(default_name)

    selected_app_name = st.sidebar.selectbox(
        "Select App",
        app_names,
        index=default_app_index,
        key="app_selectbox",
        on_change=on_app_change
    )
    selected_app_id = st.session_state.selected_app_id = app_options[selected_app_name]

    # ------------------------------------------------------------------
    # Localization Coverage
    # ------------------------------------------------------------------
    if st.sidebar.button("Check Localization", key="btn_check_loc"):
        st.session_state["show_loc_table"] = True

    if st.session_state.get("show_loc_table"):
        st.markdown("## Localization Coverage")
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT a.name,
                GROUP_CONCAT(DISTINCT COALESCE(ail.locale, avl.locale)) AS locales
            FROM apps a
            LEFT JOIN app_info_localizations ail ON a.app_id = ail.app_id AND a.store_id = ail.store_id
            LEFT JOIN app_version_localizations avl ON a.app_id = avl.app_id AND a.store_id = avl.store_id
            WHERE a.store_id = ?
            GROUP BY a.app_id, a.name
            ORDER BY a.name
            """,
            (selected_store_id,),
        )
        rows = cursor.fetchall()
        conn.close()

        if not rows:
            st.info("No localization data found.")
        else:
            total_apps = len(rows)
            st.markdown(f"**Total Apps:** `{total_apps}`")
            st.markdown("---")
            for idx, (app_name, locale_csv) in enumerate(rows, start=1):
                codes = [c.strip().upper() for c in (locale_csv or "").split(",") if c.strip()]
                langs = [f"`{code}` → {locale_names.get(code, code)}" for code in codes]
                if not langs:
                    langs = ["`EN-US` → English (United States)"]
                langs.sort(key=lambda x: x.split("→")[-1].strip())
                lang_count = len(langs)
                st.markdown(f"**{idx}. {app_name}** — `{lang_count}` language{'s' if lang_count != 1 else ''}")
                st.caption(" | ".join(langs))
                st.markdown("---")
        if st.button("Close"):
            del st.session_state["show_loc_table"]
            st.rerun()
        st.markdown("---")
    # -------------------------------
    # Title + Refresh + Search
    # -------------------------------
    col_title, col_refresh, col_search = st.columns([3, 1, 1])
    with col_title:
        st.markdown(f"### {selected_app_name}")
    with col_refresh:
        if st.button("Refresh"):
            with st.spinner("Refreshing…"):
                success = fetch_and_store_single_app(selected_app_id, selected_store_id, issuer_id, key_id, private_key)
                if success:
                    st.success("Refreshed!")
                else:
                    st.error("Refresh failed.")
            sync_db_to_github()
            st.rerun()
    with col_search:
        if st.button("Search iTunes"):
            st.session_state['show_itunes_search'] = True

    st.caption(f"App ID: `{selected_app_id}`")

    # iTunes Search
    if st.session_state.get('show_itunes_search'):
        with st.expander("iTunes App Search – Copy Metadata", expanded=True):
            col1, col2, col3 = st.columns([2, 1, 1])
            with col1:
                search_term = st.text_input("Keywords", placeholder="e.g., photo editor, calculator")
            with col2:
                country = st.selectbox("Country", ["us", "gb", "in", "ca", "au", "de", "fr", "jp"], index=0)
            with col3:
                entity = st.selectbox("Entity", ["software", "desktopSoftware", "iPadSoftware"])
            if st.button("Search"):
                if search_term.strip():
                    with st.spinner("Searching..."):
                        results = search_itunes_apps(search_term, country, entity)
                        st.session_state['itunes_results'] = results
                        st.session_state['search_performed'] = True
            if st.session_state.get('search_performed'):
                results = st.session_state.get('itunes_results', [])
                for app in results:
                    name = app.get("trackName", "Unknown")
                    bundle = app.get("bundleId", "")
                    track_view_url = app.get("trackViewUrl", "")
                    icon = app.get("artworkUrl100", "")
                    desc = app.get("description", "")[:100] + "..."
                    with st.container():
                        c1, c2, c3 = st.columns([1, 5, 2])
                        with c1:
                            if icon: st.image(icon, width=60)
                        with c2:
                            st.markdown(f"**{name}**")
                            st.markdown(f"[View on App Store]({track_view_url})")
                            st.caption(f"`{bundle}`")
                            st.caption(desc)
                        with c3:
                            if st.button("Use This", key=f"use_{bundle}"):
                                scraped_data = scrape_appstore_page(track_view_url) if track_view_url else None
                                if scraped_data:
                                    st.session_state["source_text_name"] = name
                                    st.session_state["source_text_subtitle"] = scraped_data['subtitle']
                                    st.session_state["source_text_description"] = app.get("description", "")
                                    st.success(f"Scraped and copied")
                                else:
                                    st.session_state["source_text_name"] = name
                                    st.session_state["source_text_description"] = app.get("description", "")
                                    st.warning(f"Scraping failed for {name}, using API data.")
                                if 'selected_attribute' not in st.session_state:
                                    st.session_state['selected_attribute'] = 'name'
                                st.session_state['show_itunes_search'] = False
                                st.rerun()
                    st.markdown("---")

    # -------------------------------
    # Editing Area
    # -------------------------------
    col_left, col_right = st.columns([1, 3])
    
    # Left COL
    with col_left:
        # ========================================
        # 1. APP INFO ATTRIBUTES (No Platform)
        # ========================================
        app_info_attrs = ['name', 'subtitle', 'privacy_policy_url', 'privacy_choices_url']
        emoji_info = {'name': '📛',
                'subtitle': '📝',
                'privacy_policy_url': '🔒',
                'privacy_choices_url': '⚙️'}

        for attr in app_info_attrs:
            col_btn, col_sync = st.columns([3, 1])
            with col_btn:
                if st.button(f"{emoji_info.get(attr, '')} {attr.replace('_', ' ').title()}", key=f"info_{attr}"):
                    st.session_state['selected_attribute'] = attr
            with col_sync:
                if st.button("Sync", key=f"sync_info_{attr}"):
                    with st.spinner(f"Syncing {attr.replace('_', ' ')}..."):
                        success = sync_attribute_data(
                            attr, selected_app_id, selected_store_id,
                            issuer_id, key_id, private_key,
                            platform=None  # No platform
                        )
                        if success:
                            st.success(f"{attr.replace('_', ' ').title()} synced!")
                            sync_db_to_github()
                            st.rerun()

        # ========================================
        # 2. APP VERSION ATTRIBUTES (Platform-Based)
        # ========================================
        version_attrs = ['description', 'keywords', 'marketing_url', 'promotional_text', 'support_url', 'whats_new']
        emoji_version = {
                'description': '📖',
                'keywords': '🔍',
                'marketing_url': '📣',
                'promotional_text': '🎉',
                'support_url': '🛠️',
                'whats_new': '✨'
        }

        for attr in version_attrs:
            col_btn, col_sync = st.columns([3, 1])
            with col_btn:
                if st.button(f"{emoji_version.get(attr, '')} {attr.replace('_', ' ').title()}", key=f"version_{attr}"):
                    st.session_state['selected_attribute'] = attr

            with col_sync:
                platform = st.session_state.get('platform')
                if not platform:
                    st.button("Sync", disabled=True, key=f"sync_version_{attr}_disabled")
                else:
                    platform_name = "iOS" if platform == "IOS" else "macOS"
                    if st.button(f"Sync", key=f"sync_version_{attr}"):
                        with st.spinner(f"Syncing {attr.replace('_', ' ')} for {platform_name}..."):
                            success = sync_attribute_data(
                                attr, selected_app_id, selected_store_id,
                                issuer_id, key_id, private_key,
                                platform=platform
                            )
                            if success:
                                st.success(f"{attr.replace('_', ' ').title()} synced for {platform_name}!")
                                sync_db_to_github()
                                st.rerun()

        # ========================================
        # 3. SCREENSHOTS (Platform-Based)
        # ========================================
        col_btn, col_sync = st.columns([3, 1])
        with col_btn:
            if st.button("🖼️ Screenshots", key="attr_screenshots"):
                st.session_state['selected_attribute'] = 'screenshots'

        with col_sync:
            platform = st.session_state.get('platform')
            if not platform:
                st.button("Sync", disabled=True, key="sync_screenshots_disabled")
            else:
                platform_name = "iOS" if platform == "IOS" else "macOS"
                if st.button(f"Sync", key="sync_screenshots"):
                    with st.spinner(f"Syncing screenshots for {platform_name}..."):
                        success = sync_attribute_data(
                            'screenshots',
                            selected_app_id, selected_store_id,
                            issuer_id, key_id, private_key,
                            platform=platform
                        )
                        if success:
                            st.success(f"Screenshots synced for {platform_name}!")
                            sync_db_to_github()
                            st.rerun()

    # Right COL
    with col_right:
        attr = st.session_state.get('selected_attribute')
        if attr and attr != 'screenshots':
            platform = None
            if attr in ['description', 'keywords', 'marketing_url', 'promotional_text', 'support_url', 'whats_new']:
                platform = st.selectbox("Platform", ["IOS", "MAC_OS"], key="platform_select")
                st.session_state['platform'] = platform
                st.markdown("---")

            data, table = get_attribute_data(attr, selected_app_id, selected_store_id, platform)
            if data.empty:
                st.warning(f"No data found for {attr.capitalize()}.")
            else:
                st.markdown(f"#### Editing {attr.capitalize()} for {platform or 'App Info'}")
                st.markdown("---")
                changes = {}
                locales = data['locale'].tolist()

                source_text = st.text_area(
                    "Source Text (English)", 
                    value=st.session_state.get(f"source_text_{attr}", ""),
                    placeholder="Write your text in English..." if attr not in ['privacy_policy_url', 'privacy_choices_url', 'marketing_url', 'support_url'] else "Enter URL...",
                    height=100,
                    key=f"source_input_{attr}"
                )
                st.session_state[f"source_text_{attr}"] = source_text

                # -------------------------------
                # TRANSLATE ALL (Field-Specific)
                # -------------------------------
                text_attrs = ['name', 'subtitle', 'description', 'keywords', 'promotional_text', 'whats_new']
                if attr in text_attrs:
                    if st.button("Translate All"):
                        if not source_text.strip():
                            st.warning("Please write English text first.")
                        else:
                            with st.spinner("Translating all locales..."):
                                for _, row in data.iterrows():
                                    loc_id = row["localization_id"]
                                    locale = row["locale"]
                                    input_key = f"edit_{loc_id}"

                                    translated = translate_text(source_text, locale)
                                    if attr == "keywords":
                                        translated = translated.replace(", ", ",").replace(" ،", "،").replace(" , ", ",").replace(" ، ", "،")
                                    st.session_state[input_key] = translated

                                    time.sleep(1)
                            st.success("All locales translated successfully!")
                            st.rerun()

                # -------------------------------
                # FILL ALL LOCALES (URL + Keywords)
                # -------------------------------
                url_attrs = ['privacy_policy_url', 'privacy_choices_url', 'marketing_url', 'support_url']
                fillable_attrs = url_attrs + ['keywords']  # Add keywords to fillable attributes

                if attr in fillable_attrs:
                    if st.button("Fill All Locales"):
                        if not source_text.strip():
                            warning_msg = "Please enter a URL first." if attr in url_attrs else "Please enter keywords first."
                            st.warning(warning_msg)
                        else:
                            with st.spinner(f"Filling all locales with {'URL' if attr in url_attrs else 'keywords'}..."):
                                for _, row in data.iterrows():
                                    loc_id = row["localization_id"]
                                    input_key = f"edit_{loc_id}"
                                    
                                    if attr == "keywords":
                                        # Clean up keywords (remove extra spaces around commas)
                                        cleaned_text = source_text.strip()
                                        cleaned_text = re.sub(r',\s*,\s*', ',', cleaned_text)  # Remove double commas
                                        cleaned_text = re.sub(r'\s*,\s*', ',', cleaned_text)  # Normalize spaces around commas
                                        st.session_state[input_key] = cleaned_text
                                    else:
                                        # For URLs, just use the text as-is
                                        st.session_state[input_key] = source_text.strip()

                            action_msg = "URL" if attr in url_attrs else "keywords"
                            st.success(f"All {len(locales)} locales filled with the same {action_msg}!")
                            st.rerun()
                                            
                st.markdown("---")
                # url_attrs = ['privacy_policy_url', 'privacy_choices_url', 'marketing_url', 'support_url']
                # if attr in url_attrs:
                #     if st.button("Fill All Locales"):
                #         if not source_text.strip():
                #             st.warning("Please enter a URL first.")
                #         else:
                #             for _, row in data.iterrows():
                #                 loc_id = row["localization_id"]
                #                 input_key = f"edit_{loc_id}"

                #                 st.session_state[input_key] = source_text.strip()

                #             st.success(f"All {len(locales)} locales filled with the same URL!")
                #             st.rerun()
                            
                # st.markdown("---")

                for _, row in data.iterrows():
                    loc_id = row["localization_id"]
                    locale = row["locale"]
                    current_val = row[attr] or ""
                    val = st.session_state.get(f"auto_{attr}_{locale}", current_val)
                    limit = FIELD_LIMITS.get(attr)
                    is_url = attr.endswith("_url") or attr == "keywords"
                    height = 160 if attr in ["description", "promotional_text", "whats_new"] else 80
                    input_key = f"edit_{loc_id}"

                    full_name = locale_names.get(locale.upper(), locale)   # fallback to code if missing
                    label = f"{locale.upper()} – {full_name}"

                    if is_url:
                        user_text = st.text_input(label, value=val, key=input_key)
                    else:
                        user_text = st.text_area(label, value=val, key=input_key, height=height)

                    if limit and len(user_text) > limit:
                        st.error(f"Warning: Limit: **{limit}** chars | You have: **{len(user_text)}** (+{len(user_text) - limit} extra)")
                    elif limit:
                        st.caption(f"{len(user_text)} / {limit} characters")

                    changes[loc_id] = user_text or None
                    st.markdown("---")

                save_key = f"save_changes_{attr}_{selected_app_id}"
                exceeded = [
                    f"{data[data['localization_id'] == loc_id]['locale'].iloc[0].upper()} ({len(val)} > {FIELD_LIMITS[attr]})"
                    for loc_id, val in changes.items()
                    if val and FIELD_LIMITS.get(attr) and len(val) > FIELD_LIMITS[attr]
                ]

                if exceeded:
                    st.error(f"Cannot save! Fix {len(exceeded)} field(s) exceeding limit:\n" + ", ".join(exceeded))
                    st.button("Save Changes", disabled=True, key=f"{save_key}_disabled")
                else:
                    if st.button("Save Changes", key=save_key):
                        with st.spinner("Saving..."):
                            success = True
                            for loc_id, val in changes.items():
                                func = patch_app_info_localization if 'app_info' in table else patch_app_store_version_localization
                                if not func(loc_id, {attr: val}, issuer_id, key_id, private_key):
                                    success = False
                            if success:
                                st.success("Saved successfully!")

                                # 1. Sync DB with App Store (pull latest)
                                with st.spinner("Syncing latest data from App Store..."):
                                    sync_attribute_data(
                                        attr, selected_app_id, selected_store_id,
                                        issuer_id, key_id, private_key, platform
                                    )

                                # 2. Push DB to GitHub
                                with st.spinner("Pushing to GitHub..."):
                                    sync_db_to_github()

                                # 3. Clear auto-fill
                                for loc in locales:
                                    auto_key = f"auto_{attr}_{loc}"
                                    if auto_key in st.session_state:
                                        del st.session_state[auto_key]
                            else:
                                st.error("Save failed.")
                            st.rerun()

        if attr == 'screenshots':
            platform = st.selectbox("Platform", ["IOS", "MAC_OS"], key="platform_select_screenshots")
            st.session_state['platform'] = platform
            platform_name = "iOS" if platform == "IOS" else "macOS"
            st.markdown("---")

            # --- Tabs: View | Upload ---
            tab_view, tab_upload = st.tabs(["View Screenshots", "Upload / Replace"])

            # =================================================================
            # TAB 1: VIEW (Existing + Refresh)
            # =================================================================
            with tab_view:
                df = load_screenshots(selected_app_id, selected_store_id, platform)
                if df.empty:
                    st.info(f"No screenshots found for {platform_name}.")
                else:
                    for locale, loc_group in df.groupby('locale'):
                        full_name = locale_names.get(locale.upper(), locale)
                        with st.expander(f"{locale.upper()} – {full_name}", expanded=False):
                            for disp_type, disp_group in loc_group.groupby('display_type'):
                                clean_name = disp_type.replace('_', ' ').replace('IPHONE', 'iPhone').replace('IPAD', 'iPad').title()
                                count = len(disp_group)
                                st.markdown(f"**{clean_name}** ({count} screenshot{'' if count == 1 else 's'})")
                                cols = st.columns(4)
                                for idx, row in enumerate(disp_group.itertuples()):
                                    with cols[idx % 4]:
                                        st.image(row.url, use_column_width=True, caption=f"{row.width}×{row.height}")
                                st.markdown("---")

            # =================================================================
            # TAB 2: UPLOAD ALL LOCALES AT ONCE
            # =================================================================
            with tab_upload:
                platform = st.session_state.get('platform')
                if not platform:
                    st.warning("Please select a platform first.")
                    st.stop()

                conn = get_db_connection()
                query = """
                    SELECT DISTINCT locale 
                    FROM app_version_localizations 
                    WHERE app_id = ? AND store_id = ? AND platform = ?
                    ORDER BY locale
                """
                df = pd.read_sql_query(query, conn, params=(selected_app_id, selected_store_id, platform))
                conn.close()
                locales = df['locale'].tolist()
                if not locales:
                    st.warning(f"No version localizations found for { 'iOS' if platform == 'IOS' else 'macOS' }. Please sync version data first.")
                    st.stop()

                # Store selections
                if "screenshot_selections" not in st.session_state:
                    st.session_state.screenshot_selections = {}

                upload_data = []

                for locale in sorted(locales):
                    full_name = locale_names.get(locale.upper(), locale)
                    with st.expander(f"{locale.upper()} – {full_name}", expanded=True):
                        col1, col2 = st.columns([2, 2])
                        with col1:
                            display_type = st.selectbox(
                                "Display Type",
                                options=DISPLAY_TYPES[platform],
                                format_func=lambda x: x.replace('_', ' ').replace('IPHONE', 'iPhone').replace('IPAD', 'iPad').replace('APP_', '').title(),
                                key=f"display_{locale}_{platform}"
                            )
                        with col2:
                            action = st.radio(
                                "Action",
                                ["POST (Add New)", "UPDATE (Replace All)"],
                                horizontal=True,
                                key=f"action_{locale}_{platform}"
                            )

                        uploaded_files = st.file_uploader(
                            f"Upload screenshots for {display_type.replace('_', ' ').title()} ({', '.join([f'{w}×{h}' for w,h in VALID_SIZES[display_type]])})",
                            type=['png', 'jpg', 'jpeg'],
                            accept_multiple_files=True,
                            key=f"uploader_{locale}_{platform}_{display_type}"
                        )

                        valid_files = []
                        if uploaded_files:
                            for file in uploaded_files:
                                try:
                                    img = Image.open(file)
                                    w, h = img.size
                                    if (w, h) not in VALID_SIZES[display_type] and (h, w) not in VALID_SIZES[display_type]:
                                        st.error(f"{file.name}: Wrong size → {w}×{h}")
                                        continue
                                    valid_files.append((file.name, file.getvalue(), img.format.lower()))
                                except Exception:
                                    st.error(f"{file.name}: Corrupted image")

                            if valid_files:
                                st.success(f"{len(valid_files)} valid file(s) ready")

                        # Save selection for final upload
                        if valid_files:
                            upload_data.append({
                                "locale": locale,
                                "display_type": display_type,
                                "action": "UPDATE" if "UPDATE" in action else "POST",
                                "files": valid_files
                            })

                st.markdown("---")
                if st.button("Upload Screenshots"):
                    if not upload_data:
                        st.error("No screenshots selected!")
                    else:
                        with st.spinner(f"Uploading {sum(len(d['files']) for d in upload_data)} screenshots..."):
                            all_success = True
                            for item in upload_data:
                                success = upload_screenshots_dashboard(
                                    issuer_id=issuer_id,
                                    key_id=key_id,
                                    private_key=private_key,
                                    app_id=selected_app_id,
                                    locale=item["locale"],
                                    platform=platform,
                                    display_type=item["display_type"],
                                    action=item["action"],
                                    files=[(f[0], f[1], f[2]) for f in item["files"]]
                                )
                                if not success:
                                    st.error(f"Failed → {item['locale']} – {item['display_type']}")
                                    all_success = False
                                else:
                                    st.success(f"Uploaded → {item['locale']} – {item['display_type']} ({len(item['files'])})")

                            if all_success:
                                st.success("Screenshots uploaded successfully!")
                                fetch_screenshots(selected_app_id, selected_store_id, issuer_id, key_id, private_key, platform=platform)
                                sync_db_to_github()
                                st.rerun()
                            else:
                                st.error("Upload failed.")

    st.markdown("---")
    st.markdown(
        "<p style='text-align: center; color: #666; font-size: 14px; font-weight: 500;'>"
        "Powered by <b>Dzine Media</b> | Created by <b>Qasim Hameed</b></p>",
        unsafe_allow_html=True
    )

if __name__ == "__main__":
    main()
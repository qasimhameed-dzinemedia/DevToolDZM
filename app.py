import time
import streamlit as st
import sqlite3
import pandas as pd
import google.generativeai as genai
import requests
import hashlib
from main import (
    fetch_and_store_apps,
    patch_app_info_localization,
    patch_app_store_version_localization,
    fetch_app_info,
    fetch_app_info_localizations,
    fetch_app_store_versions,
    fetch_app_store_version_localizations,
    fetch_and_store_single_app,
    patch_screenshots,
    fetch_screenshots,
    sync_db_to_github
)

# -------------------------------
# Gemini AI Setup
# -------------------------------
GEMINI_API_KEY = "AIzaSyCoIwS0zRQ0CTl4WY8et_QDTOUrIIuB3iA"
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
        st.markdown("<h2 style='text-align:center;'>App Store Metadata Manager</h2>", unsafe_allow_html=True)
        with st.container():
            col1, col2, col3 = st.columns([1, 2, 1])
            with col2:
                with st.form("login_form", clear_on_submit=True):
                    st.markdown("### Login Required")
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
# Get Stores (Admin: all, User: assigned only)
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
# Update DB Attribute
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
# Sync & Attribute Functions
# -------------------------------
def sync_attribute_data(attribute, app_id, store_id, issuer_id, key_id, private_key, platform=None):
    conn = get_db_connection()
    cursor = conn.cursor()
    success = True

    if attribute == 'screenshots':
        return bool(fetch_screenshots(app_id, store_id, issuer_id, key_id, private_key))
    
    if attribute in ['name', 'subtitle', 'privacy_policy_url', 'privacy_choices_url']:
        app_info_data = fetch_app_info(app_id, issuer_id, key_id, private_key)
        if app_info_data and "data" in app_info_data and app_info_data["data"]:
            app_info_index = 1 if len(app_info_data["data"]) > 1 else 0
            app_info_id = app_info_data["data"][app_info_index].get("id")
            app_info_localizations = fetch_app_info_localizations(app_info_id, issuer_id, key_id, private_key)
            if app_info_localizations and "data" in app_info_localizations:
                for loc in app_info_localizations["data"]:
                    cursor.execute(
                        """
                        INSERT OR REPLACE INTO app_info_localizations 
                        (localization_id, app_id, store_id, locale, name, subtitle, privacy_policy_url, privacy_choices_url) 
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            loc["id"], app_id, store_id, loc["attributes"].get("locale"),
                            loc["attributes"].get("name"), loc["attributes"].get("subtitle"),
                            loc["attributes"].get("privacyPolicyUrl"), loc["attributes"].get("privacyChoicesUrl")
                        )
                    )
                conn.commit()
            else:
                success = False
        else:
            success = False
    else:
        versions_data = fetch_app_store_versions(app_id, issuer_id, key_id, private_key)
        if versions_data and "data" in versions_data:
            for version in versions_data["data"]:
                version_id = version["id"]
                version_platform = version["attributes"].get("platform", "UNKNOWN")
                if platform and version_platform != platform:
                    continue
                version_localizations = fetch_app_store_version_localizations(version_id, issuer_id, key_id, private_key)
                if version_localizations and "data" in version_localizations:
                    for loc in version_localizations["data"]:
                        cursor.execute(
                            """
                            INSERT OR REPLACE INTO app_version_localizations 
                            (localization_id, version_id, app_id, store_id, locale, description, keywords, 
                            marketing_url, promotional_text, support_url, whats_new, platform) 
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            (
                                loc["id"], version_id, app_id, store_id, loc["attributes"].get("locale"),
                                loc["attributes"].get("description"), loc["attributes"].get("keywords"),
                                loc["attributes"].get("marketingUrl"), loc["attributes"].get("promotionalText"),
                                loc["attributes"].get("supportUrl"), loc["attributes"].get("whatsNew"), version_platform
                            )
                        )
                    conn.commit()
                else:
                    success = False
        else:
            success = False
    conn.close()
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

def translate_text(text, locale):
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

    # Logout
    if st.sidebar.button("Logout"):
        for key in ['authenticated', 'user', 'is_admin', 'selected_attribute']:
            if key in st.session_state:
                del st.session_state[key]
        st.rerun()

    st.sidebar.success(f"Logged in as: **{st.session_state.user['username']}**")
    if st.session_state.is_admin:
        st.sidebar.success("You are **ADMIN**")

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

    # Stores
    stores_df = get_stores()

    # Add Store – ONLY ADMIN
    if st.session_state.is_admin:
        with st.sidebar.expander("➕ Add New Store"):
            name = st.text_input("Store Name")
            issuer_id = st.text_input("Issuer ID")
            key_id = st.text_input("Key ID")
            private_key = st.text_area("Private Key")
            if st.button("Add Store"):
                if name and issuer_id and key_id and private_key:
                    store_id = add_store(name, issuer_id, key_id, private_key)
                    with st.spinner("Fetching..."):
                        success = fetch_and_store_apps(store_id, issuer_id, key_id, private_key)
                        if success:
                            st.success("Store added and data fetched!")
                        else:
                            st.error("Store added, but data fetch failed.")
                    sync_db_to_github()
                    st.rerun()
    else:
        st.sidebar.info("Only admins can add stores.")

    if stores_df.empty:
        st.warning("No stores assigned!")
        return

    store_options = {row['name']: row['store_id'] for _, row in stores_df.iterrows()}
    selected_store_name = st.sidebar.selectbox("Select Store", list(store_options.keys()))
    selected_store_id = store_options[selected_store_name]

    # --- ADMIN: DELETE STORE BUTTON (with confirmation) ---
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
        with st.spinner("Fetching..."):
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
    selected_app_name = st.sidebar.selectbox("Select App", list(app_options.keys()))
    selected_app_id = app_options[selected_app_name]

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
                    icon = app.get("artworkUrl100", "")
                    desc = app.get("description", "")[:100] + "..."
                    with st.container():
                        c1, c2, c3 = st.columns([1, 5, 2])
                        with c1:
                            if icon: st.image(icon, width=60)
                        with c2:
                            st.markdown(f"**{name}**")
                            st.caption(f"`{bundle}`")
                            st.caption(desc)
                        with c3:
                            if st.button("Use This", key=f"use_{bundle}"):
                                st.session_state["source_text_name"] = name
                                st.session_state["source_text_description"] = app.get("description", "")
                                st.success(f"Copied from **{name}**!")
                                st.session_state['show_itunes_search'] = False
                                st.rerun()
                    st.markdown("---")

    # Editing Area
    col_left, col_right = st.columns([1, 3])

    with col_left:
        attributes = [
            'name', 'subtitle', 'privacy_policy_url', 'privacy_choices_url',
            'description', 'keywords', 'marketing_url', 'promotional_text', 'support_url', 'whats_new'
        ]
        for attr in attributes:
            emoji = {
                'name': '📛',
                'subtitle': '📝',
                'privacy_policy_url': '🔒',
                'privacy_choices_url': '⚙️',
                'description': '📖',
                'keywords': '🔍',
                'marketing_url': '📣',
                'promotional_text': '🎉',
                'support_url': '🛠️',
                'whats_new': '✨'
            }.get(attr, '')
            col_btn, col_sync = st.columns([3, 1])
            with col_btn:
                if st.button(f"{emoji} {attr.capitalize()}", key=f"attr_{attr}"):
                    st.session_state['selected_attribute'] = attr
            with col_sync:
                if st.button("Sync", key=f"sync_{attr}"):
                    with st.spinner("Syncing..."):
                        platform = st.session_state.get('platform')
                        success = sync_attribute_data(attr, selected_app_id, selected_store_id, issuer_id, key_id, private_key, platform)
                        if success:
                            st.success("Synced!")
                        else:
                            st.error("Sync failed.")
                        st.rerun()

        col_btn, col_sync = st.columns([3, 1])
        with col_btn:
            if st.button("🖼️ Screenshots", key="attr_screenshots"):
                st.session_state['selected_attribute'] = 'screenshots'
        with col_sync:
            if st.button("Sync", key="sync_screenshots"):
                with st.spinner("Syncing..."):
                    success = sync_attribute_data('screenshots', selected_app_id, selected_store_id, issuer_id, key_id, private_key)
                    if success:
                        st.success("Screenshots synced!")
                    else:
                        st.error("Sync failed.")
                    st.rerun()

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
                en_row = data[data['locale'] == 'en-US']
                itunes_value = st.session_state.get(f"source_text_{attr}", "")
                default_value = itunes_value if itunes_value else (en_row.iloc[0][attr] if not en_row.empty else "")
                source_text = st.text_area("Source (en-US)", value=default_value, height=100)
                if st.button("Translate All"):
                    for loc in locales:
                        st.session_state[f"auto_{attr}_{loc}"] = source_text if loc == 'en-US' else translate_text(source_text, loc)
                        time.sleep(4)
                    st.rerun()
                st.markdown("---")

                for _, row in data.iterrows():
                    loc_id = row['localization_id']
                    locale = row['locale']
                    val = st.session_state.get(f"auto_{attr}_{locale}", row[attr] or "")
                    if attr in ['description', 'keywords', 'promotional_text', 'whats_new']:
                        new_val = st.text_area(locale, value=val, key=f"edit_{loc_id}", height=100)
                    else:
                        new_val = st.text_input(locale, value=val, key=f"edit_{loc_id}")
                    changes[loc_id] = new_val or None
                    st.markdown("---")
                if st.button("Save Changes"):
                    success = True
                    for loc_id, val in changes.items():
                        func = patch_app_info_localization if 'app_info' in table else patch_app_store_version_localization
                        if not func(loc_id, {attr: val}, issuer_id, key_id, private_key):
                            success = False
                    if success:
                        st.success("Saved!")
                    else:
                        st.error("Save failed.")
                    sync_db_to_github()
                    st.rerun()

        platform = None
        if attr == 'screenshots':
            platform = st.selectbox("Platform", ["IOS", "MAC_OS"], key="platform_select")
            st.session_state['platform'] = platform
            st.markdown("---")
            df = load_screenshots(selected_app_id, selected_store_id, platform)
            if df.empty:
                st.warning(f"No screenshots found for {platform}.")
            else:
                st.markdown(f"#### Editing Screenshots for {platform}")
                st.markdown("---")
                changes = {}
                for locale, loc_group in df.groupby('locale'):
                    with st.expander(locale):
                        for disp, disp_group in loc_group.groupby('display_type'):
                            st.markdown(f"**{disp}**")
                            cols = st.columns(3)
                            for idx, row in enumerate(disp_group.itertuples()):
                                with cols[idx % 3]:
                                    st.image(row.url, caption=f"{row.width}×{row.height}", use_column_width=True)
                                    # UNIQUE KEY: localization_id + display_type + index
                                    new_url = st.text_input(
                                        "Replace with new URL",
                                        value="",
                                        key=f"shot_{row.localization_id}_{disp}_{idx}",
                                        placeholder="https://example.com/image.jpg"
                                    )
                                    if new_url.strip():
                                        changes[f"{row.localization_id}_{disp}_{idx}"] = {
                                            'localization_id': row.localization_id,
                                            'display_type': disp,
                                            'new_url': new_url.strip()
                                        }

                if st.button("Save Changes", key="save_screenshots"):
                    if not changes:
                        st.warning("No changes to save.")
                    else:
                        with st.spinner("Uploading new screenshots..."):
                            if patch_screenshots(selected_app_id, selected_store_id, changes, issuer_id, key_id, private_key):
                                st.success("Screenshots updated successfully!")
                                sync_db_to_github()
                                st.rerun()
                            else:
                                st.error("Failed to update screenshots. Check console.")

    st.markdown("---")
    st.markdown(
        "<p style='text-align: center; color: #666; font-size: 14px; font-weight: 500;'>"
        "Powered by <b>Dzine Media</b> | Created by <b>Qasim Hameed</b></p>",
        unsafe_allow_html=True
    )

if __name__ == "__main__":
    main()
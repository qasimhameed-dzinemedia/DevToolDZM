import time
import streamlit as st
import sqlite3
import pandas as pd
import os
import google.generativeai as genai
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
        generation_config={"temperature": 0.0}  # No creativity
    )

# -------------------------------
# Database Connection
# -------------------------------
def get_db_connection():
    conn = sqlite3.connect("app_store_data.db", timeout=30)
    return conn

# -------------------------------
# Initialize Database
# -------------------------------
def initialize_database():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS stores (
            store_id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            issuer_id TEXT NOT NULL,
            key_id TEXT NOT NULL,
            private_key TEXT NOT NULL
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS apps (
            app_id TEXT PRIMARY KEY,
            store_id INTEGER,
            name TEXT,
            FOREIGN KEY (store_id) REFERENCES stores (store_id)
        )
    """)
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
    conn.commit()
    conn.close()
    st.success("Database initialized successfully!")

# -------------------------------
# Reset Database
# -------------------------------
def reset_database():
    db_path = "app_store_data.db"
    if os.path.exists(db_path):
        os.remove(db_path)
        st.info("Existing database deleted.")
    initialize_database()
    st.success("New database created successfully!")

# -------------------------------
# Check Database Existence
# -------------------------------
def check_database_exists():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='stores'")
    exists = cursor.fetchone() is not None
    conn.close()
    return exists

# -------------------------------
# NEW: iTunes Search Feature
# -------------------------------

import requests

def search_itunes_apps(term, country, entity):
    if not term.strip():
        return []
    url = "https://itunes.apple.com/search"
    params = {
        "term": term,
        "country": country,
        "entity": entity,
        "limit": 200
    }
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        return data.get("results", [])
    except Exception as e:
        st.error(f"Search failed: {e}")
        return []
    
# -------------------------------
# Load App Data
# -------------------------------
def load_app_data(app_id, store_id):
    conn = get_db_connection()
    query = "SELECT * FROM apps WHERE app_id = ? AND store_id = ?"
    df = pd.read_sql_query(query, conn, params=(app_id, store_id))
    conn.close()
    return df.iloc[0] if not df.empty else None

# -------------------------------
# Load App Info Localizations
# -------------------------------
def load_app_info_localizations(app_id, store_id):
    conn = get_db_connection()
    query = "SELECT * FROM app_info_localizations WHERE app_id = ? AND store_id = ?"
    df = pd.read_sql_query(query, conn, params=(app_id, store_id))
    conn.close()
    return df.drop_duplicates(subset=['localization_id'])

# -------------------------------
# Load Version Localizations
# -------------------------------
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

# -------------------------------
# Load Screenshots
# -------------------------------
def load_screenshots(app_id, store_id, platform=None):
    conn = get_db_connection()
    query = """
        SELECT localization_id, locale, display_type, url, width, height, platform
        FROM app_screenshots 
        WHERE app_id = ? AND store_id = ?
    """
    params = [app_id, store_id]
    if platform:
        query += " AND platform = ?"
        params.append(platform)
    query += " ORDER BY locale, display_type"
    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    return df

# -------------------------------
# Get Apps List
# -------------------------------
def get_apps_list(store_id):
    conn = get_db_connection()
    query = "SELECT app_id, COALESCE(name, 'Unnamed App') AS name FROM apps WHERE store_id = ? ORDER BY name"
    df = pd.read_sql_query(query, conn, params=(store_id,))
    conn.close()
    return df

# -------------------------------
# Get Stores
# -------------------------------
def get_stores():
    if not check_database_exists():
        initialize_database()
    conn = get_db_connection()
    df = pd.read_sql_query("SELECT * FROM stores ORDER BY name", conn)
    conn.close()
    return df

# -------------------------------
# Get Store Credentials
# -------------------------------
def get_store_credentials(store_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT issuer_id, key_id, private_key FROM stores WHERE store_id = ?", (store_id,))
    result = cursor.fetchone()
    conn.close()
    return result if result else (None, None, None)

# -------------------------------
# Add Store
# -------------------------------
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

# -------------------------------
# Delete Store
# -------------------------------
def delete_store(store_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM stores WHERE store_id = ?", (store_id,))
    conn.commit()
    conn.close()
    st.sidebar.success(f"Store ID {store_id} deleted successfully!")

# -------------------------------
# Update Database Attribute
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
# Sync Attribute Data
# -------------------------------
def sync_attribute_data(attribute, app_id, store_id, issuer_id, key_id, private_key, platform=None):
    conn = get_db_connection()
    cursor = conn.cursor()
    success = True

    if attribute == 'screenshots':
        return bool(fetch_screenshots(app_id, store_id, issuer_id, key_id, private_key))
    
    if attribute in ['name', 'subtitle', 'privacy_policy_url', 'privacy_choices_url']:
        # Fetch app info
        app_info_data = fetch_app_info(app_id, issuer_id, key_id, private_key)
        if app_info_data and "data" in app_info_data and app_info_data["data"]:
            app_info_index = 1 if len(app_info_data["data"]) > 1 else 0
            app_info_id = app_info_data["data"][app_info_index].get("id")
            # Fetch app info localizations
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
            else:
                success = False
        else:
            success = False
    else:
        # Fetch app store versions
        versions_data = fetch_app_store_versions(app_id, issuer_id, key_id, private_key)
        if versions_data and "data" in versions_data:
            for version in versions_data["data"]:
                version_id = version["id"]
                version_platform = version["attributes"].get("platform", "UNKNOWN")
                if platform and version_platform != platform:
                    continue
                # Fetch version localizations
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
                                version_platform
                            )
                        )
                    conn.commit()
                else:
                    success = False
        else:
            success = False
    conn.close()
    return success

# -------------------------------
# Get Attribute Data
# -------------------------------
def get_attribute_data(attribute, app_id, store_id, platform=None):
    if attribute in ['name', 'subtitle', 'privacy_policy_url', 'privacy_choices_url']:
        table = 'app_info_localizations'
        type_name = 'appInfoLocalizations'
        query = f"SELECT localization_id, locale, {attribute} FROM {table} WHERE app_id = ? AND store_id = ?"
        params = (app_id, store_id)
    else:
        table = 'app_version_localizations'
        type_name = 'appStoreVersionLocalizations'
        query = f"SELECT localization_id, locale, {attribute} FROM {table} WHERE app_id = ? AND store_id = ?"
        params = [app_id, store_id]
        if platform:
            query += " AND platform = ?"
            params.append(platform)
    conn = get_db_connection()
    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    return df, table, type_name

# -------------------------------
# Get Locales
# -------------------------------
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

# -------------------------------
# Translate with Gemini
# -------------------------------
def translate_text(text, locale):
    if not gemini_model or not text.strip():
        return text
    try:
        prompt = f"{text}\n\nTranslate to {locale}.\n Only provide the translated text without any additional commentary."
        response = gemini_model.generate_content(prompt)
        return response.text.strip()
    except Exception as e:
        error_detail = f"Translation failed for {locale}: {str(e)}"
        st.error(error_detail)
        return text
    
# -------------------------------
# Main Dashboard
# -------------------------------
def main():
    st.set_page_config(page_title="App Metadata Dashboard", page_icon="📊", layout="wide")
    st.title("📱 App Metadata Dashboard")

    if not check_database_exists():
        initialize_database()

    st.sidebar.header("🏬 Stores")
    stores_df = get_stores()

    with st.sidebar.expander("➕ Add New Store"):
        name = st.text_input("Store Name")
        issuer_id = st.text_input("Issuer ID")
        key_id = st.text_input("Key ID")
        private_key = st.text_area("Private Key")
        if st.button("Add Store"):
            if name and issuer_id and key_id and private_key:
                try:
                    store_id = add_store(name, issuer_id, key_id, private_key)
                    if store_id:
                        with st.spinner(f"Fetching data for {name}..."):
                            success = fetch_and_store_apps(store_id, issuer_id, key_id, private_key)
                            if success:
                                st.sidebar.success(f"Store '{name}' added and data fetched successfully!")
                            else:
                                st.sidebar.error(f"Store '{name}' added, but failed to fetch data. Check console for request errors.")
                        sync_db_to_github()
                        st.rerun()
                    else:
                        st.sidebar.error("Failed to add store to database.")
                except Exception as e:
                    st.sidebar.error(f"Failed to add store: {str(e)}")
            else:
                st.sidebar.error("Please fill all fields!")

    if stores_df.empty:
        st.warning("No stores found! Add a store below.")
    else:
        store_options = {row['name']: row['store_id'] for _, row in stores_df.iterrows()}
        selected_store_name = st.sidebar.selectbox("Select Store", options=list(store_options.keys()))
        selected_store_id = store_options[selected_store_name]

        # Delete Store Button
        if st.sidebar.button("🗑️", key=f"delete_store_{selected_store_id}"):
            st.session_state['pending_store_delete'] = selected_store_name

        if st.session_state.get('pending_store_delete') == selected_store_name:
            st.sidebar.warning(f"Are you sure you want to delete store '{selected_store_name}'?")
            col1, col2 = st.sidebar.columns(2)
            with col1:
                if st.button("Confirm", key=f"confirm_delete_{selected_store_id}"):
                    delete_store(selected_store_id)
                    st.rerun()
            with col2:
                if st.button("Cancel", key=f"cancel_delete_{selected_store_id}"):
                    st.session_state['pending_store_delete'] = None
                    st.rerun()

        # Fetch Data Button
        issuer_id, key_id, private_key = get_store_credentials(selected_store_id)
        if issuer_id:
            if st.sidebar.button("🔄 Fetch Data for Store"):
                with st.spinner(f"Fetching data for {selected_store_name}..."):
                    success = fetch_and_store_apps(selected_store_id, issuer_id, key_id, private_key)
                    if success:
                        st.sidebar.success(f"Data fetched successfully for '{selected_store_name}'!")
                    else:
                        st.sidebar.error(f"Failed to fetch data for '{selected_store_name}'. Check console for detailed request errors.")
                    sync_db_to_github()
                    st.rerun()

    if stores_df.empty:
        return

    apps_df = get_apps_list(selected_store_id)
    if apps_df.empty:
        st.warning("No apps found for this store! Try fetching data.")
        return

    st.sidebar.header("📱 Search Apps")
    app_options = {row['name']: row['app_id'] for _, row in apps_df.iterrows()}
    selected_app_name = st.sidebar.selectbox("Select App", options=list(app_options.keys()))
    selected_app_id = app_options[selected_app_name]

    with st.spinner(f"Loading {selected_app_name}..."):
        app_data = load_app_data(selected_app_id, selected_store_id)
        if app_data is None:
            st.error("App data not found in local database!")
            return

        issuer_id, key_id, private_key = get_store_credentials(selected_store_id)
        if not issuer_id:
            st.error("Store credentials not found!")
            return

    # -------------------------------------------------
    # App header + single-app refresh button + iTunes Search
    # -------------------------------------------------
    col_title, col_refresh, col_search = st.columns([3, 1, 1])
    with col_title:
        st.markdown(f"### {selected_app_name}")
    with col_refresh:
        if st.button("Refresh", help="Re-fetch only this app from App Store Connect"):
            with st.spinner(f"Refreshing {selected_app_name}…"):
                success = fetch_and_store_single_app(
                    selected_app_id, selected_store_id,
                    issuer_id, key_id, private_key
                )
                if success:
                    st.success(f"'{selected_app_name}' refreshed!")
                else:
                    st.error(f"Refresh failed. Check console.")
            st.rerun()

    with col_search:
        if st.button("Search iTunes", help="Search apps on App Store to copy metadata"):
            st.session_state['show_itunes_search'] = True

    st.caption(f"App ID: `{selected_app_id}`")

    # -------------------------------
    # iTunes Search Popup
    # -------------------------------
    if st.session_state.get('show_itunes_search'):
        with st.expander("iTunes App Search – Copy Metadata", expanded=True):
            col1, col2, col3 = st.columns([2, 1, 1])
            with col1:
                search_term = st.text_input("Keywords", placeholder="e.g., photo editor, calculator")
            with col2:
                country = st.selectbox("Country", ["us", "gb", "in", "ca", "au", "de", "fr", "jp"], index=0)
            with col3:
                entity = st.selectbox("Entity", ["software", "desktopSoftware", "iPadSoftware"])

            if st.button("Search App Store"):
                if search_term.strip():
                    with st.spinner("Searching App Store..."):
                        results = search_itunes_apps(search_term, country, entity)
                        st.session_state['itunes_results'] = results
                        st.session_state['search_performed'] = True
                else:
                    st.warning("Enter keywords!")

            if st.session_state.get('search_performed'):
                results = st.session_state.get('itunes_results', [])
                if not results:
                    st.info("No apps found. Try different keywords.")
                else:
                    st.write(f"**Found {len(results)} apps**")
                    for app in results:
                        app_name = app.get("trackName", "Unknown")
                        bundle_id = app.get("bundleId", "")
                        icon = app.get("artworkUrl100", "")
                        description = app.get("description", "")

                        with st.container():
                            col_icon, col_info, col_action = st.columns([1, 5, 2])
                            with col_icon:
                                if icon:
                                    st.image(icon, width=60)
                            with col_info:
                                st.markdown(f"**{app_name}**")
                                st.caption(f"`{bundle_id}`")
                                if len(description) > 100:
                                    description = description[:100] + "..."
                                st.caption(description)
                            with col_action:
                                if st.button("Use This", key=f"use_{bundle_id}"):
                                    # Auto-fill fields
                                    st.session_state[f"auto_name_en-US"] = app_name
                                    st.session_state[f"auto_description_en-US"] = app.get("description", "")
                                    
                                    st.success(f"Metadata copied from **{app_name}**!")
                                    st.session_state['show_itunes_search'] = False
                                    st.rerun()
                        st.markdown("---")

    # Create two columns in the main dashboard
    col_left, col_right = st.columns([1, 3])  # Left column narrower, right column wider

    with col_left:
        # Attribute buttons with Sync button
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
                if st.button("🔄", key=f"sync_{attr}", help=f"Sync {attr.capitalize()} from App Store Connect"):
                    with st.spinner(f"Syncing {attr.capitalize()}..."):
                        platform_for_sync = st.session_state.get('platform', None) if attr in ['description', 'keywords', 'marketing_url', 'promotional_text', 'support_url', 'whats_new'] else None
                        success = sync_attribute_data(
                            attr, selected_app_id, selected_store_id, issuer_id, key_id, private_key,
                            platform=platform_for_sync
                        )
                        if success:
                            st.success(f"{attr.capitalize()} synced successfully!")
                        else:
                            st.error(f"Failed to sync {attr.capitalize()}. Check console for detailed request errors (e.g., API response body).")
                        st.rerun()
        # -------------------------------
        # Screenshots Button (Separate)
        # -------------------------------
        col_btn, col_sync = st.columns([3, 1])
        with col_btn:
            if st.button("🖼️ Screenshots", key="attr_screenshots"):
                st.session_state['selected_attribute'] = 'screenshots'
        with col_sync:
            if st.button("Sync", key="sync_screenshots", help="Refresh screenshots from App Store"):
                with st.spinner("Syncing screenshots..."):
                    if sync_attribute_data('screenshots', selected_app_id, selected_store_id, issuer_id, key_id, private_key, platform=st.session_state.get('platform')):
                        st.success("Screenshots synced!")
                    else:
                        st.error("Sync failed.")
                    st.rerun()
    with col_right:
        selected_attribute = st.session_state.get('selected_attribute', None)
        if selected_attribute and selected_attribute != 'screenshots':
            # Existing text attributes logic (unchanged)
            platform = None
            if selected_attribute in ['description', 'keywords', 'marketing_url', 'promotional_text', 'support_url', 'whats_new']:
                platform = st.selectbox("Select Platform", options=["IOS", "MAC_OS"], key="platform_select")
                st.session_state['platform'] = platform
                st.markdown("---")

            attr_data, table, type_name = get_attribute_data(selected_attribute, selected_app_id, selected_store_id, platform)
            
            if attr_data.empty:
                st.warning(f"No data found for {selected_attribute}.")
            else:
                st.markdown(f"#### Editing {selected_attribute.capitalize()} for {platform or 'App Info'}")
                changes = {}
                all_locales = attr_data['locale'].tolist()

                en_row = attr_data[attr_data['locale'] == 'en-US']
                # --- SOURCE TEXT FIELD (FIXED) ---
                en_current = en_row.iloc[0][selected_attribute] if not en_row.empty and pd.notna(en_row.iloc[0][selected_attribute]) else ""
                auto_key = f"auto_{selected_attribute}_en-US"
                default_val = st.session_state.get(auto_key, en_current)

                # Unique key har baar
                import uuid
                source_key = f"source_input_{selected_app_id}_{selected_attribute}_{uuid.uuid4().hex[:8]}"

                col_field, col_btn = st.columns([5, 1])
                with col_field:
                    source_text = st.text_area(
                        "Source Text (en-US)", value=default_val, height=100,
                        key=source_key, label_visibility="collapsed"
                    )
                with col_btn:
                    st.markdown("<br>", unsafe_allow_html=True)
                    if st.button("Translate", key=f"btn_trans_{selected_attribute}_{selected_app_id}"):
                        if not source_text.strip():
                            st.error("Enter source text!")
                        else:
                            with st.spinner("Translating..."):
                                for locale in all_locales:
                                    translated = source_text if locale == 'en-US' else translate_text(source_text, locale)
                                    st.session_state[f"auto_{selected_attribute}_{locale}"] = translated
                                    time.sleep(0.6)
                                st.success("Translated!")
                            st.rerun()

                st.markdown("---")
                changes = {}
                for _, row in attr_data.iterrows():
                    loc_id = row['localization_id']
                    locale = row['locale']
                    db_val = row[selected_attribute] if pd.notna(row[selected_attribute]) else ""
                    fill_key = f"auto_{selected_attribute}_{locale}"
                    display_val = st.session_state.get(fill_key, db_val)

                    st.markdown(f"**{locale}**")
                    if selected_attribute in ['description', 'keywords', 'promotional_text', 'whats_new']:
                        new_val = st.text_area(f"{locale} {selected_attribute}", value=display_val, key=f"edit_{loc_id}", height=100, label_visibility="collapsed")
                    else:
                        new_val = st.text_input(f"{locale} {selected_attribute}", value=display_val, key=f"edit_{loc_id}", label_visibility="collapsed")
                    changes[loc_id] = new_val
                    st.markdown("---")

                if st.button("Save Changes", key=f"save_{selected_attribute}"):
                    with st.spinner("Saving..."):
                        success = True
                        for loc_id, val in changes.items():
                            val = None if val == "" else val
                            func = patch_app_info_localization if table == 'app_info_localizations' else patch_app_store_version_localization
                            if not func(loc_id, {selected_attribute: val}, issuer_id, key_id, private_key):
                                success = False
                            else:
                                update_db_attribute(table, loc_id, selected_attribute, val, selected_store_id)
                        if success:
                            st.success("Saved!")
                            sync_db_to_github()
                            for loc in all_locales:
                                k = f"auto_{selected_attribute}_{loc}"
                                if k in st.session_state: del st.session_state[k]
                            st.rerun()
                        else:
                            st.error("Save failed.")

        # -------------------------------------------------
        # Screenshots: No Translation, Only Image + URL
        # -------------------------------------------------
        elif selected_attribute == 'screenshots':
            st.markdown("#### Editing Screenshots")

            platform = st.selectbox("Select Platform", ["IOS", "MAC_OS"], key="platform_screenshots")
            st.session_state['platform'] = platform
            st.markdown("---")

            df = load_screenshots(selected_app_id, selected_store_id, platform)

            if df.empty:
                st.warning(f"No screenshots found for {platform}.")
            else:
                changes = {}

                for locale, loc_group in df.groupby('locale'):
                    with st.expander(f"{locale}"):
                        for disp, disp_group in loc_group.groupby('display_type'):
                            st.markdown(f"**{disp}**")
                            cols = st.columns(3)
                            for idx, row in enumerate(disp_group.itertuples()):
                                col = cols[idx % 3]
                                shot_id = f"{row.localization_id}_{disp}_{idx}"
                                with col:
                                    st.image(row.url, caption=f"{row.width}×{row.height}", use_column_width=True)
                                    new_url = st.text_input(
                                        "Replace with new image URL",
                                        value="",
                                        key=f"edit_shot_{shot_id}",
                                        placeholder="https://example.com/image.jpg"
                                    )
                                    if new_url.strip():
                                        changes[shot_id] = {
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

if __name__ == "__main__":
    main()
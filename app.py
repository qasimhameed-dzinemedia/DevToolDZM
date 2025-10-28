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
    # App header + single-app refresh button
    # -------------------------------------------------
    col_title, col_btn = st.columns([4, 1])
    with col_title:
        st.markdown(f"### {selected_app_name}")
    with col_btn:
        if st.button("🔄 Refresh", help="Re-fetch **only** this app from App Store Connect"):
            with st.spinner(f"Refreshing {selected_app_name}…"):
                success = fetch_and_store_single_app(
                    selected_app_id, selected_store_id,
                    issuer_id, key_id, private_key
                )
                if success:
                    st.success(f"'{selected_app_name}' refreshed successfully!")
                else:
                    st.error(f"Refresh failed for '{selected_app_name}'. Check console for detailed request errors (e.g., API response).")
            st.rerun()          # force page reload → fresh tables
    st.caption(f"App ID: `{selected_app_id}`")

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

    with col_right:
        selected_attribute = st.session_state.get('selected_attribute', None)
        if selected_attribute:
            # Platform selection for version-specific attributes
            platform = None
            if selected_attribute in ['description', 'keywords', 'marketing_url', 'promotional_text', 'support_url', 'whats_new']:
                platform = st.selectbox("Select Platform", options=["IOS", "MAC_OS"], key="platform_select")
                st.session_state['platform'] = platform
                st.markdown("---")

            # Fetch attribute data
            attr_data, table, type_name = get_attribute_data(selected_attribute, selected_app_id, selected_store_id, platform)
            
            if attr_data.empty:
                st.warning(f"No data found for {selected_attribute} in {platform or 'app info'}.")
            else:
                st.markdown(f"#### ✏️ Editing {selected_attribute.capitalize()} for {platform or 'App Info'}")
                # Dictionary to store changes
                changes = {}
                # === Get only existing locales for this attribute ===
                all_locales = attr_data['locale'].tolist()

                # === Hidden: Get en-US current value (if exists) ===
                en_row = attr_data[attr_data['locale'] == 'en-US']
                en_current = ""
                if not en_row.empty:
                    val = en_row.iloc[0][selected_attribute]
                    en_current = val if pd.notna(val) else ""

                # === SIMPLE: One empty field + Translate button ===
                col_field, col_btn = st.columns([5, 1])
                with col_field:
                    source_text = st.text_area(
                        label="Source Text (en-US)",
                        value=en_current,
                        height=100,
                        key=f"source_{selected_attribute}",
                        placeholder="Enter text here and click Translate",
                        label_visibility="collapsed"
                    )
                with col_btn:
                    st.markdown("<br>", unsafe_allow_html=True)
                    if st.button("Translate", key=f"btn_trans_{selected_attribute}"):
                        if not source_text.strip():
                            st.error("Please enter source text!")
                        elif not gemini_model:
                            st.error("Gemini model is not available.")
                        else:
                            with st.spinner("Translating..."):
                                translation_success = True
                                for locale in all_locales:
                                    if locale == 'en-US':
                                        translated = source_text
                                    else:
                                        translated = translate_text(source_text, locale)
                                        # Small delay between requests
                                        time.sleep(0.6)
                                        if translated == source_text:  # Assuming failure if unchanged
                                            translation_success = False
                                    st.session_state[f"auto_{selected_attribute}_{locale}"] = translated
                                if translation_success:
                                    st.success("All translations completed!")
                                else:
                                    st.error("Some translations failed. See individual locale errors above.")
                            st.rerun()

                st.markdown("---")

                # === Show all existing fields (auto-filled) ===
                                # === Show all existing fields (auto-filled) ===
                changes = {}
                for _, row in attr_data.iterrows():
                    loc_id = row['localization_id']
                    locale = row['locale']
                    db_val = row[selected_attribute] if pd.notna(row[selected_attribute]) else ""

                    # Auto-fill from translation
                    fill_key = f"auto_{selected_attribute}_{locale}"
                    display_val = st.session_state.get(fill_key, db_val)

                    st.markdown(f"**{locale}**")
                    if selected_attribute in ['description', 'keywords', 'promotional_text', 'whats_new']:
                        new_val = st.text_area(
                            label=f"{locale} {selected_attribute}",
                            value=display_val,
                            key=f"edit_{loc_id}",
                            height=100,
                            label_visibility="collapsed"  # Hides label but satisfies accessibility
                        )
                    else:
                        new_val = st.text_input(
                            label=f"{locale} {selected_attribute}",
                            value=display_val,
                            key=f"edit_{loc_id}",
                            label_visibility="collapsed"
                        )
                    
                    changes[loc_id] = new_val
                    st.markdown("---")
                
                # Save Changes Button
                if st.button("💾 Save Changes", key=f"save_{selected_attribute}"):
                    patch_success = True
                    patch_errors = []
                    with st.spinner("Saving changes to App Store Connect..."):
                        for loc_id, new_value in changes.items():
                            new_value = None if new_value == "" else new_value
                            patch_func = patch_app_info_localization if table == 'app_info_localizations' else patch_app_store_version_localization
                            if not patch_func(loc_id, {selected_attribute: new_value}, issuer_id, key_id, private_key):
                                patch_success = False
                                patch_errors.append(f"Locale ID {loc_id}")
                            else:
                                update_db_attribute(table, loc_id, selected_attribute, new_value, selected_store_id)
                    if patch_success:
                        st.success("All changes saved successfully to App Store Connect and local DB!")
                        # Clear auto-fill
                        for loc in all_locales:
                            key = f"auto_{selected_attribute}_{loc}"
                            if key in st.session_state:
                                del st.session_state[key]
                        st.rerun()
                    else:
                        error_msg = f"Failed to save for {len(patch_errors)} locale(s): {', '.join(patch_errors)}. Check console for detailed API response errors."
                        st.error(error_msg)

if __name__ == "__main__":
    main()
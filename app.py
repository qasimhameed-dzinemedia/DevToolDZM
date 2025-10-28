import streamlit as st
import sqlite3
import pandas as pd
import os
from main import (
    fetch_and_store_apps,
    patch_app_info_localization,
    patch_app_store_version_localization,
    fetch_app_info,
    fetch_app_info_localizations,
    fetch_app_store_versions,
    fetch_app_store_version_localizations,
    fetch_and_store_single_app
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
    st.info("Database initialized!")

# -------------------------------
# Reset Database
# -------------------------------
def reset_database():
    db_path = "app_store_data.db"
    if os.path.exists(db_path):
        os.remove(db_path)
        st.info("Existing database deleted.")
    initialize_database()
    st.success("New database created!")

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
    st.sidebar.success(f"Store ID {store_id} deleted!")

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
                                st.sidebar.success(f"Store {name} added and data fetched!")
                            else:
                                st.sidebar.error(f"Store {name} added but failed to fetch data!")
                        st.rerun()
                    else:
                        st.sidebar.error("Failed to add store!")
                except Exception as e:
                    st.sidebar.error(f"Failed to add store: {e}")
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
            st.sidebar.warning(f"Delete store '{selected_store_name}'?")
            col1, col2 = st.sidebar.columns(2)
            with col1:
                if st.button("OK", key=f"confirm_delete_{selected_store_id}"):
                    delete_store(selected_store_id)
                    st.rerun()
            with col2:
                if st.button("Cancel", key=f"cancel_delete_{selected_store_id}"):
                    st.session_state['pending_store_delete'] = None
                    st.rerun()

        # Fetch Data Button
        issuer_id, key_id, private_key = get_store_credentials(selected_store_id)
        if issuer_id:
            if st.sidebar.button("Fetch Data for Store"):
                with st.spinner(f"Fetching data for {selected_store_name}..."):
                    success = fetch_and_store_apps(selected_store_id, issuer_id, key_id, private_key)
                    if success:
                        st.sidebar.success(f"Data fetched for {selected_store_name}!")
                    else:
                        st.sidebar.error(f"Failed to fetch data for {selected_store_name}.")
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
            st.error("App data not found!")
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
        if st.button("Refresh", help="Re-fetch **only** this app from App Store Connect"):
            with st.spinner(f"Refreshing {selected_app_name}…"):
                success = fetch_and_store_single_app(
                    selected_app_id, selected_store_id,
                    issuer_id, key_id, private_key
                )
                if success:
                    st.success(f"{selected_app_name} refreshed!")
                else:
                    st.error("Refresh failed – see console.")
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
                        success = sync_attribute_data(
                            attr, selected_app_id, selected_store_id, issuer_id, key_id, private_key,
                            platform=st.session_state.get('platform', None) if attr in ['description', 'keywords', 'marketing_url', 'promotional_text', 'support_url', 'whats_new'] else None
                        )
                        if success:
                            st.success(f"{attr.capitalize()} synced successfully!")
                        else:
                            st.error(f"Failed to sync {attr.capitalize()}.")
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
                for _, row in attr_data.iterrows():
                    loc_id = row['localization_id']
                    locale = row['locale']
                    value = row[selected_attribute] if row[selected_attribute] is not None else ''
                    st.markdown(f"**Locale: {locale}**")
                    # Use text_area for multi-line attributes, text_input for others
                    if selected_attribute in ['description', 'keywords', 'promotional_text', 'whats_new']:
                        new_value = st.text_area(f"{selected_attribute} ({locale})", value=value, key=f"{selected_attribute}_{loc_id}_{locale}")
                        st.markdown("---")
                    else:
                        new_value = st.text_input(f"{selected_attribute} ({locale})", value=value, key=f"{selected_attribute}_{loc_id}_{locale}")
                        st.markdown("---")
                    changes[loc_id] = new_value

                # Save Changes Button
                if st.button("Save Changes", key=f"save_{selected_attribute}"):
                    success = True
                    for loc_id, new_value in changes.items():
                        new_value = None if new_value == '' else new_value
                        # Update API
                        if table == 'app_info_localizations':
                            if not patch_app_info_localization(loc_id, {selected_attribute: new_value}, issuer_id, key_id, private_key):
                                success = False
                        else:
                            if not patch_app_store_version_localization(loc_id, {selected_attribute: new_value}, issuer_id, key_id, private_key):
                                success = False
                        # Update DB if API call is successful
                        if success:
                            update_db_attribute(table, loc_id, selected_attribute, new_value, selected_store_id)
                    if success:
                        st.success(f"{selected_attribute.capitalize()} updated successfully!")
                        st.rerun()
                    else:
                        st.error(f"Failed to update {selected_attribute} in Apple API.")

if __name__ == "__main__":
    main()

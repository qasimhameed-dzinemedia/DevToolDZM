import sqlite3
import requests
import jwt
import time
import contextlib

import requests
import base64
import os
import streamlit as st

def load_db_from_github():
    """Downloads the latest database file from GitHub repo."""
    token = st.secrets["GITHUB_TOKEN"]
    repo = st.secrets["REPO"]
    db_path = st.secrets["DB_PATH"]
    api_url = f"https://api.github.com/repos/{repo}/contents/{db_path}"

    headers = {"Authorization": f"token {token}"}
    res = requests.get(api_url, headers=headers)

    if res.status_code == 200:
        import base64
        content = base64.b64decode(res.json()["content"])
        with open(db_path, "wb") as f:
            f.write(content)
        print("✅ Loaded latest database from GitHub.")
    else:
        print(f"⚠️ Could not load DB from GitHub: {res.text}")

load_db_from_github()

def sync_db_to_github():
    """Uploads the latest database file to GitHub repo."""
    token = st.secrets["GITHUB_TOKEN"]
    repo = st.secrets["REPO"]
    db_path = st.secrets["DB_PATH"]
    api_url = f"https://api.github.com/repos/{repo}/contents/{db_path}"

    with open(db_path, "rb") as f:
        content = base64.b64encode(f.read()).decode()

    headers = {"Authorization": f"token {token}"}
    get_res = requests.get(api_url, headers=headers)
    sha = get_res.json().get("sha")

    data = {
        "message": "Auto-sync database update",
        "content": content,
        "sha": sha
    }

    res = requests.put(api_url, headers=headers, json=data)
    if res.status_code in [200, 201]:
        print("✅ Database synced to GitHub successfully!")
    else:
        print("❌ Failed to sync DB:", res.text)

# -------------------------------
# Configuration
# -------------------------------
BASE_URL = "https://api.appstoreconnect.apple.com/v1"
REQUEST_DELAY = 0.2  # Delay in seconds between task submissions

# -------------------------------
# Attribute Name Mapping
# -------------------------------
ATTRIBUTE_MAPPING = {
    'privacy_policy_url': 'privacyPolicyUrl',
    'privacy_choices_url': 'privacyChoicesUrl',
    'marketing_url': 'marketingUrl',
    'support_url': 'supportUrl',
    'whats_new': 'whatsNew',
    'name': 'name',
    'subtitle': 'subtitle',
    'description': 'description',
    'keywords': 'keywords',
    'promotional_text': 'promotionalText'
}

# -------------------------------
# Database Connection
# -------------------------------
@contextlib.contextmanager
def get_db_connection():
    conn = None
    try:
        conn = sqlite3.connect("app_store_data.db", timeout=30)
        yield conn
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"Database error: {e}")
        raise
    finally:
        if conn:
            conn.close()

# -------------------------------
# Generate JWT Token
# -------------------------------
def generate_jwt(issuer_id, key_id, private_key):
    print("Generating JWT token...")
    headers = {"alg": "ES256", "kid": key_id, "typ": "JWT"}
    payload = {
        "iss": issuer_id,
        "iat": int(time.time()),
        "exp": int(time.time()) + 20 * 60,
        "aud": "appstoreconnect-v1"
    }
    try:
        token = jwt.encode(payload, private_key, algorithm="ES256", headers=headers)
        print("JWT token generated.")
        return token
    except Exception as e:
        print(f"Error generating JWT: {e}")
        return None

# -------------------------------
# Generic GET Helper
# -------------------------------
def get(url, token):
    print(f"Fetching data from {url}...")
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        print(f"Data fetched from {url}.")
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch data from {url}: {e}")
        print(f"Response: {response.text}")
        return None

# -------------------------------
# Generic PATCH Helper
# -------------------------------
def patch(url, token, payload):
    print(f"Patching data to {url}...")
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    try:
        response = requests.patch(url, json=payload, headers=headers)
        response.raise_for_status()
        print(f"Data patched to {url}.")
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Failed to patch data to {url}: {e}")
        print(f"Response: {response.text}")
        return None

# -------------------------------
# Fetch All Apps
# -------------------------------
def fetch_all_apps(issuer_id, key_id, private_key):
    print("Fetching all apps...")
    token = generate_jwt(issuer_id, key_id, private_key)
    if not token:
        print("Failed to generate JWT for fetching apps.")
        return None
    apps = []
    url = f"{BASE_URL}/apps"
    
    while url:
        data = get(url, token)
        if not data:
            print("Failed to fetch apps.")
            return None
        apps.extend(data.get("data", []))
        url = data.get("links", {}).get("next")
        print(f"Fetched {len(data.get('data', []))} apps, next URL: {url or 'None'}")
        time.sleep(REQUEST_DELAY)
    print(f"Fetched total {len(apps)} apps.")
    return apps

# -------------------------------
# Fetch App Info
# -------------------------------
def fetch_app_info(app_id, issuer_id, key_id, private_key):
    print(f"Fetching app info for app ID {app_id}...")
    token = generate_jwt(issuer_id, key_id, private_key)
    if not token:
        print(f"Failed to generate JWT for app info of app ID {app_id}.")
        return None
    url = f"{BASE_URL}/apps/{app_id}/appInfos"
    data = get(url, token)
    if data:
        print(f"Fetched app info for app ID {app_id}.")
    return data

# -------------------------------
# Fetch App Info Localizations
# -------------------------------
def fetch_app_info_localizations(app_info_id, issuer_id, key_id, private_key):
    print(f"Fetching app info localizations for app info ID {app_info_id}...")
    token = generate_jwt(issuer_id, key_id, private_key)
    if not token:
        print(f"Failed to generate JWT for app info localizations of app info ID {app_info_id}.")
        return None
    url = f"{BASE_URL}/appInfos/{app_info_id}/appInfoLocalizations"
    data = get(url, token)
    if data:
        print(f"Fetched {len(data.get('data', []))} app info localizations for app info ID {app_info_id}.")
    return data

# -------------------------------
# Fetch App Store Versions with Filter
# -------------------------------
def fetch_app_store_versions(app_id, issuer_id, key_id, private_key):
    print(f"Fetching app store versions for app ID {app_id} with PREPARE_FOR_SUBMISSION...")
    token = generate_jwt(issuer_id, key_id, private_key)
    if not token:
        print(f"Failed to generate JWT for app store versions of app ID {app_id}.")
        return None
    url = f"{BASE_URL}/apps/{app_id}/appStoreVersions?filter[appStoreState]=PREPARE_FOR_SUBMISSION"
    data = get(url, token)
    if data:
        print(f"Fetched {len(data.get('data', []))} app store versions for app ID {app_id}.")
    return data

# -------------------------------
# Fetch App Store Version Localizations
# -------------------------------
def fetch_app_store_version_localizations(app_store_version_id, issuer_id, key_id, private_key):
    print(f"Fetching version localizations for version ID {app_store_version_id}...")
    token = generate_jwt(issuer_id, key_id, private_key)
    if not token:
        print(f"Failed to generate JWT for version localizations of version ID {app_store_version_id}.")
        return None
    url = f"{BASE_URL}/appStoreVersions/{app_store_version_id}/appStoreVersionLocalizations"
    data = get(url, token)
    if data:
        print(f"Fetched {len(data.get('data', []))} version localizations for version ID {app_store_version_id}.")
    return data

# -------------------------------
# Patch App Info Localization
# -------------------------------
def patch_app_info_localization(localization_id, attributes, issuer_id, key_id, private_key):
    print(f"Patching app info localization ID {localization_id}...")
    token = generate_jwt(issuer_id, key_id, private_key)
    if not token:
        print(f"Failed to generate JWT for patching app info localization ID {localization_id}.")
        return False
    url = f"{BASE_URL}/appInfoLocalizations/{localization_id}"
    # Convert attribute names to camelCase
    mapped_attributes = {ATTRIBUTE_MAPPING.get(k, k): v for k, v in attributes.items() if v is not None}
    payload = {
        "data": {
            "type": "appInfoLocalizations",
            "id": localization_id,
            "attributes": mapped_attributes
        }
    }
    result = patch(url, token, payload)
    return result is not None

# -------------------------------
# Patch App Store Version Localization
# -------------------------------
def patch_app_store_version_localization(localization_id, attributes, issuer_id, key_id, private_key):
    print(f"Patching app store version localization ID {localization_id}...")
    token = generate_jwt(issuer_id, key_id, private_key)
    if not token:
        print(f"Failed to generate JWT for patching app store version localization ID {localization_id}.")
        return False
    url = f"{BASE_URL}/appStoreVersionLocalizations/{localization_id}"
    # Convert attribute names to camelCase
    mapped_attributes = {ATTRIBUTE_MAPPING.get(k, k): v for k, v in attributes.items() if v is not None}
    payload = {
        "data": {
            "type": "appStoreVersionLocalizations",
            "id": localization_id,
            "attributes": mapped_attributes
        }
    }
    result = patch(url, token, payload)
    return result is not None

# -------------------------------
# Process Single App Data
# -------------------------------
def process_app(app, store_id, issuer_id, key_id, private_key):
    app_id = app.get("id")
    app_name = app.get("attributes", {}).get("name", "Unknown")
    print(f"Starting fetch for app: {app_name} (ID: {app_id})")

    with get_db_connection() as conn:
        cursor = conn.cursor()
        print(f"Saving app {app_name} to database...")
        cursor.execute(
            "INSERT OR REPLACE INTO apps (app_id, store_id, name) VALUES (?, ?, ?)",
            (app_id, store_id, app_name)
        )
        conn.commit()
        print(f"Saved app {app_name} to database.")

    # Fetch app info
    app_info_data = fetch_app_info(app_id, issuer_id, key_id, private_key)

    # Process app info localizations (second appInfo if available, but typically one; assume first for simplicity, adjust if needed)
    if app_info_data and "data" in app_info_data and app_info_data["data"]:
        # Take the second element if exists, else first
        app_info_index = 1 if len(app_info_data["data"]) > 1 else 0
        app_info_id = app_info_data["data"][app_info_index].get("id")
        print(f"Processing app info ID {app_info_id} (index {app_info_index}) for app {app_name}...")
        app_info_localizations = fetch_app_info_localizations(app_info_id, issuer_id, key_id, private_key)
        time.sleep(REQUEST_DELAY)
        if app_info_localizations and "data" in app_info_localizations:
            with get_db_connection() as conn:
                cursor = conn.cursor()
                for loc in app_info_localizations["data"]:
                    locale = loc["attributes"].get("locale")
                    print(f"Saving app info localization for locale {locale}...")
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
                            locale,
                            loc["attributes"].get("name"),
                            loc["attributes"].get("subtitle"),
                            loc["attributes"].get("privacyPolicyUrl"),
                            loc["attributes"].get("privacyChoicesUrl")
                        )
                    )
                    conn.commit()
                    print(f"Saved app info localization for locale {locale}.")
        else:
            print(f"No app info localizations found for app info ID {app_info_id}.")
    else:
        print(f"No app info found for app ID {app_id}.")

    # Fetch app store versions with PREPARE_FOR_SUBMISSION
    versions_data = fetch_app_store_versions(app_id, issuer_id, key_id, private_key)
    if versions_data and "data" in versions_data:
        # Process each version (expected 2: iOS and macOS)
        for version in versions_data["data"]:
            version_id = version["id"]
            platform = version["attributes"].get("platform", "UNKNOWN")
            print(f"Processing {platform} version ID {version_id} for app {app_name}...")
            with get_db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "INSERT OR REPLACE INTO app_versions (version_id, app_id, store_id, platform) VALUES (?, ?, ?, ?)",
                    (version_id, app_id, store_id, platform)
                )
                conn.commit()
                print(f"Saved {platform} version ID {version_id}.")

            # Fetch version localizations
            version_localizations = fetch_app_store_version_localizations(version_id, issuer_id, key_id, private_key)
            time.sleep(REQUEST_DELAY)
            if version_localizations and "data" in version_localizations:
                with get_db_connection() as conn:
                    cursor = conn.cursor()
                    for loc in version_localizations["data"]:
                        loc_id = loc["id"]
                        locale = loc["attributes"].get("locale")
                        print(f"Saving {platform} version localization for locale {locale}...")
                        cursor.execute(
                            """
                            INSERT OR REPLACE INTO app_version_localizations 
                            (localization_id, version_id, app_id, store_id, locale, description, keywords, 
                            marketing_url, promotional_text, support_url, whats_new, platform) 
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            (
                                loc_id,
                                version_id,
                                app_id,
                                store_id,
                                locale,
                                loc["attributes"].get("description"),
                                loc["attributes"].get("keywords"),
                                loc["attributes"].get("marketingUrl"),
                                loc["attributes"].get("promotionalText"),
                                loc["attributes"].get("supportUrl"),
                                loc["attributes"].get("whatsNew"),
                                platform
                            )
                        )
                        conn.commit()
                        print(f"Saved {platform} version localization for locale {locale}.")
            else:
                print(f"No {platform} version localizations found for version ID {version_id}.")
    else:
        print(f"No app store versions with PREPARE_FOR_SUBMISSION found for app ID {app_id}.")
    
    print(f"Completed fetch for app: {app_name} (ID: {app_id})")

    sync_db_to_github()

    return app_id, True

# -------------------------------------------------
# NEW: fetch & store data for ONE app only
# -------------------------------------------------
def fetch_and_store_single_app(app_id, store_id, issuer_id, key_id, private_key):
    """
    Re-run the whole pipeline that `process_app()` does, but for a single app.
    Returns True on success, False otherwise.
    """
    # 1. Get the app record (we need the name for logging)
    with get_db_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT name FROM apps WHERE app_id = ? AND store_id = ?", (app_id, store_id))
        row = cur.fetchone()
        app_name = row[0] if row else "Unknown"

    print(f"[SYNC SINGLE] Starting refresh for app {app_name} (ID: {app_id}) …")

    # 2. Build a dummy “app” dict that matches what `fetch_all_apps()` returns
    dummy_app = {"id": app_id, "attributes": {"name": app_name}}

    # 3. Run the same processing logic that the bulk fetch uses
    try:
        _, success = process_app(dummy_app, store_id, issuer_id, key_id, private_key)
        if success:
            print(f"[SYNC SINGLE] App {app_name} refreshed successfully.")
            
            sync_db_to_github()

        else:
            print(f"[SYNC SINGLE] Failed to refresh app {app_name}.")
        return success
    except Exception as e:
        print(f"[SYNC SINGLE] Exception while refreshing app {app_name}: {e}")
        return False
    
# -------------------------------
# Fetch and Store All Apps
# -------------------------------
def fetch_and_store_apps(store_id, issuer_id, key_id, private_key):
    print(f"Starting data fetch for store_id {store_id}...")
    apps = fetch_all_apps(issuer_id, key_id, private_key)
    if not apps:
        print("No apps found or failed to fetch apps.")
        return False
    
    print(f"Processing {len(apps)} apps...")
    success_count = 0
    for app in apps:
        try:
            app_id, success = process_app(app, store_id, issuer_id, key_id, private_key)
            if success:
                success_count += 1
                print(f"Successfully processed app ID {app_id}.")
            else:
                print(f"Failed to process app ID {app_id}.")
        except Exception as e:
            print(f"Error processing app ID {app.get('id')}: {e}")
    
    print(f"Successfully fetched and stored {success_count}/{len(apps)} apps for store_id {store_id}.")

    sync_db_to_github()

    return success_count > 0

if __name__ == "__main__":
    print("Running main.py in test mode...")
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT store_id, issuer_id, key_id, private_key FROM stores LIMIT 1")
        result = cursor.fetchone()
        if result:
            store_id, issuer_id, key_id, private_key = result
            print(f"Found test store credentials for store_id {store_id}.")
            fetch_and_store_apps(store_id, issuer_id, key_id, private_key)
        else:
            print("No store credentials found in database for testing.")
import sqlite3
import requests
import jwt
import time
import contextlib
from concurrent.futures import ThreadPoolExecutor
import streamlit as st
import base64
import os

# ===============================
# GitHub Database Sync
# ===============================
def load_db_from_github():
    """Download DB from GitHub with corruption check."""
    print("Downloading database from GitHub...")
    token = st.secrets["GITHUB_TOKEN"]
    repo = st.secrets["REPO"]
    db_path = st.secrets["DB_PATH"]
    api_url = f"https://api.github.com/repos/{repo}/contents/{db_path}"
    headers = {"Authorization": f"token {token}"}

    res = requests.get(api_url, headers=headers)
    if res.status_code != 200:
        print(f"Failed to fetch DB from GitHub: {res.status_code}")
        return

    data = res.json()
    download_url = data.get("download_url")
    if download_url:
        file_data = requests.get(download_url)
        content = file_data.content
    else:
        content = base64.b64decode(data["content"])

    # Backup old DB
    backup_path = db_path + ".backup"
    if os.path.exists(db_path):
        import shutil
        shutil.copy2(db_path, backup_path)
        print(f"Backup created: {backup_path}")

    # Write new DB
    with open(db_path, "wb") as f:
        f.write(content)

    # Validate new DB
    try:
        conn = sqlite3.connect(db_path, timeout=30)
        conn.execute("SELECT 1 FROM sqlite_master LIMIT 1")
        conn.close()
        print(f"Database loaded and validated ({len(content)} bytes)")
    except Exception as e:
        print(f"Downloaded DB is corrupted! Reverting to backup...")
        if os.path.exists(backup_path):
            shutil.copy2(backup_path, db_path)
            print("Reverted to backup DB.")
        else:
            print("No backup found. Starting fresh.")
            os.remove(db_path)

def sync_db_to_github():
    """Safely upload DB to GitHub with corruption check."""
    print("Preparing to sync database to GitHub...")
    token = st.secrets["GITHUB_TOKEN"]
    repo = st.secrets["REPO"]
    db_path = st.secrets["DB_PATH"]
    api_url = f"https://api.github.com/repos/{repo}/contents/{db_path}"
    headers = {"Authorization": f"token {token}", "Accept": "application/vnd.github.v3+json"}

    # Step 1: Check if DB is valid
    try:
        conn = sqlite3.connect(db_path, timeout=30)
        conn.execute("SELECT 1 FROM sqlite_master LIMIT 1")
        conn.close()
        print("Database integrity check: PASSED")
    except Exception as e:
        print(f"Database is corrupted or locked: {e}")
        return

    # Step 2: Only sync if file exists and > 1KB
    if not os.path.exists(db_path) or os.path.getsize(db_path) < 1024:
        print("Database file too small or missing. Skipping sync.")
        return

    # Step 3: Read and encode
    try:
        with open(db_path, "rb") as f:
            content = base64.b64encode(f.read()).decode()
    except Exception as e:
        print(f"Failed to read DB file: {e}")
        return

    # Step 4: Get current SHA
    get_res = requests.get(api_url, headers=headers)
    sha = get_res.json().get("sha") if get_res.status_code == 200 else None

    # Step 5: Upload
    data = {
        "message": "Auto-sync: DB update (safe)",
        "content": content,
        "branch": "main"
    }
    if sha:
        data["sha"] = sha

    res = requests.put(api_url, headers=headers, json=data)
    if res.status_code in [200, 201]:
        print("Database synced to GitHub successfully!")
    else:
        print(f"GitHub sync failed: {res.status_code} - {res.text}")

# ===============================
# Database Connection
# ===============================
@contextlib.contextmanager
def get_db_connection():
    """Safely open and close SQLite connection."""
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

# ===============================
# Safe Insert (Only Valid Data)
# ===============================
def safe_insert_or_replace(conn, table, data_dict):
    """Insert data only if all values are valid."""
    if not data_dict or not all(k in data_dict for k in data_dict):
        return False
    cursor = conn.cursor()
    columns = ", ".join(data_dict.keys())
    placeholders = ", ".join(["?"] * len(data_dict))
    sql = f"INSERT OR REPLACE INTO {table} ({columns}) VALUES ({placeholders})"
    cursor.execute(sql, tuple(data_dict.values()))
    conn.commit()
    return True

# ===============================
# API Configuration
# ===============================
BASE_URL = "https://api.appstoreconnect.apple.com/v1"
REQUEST_DELAY = 0.2
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

# ===============================
# JWT & HTTP Helpers
# ===============================
def generate_jwt(issuer_id, key_id, private_key):
    """Generate JWT token for App Store Connect."""
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
        print("JWT token generated successfully.")
        return token
    except Exception as e:
        print(f"Failed to generate JWT: {e}")
        return None

def get(url, token):
    """GET request with error handling."""
    print(f"Fetching data from: {url}")
    headers = {"Authorization": f"Bearer {token}"}
    try:
        r = requests.get(url, headers=headers, timeout=30)
        r.raise_for_status()
        print(f"Data fetched successfully from {url}")
        return r.json()
    except Exception as e:
        print(f"GET request failed: {e}")
        return None

def patch(url, token, payload):
    """PATCH request with error handling."""
    print(f"Patching data to: {url}")
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    try:
        r = requests.patch(url, json=payload, headers=headers, timeout=30)
        r.raise_for_status()
        print(f"Patch successful: {url}")
        return r.json()
    except Exception as e:
        print(f"Patch failed: {e}")
        return None

# ===============================
# Fetch All Apps
# ===============================
def fetch_all_apps(issuer_id, key_id, private_key):
    """Fetch all apps from App Store Connect."""
    print("Starting to fetch all apps...")
    token = generate_jwt(issuer_id, key_id, private_key)
    if not token:
        return None

    apps = []
    url = f"{BASE_URL}/apps"
    while url:
        data = get(url, token)
        if not data or 'data' not in data:
            print("No app data received. Stopping.")
            return None
        apps.extend(data['data'])
        url = data.get("links", {}).get("next")
        print(f"Fetched {len(data['data'])} apps. Next page: {'Yes' if url else 'No'}")
        time.sleep(REQUEST_DELAY)
    print(f"Total apps fetched: {len(apps)}")
    sync_db_to_github()
    return apps

# ===============================
# Fetch App Info + Localizations
# ===============================
def fetch_app_info(app_id, issuer_id, key_id, private_key):
    """Fetch app info (contains localizations)."""
    print(f"Fetching app info for App ID: {app_id}")
    token = generate_jwt(issuer_id, key_id, private_key)
    if not token:
        return None
    url = f"{BASE_URL}/apps/{app_id}/appInfos"
    data = get(url, token)
    if not data or 'data' not in data:
        print(f"No app info found for App ID: {app_id}")
        return None

    # Clear old localizations
    with get_db_connection() as conn:
        conn.execute("DELETE FROM app_info_localizations WHERE app_id = ?", (app_id,))
    print(f"App info fetched for {app_id}. Found {len(data['data'])} entries.")
    sync_db_to_github()
    return data

def fetch_app_info_localizations(app_info_id, issuer_id, key_id, private_key):
    """Fetch localizations for a specific app info."""
    print(f"Fetching app info localizations for Info ID: {app_info_id}")
    token = generate_jwt(issuer_id, key_id, private_key)
    if not token:
        return None
    url = f"{BASE_URL}/appInfos/{app_info_id}/appInfoLocalizations"
    data = get(url, token)
    if not data or 'data' not in data:
        print(f"No localizations found for Info ID: {app_info_id}")
        return None

    with get_db_connection() as conn:
        for loc in data['data']:
            attrs = loc['attributes']
            safe_insert_or_replace(conn, 'app_info_localizations', {
                'localization_id': loc['id'],
                'app_id': app_info_id.split('_')[0] if '_' in app_info_id else app_info_id,
                'store_id': None,
                'locale': attrs.get('locale'),
                'name': attrs.get('name'),
                'subtitle': attrs.get('subtitle'),
                'privacy_policy_url': attrs.get('privacyPolicyUrl'),
                'privacy_choices_url': attrs.get('privacyChoicesUrl')
            })
    print(f"Saved {len(data['data'])} app info localizations for {app_info_id}")
    sync_db_to_github()
    return data

# ===============================
# Fetch App Store Versions
# ===============================
def fetch_app_store_versions(app_id, issuer_id, key_id, private_key):
    """Fetch PREPARE_FOR_SUBMISSION versions."""
    print(f"Fetching App Store versions for App ID: {app_id}")
    token = generate_jwt(issuer_id, key_id, private_key)
    if not token:
        return None
    url = f"{BASE_URL}/apps/{app_id}/appStoreVersions?filter[appStoreState]=PREPARE_FOR_SUBMISSION"
    data = get(url, token)
    if not data or 'data' not in data:
        print(f"No PREPARE_FOR_SUBMISSION versions found for App ID: {app_id}")
        return None

    with get_db_connection() as conn:
        conn.execute("DELETE FROM app_versions WHERE app_id = ?", (app_id,))
        conn.execute("DELETE FROM app_version_localizations WHERE app_id = ?", (app_id,))
        for v in data['data']:
            safe_insert_or_replace(conn, 'app_versions', {
                'version_id': v['id'],
                'app_id': app_id,
                'store_id': None,
                'platform': v['attributes'].get('platform')
            })
    print(f"Saved {len(data['data'])} versions for App ID: {app_id}")
    sync_db_to_github()
    return data

def fetch_app_store_version_localizations(version_id, issuer_id, key_id, private_key):
    """Fetch localizations for a version with 100% safe key access."""
    print(f"Fetching version localizations for Version ID: {version_id}")
    token = generate_jwt(issuer_id, key_id, private_key)
    if not token:
        print("JWT generation failed.")
        return None

    url = f"{BASE_URL}/appStoreVersions/{version_id}/appStoreVersionLocalizations"
    data = get(url, token)
    if not data or 'data' not in data:
        print(f"No localizations found for Version ID: {version_id}")
        return None

    saved_count = 0
    with get_db_connection() as conn:
        for loc in data['data']:
            attrs = loc.get('attributes', {})

            # SAFELY extract app_id
            app_id = None
            try:
                rel = loc.get('relationships', {})
                asv = rel.get('appStoreVersion', {})
                data_part = asv.get('data', {})
                app_id = data_part.get('id') if isinstance(data_part, dict) else None
            except Exception as e:
                print(f"Error extracting app_id: {e}")

            if not app_id:
                print(f"Skipping localization {loc.get('id', 'unknown')} — missing app_id")
                continue

            # Insert only if valid
            success = safe_insert_or_replace(conn, 'app_version_localizations', {
                'localization_id': loc.get('id'),
                'version_id': version_id,
                'app_id': app_id,
                'store_id': None,
                'locale': attrs.get('locale'),
                'description': attrs.get('description'),
                'keywords': attrs.get('keywords'),
                'marketing_url': attrs.get('marketingUrl'),
                'promotional_text': attrs.get('promotionalText'),
                'support_url': attrs.get('supportUrl'),
                'whats_new': attrs.get('whatsNew'),
                'platform': attrs.get('platform')
            })

            if success:
                saved_count += 1

    print(f"Saved {saved_count}/{len(data['data'])} version localizations for Version ID: {version_id}")
    sync_db_to_github()
    return data

# ===============================
# Patch + Auto DB Refresh
# ===============================
def patch_and_refresh(patch_func, loc_id, attrs, app_id, store_id, issuer_id, key_id, private_key, is_app_info=False):
    """Patch and refresh DB only if patch succeeds."""
    print(f"Patching localization ID: {loc_id} | Attribute: {list(attrs.keys())[0]}")
    success = patch_func(loc_id, attrs, issuer_id, key_id, private_key)
    if not success:
        print(f"Patch FAILED for localization ID: {loc_id}. DB not updated.")
        return False

    print(f"Patch SUCCESS. Refreshing DB for App ID: {app_id}")
    if is_app_info:
        info = fetch_app_info(app_id, issuer_id, key_id, private_key)
        if info and info['data']:
            app_info_id = info['data'][1]['id'] if len(info['data']) > 1 else info['data'][0]['id']
            fetch_app_info_localizations(app_info_id, issuer_id, key_id, private_key)
    else:
        versions = fetch_app_store_versions(app_id, issuer_id, key_id, private_key)
        if versions and versions['data']:
            for v in versions['data']:
                fetch_app_store_version_localizations(v['id'], issuer_id, key_id, private_key)
    sync_db_to_github()
    return True

def patch_app_info_localization(loc_id, attrs, issuer_id, key_id, private_key):
    token = generate_jwt(issuer_id, key_id, private_key)
    if not token:
        return False
    url = f"{BASE_URL}/appInfoLocalizations/{loc_id}"
    mapped = {ATTRIBUTE_MAPPING.get(k, k): v for k, v in attrs.items() if v is not None}
    payload = {"data": {"type": "appInfoLocalizations", "id": loc_id, "attributes": mapped}}
    return patch(url, token, payload) is not None

def patch_app_store_version_localization(loc_id, attrs, issuer_id, key_id, private_key):
    token = generate_jwt(issuer_id, key_id, private_key)
    if not token:
        return False
    url = f"{BASE_URL}/appStoreVersionLocalizations/{loc_id}"
    mapped = {ATTRIBUTE_MAPPING.get(k, k): v for k, v in attrs.items() if v is not None}
    payload = {"data": {"type": "appStoreVersionLocalizations", "id": loc_id, "attributes": mapped}}
    return patch(url, token, payload) is not None

# ===============================
# Screenshots (Safe + Full Log)
# ===============================
def fetch_screenshots(app_id, store_id, issuer_id, key_id, private_key):
    """Fetch and save all screenshots."""
    print(f"Fetching screenshots for App ID: {app_id}")
    token = generate_jwt(issuer_id, key_id, private_key)
    if not token:
        print("JWT failed. Cannot fetch screenshots.")
        return []

    versions = fetch_app_store_versions(app_id, issuer_id, key_id, private_key)
    if not versions or 'data' not in versions:
        print("No versions found. Skipping screenshots.")
        return []

    all_shots = []
    with ThreadPoolExecutor() as executor:
        futures = []
        for v in versions['data']:
            v_id = v['id']
            plat = v['attributes'].get('platform')
            locs = fetch_app_store_version_localizations(v_id, issuer_id, key_id, private_key)
            if locs and 'data' in locs:
                for loc in locs['data']:
                    locale = loc['attributes']['locale']
                    sets_url = loc['relationships']['appScreenshotSets']['links']['related']
                    sets = get(sets_url, token)
                    if sets and 'data' in sets:
                        for s in sets['data']:
                            disp = s['attributes']['screenshotDisplayType']
                            shots_url = s['relationships']['appScreenshots']['links']['related']
                            shots = get(shots_url, token)
                            if shots and 'data' in shots:
                                for shot in shots['data']:
                                    asset = shot['attributes']['imageAsset']
                                    url = asset['templateUrl'].format(w=asset['width'], h=asset['height'], f='jpg')
                                    all_shots.append({
                                        'localization_id': loc['id'],
                                        'locale': locale,
                                        'display_type': disp,
                                        'url': url,
                                        'width': asset['width'],
                                        'height': asset['height'],
                                        'platform': plat
                                    })

    with get_db_connection() as conn:
        conn.execute("DELETE FROM app_screenshots WHERE app_id = ? AND store_id = ?", (app_id, store_id))
        for s in all_shots:
            sid = f"{app_id}_{s['localization_id']}_{s['display_type']}"
            safe_insert_or_replace(conn, 'app_screenshots', {
                'id': sid, 'app_id': app_id, 'store_id': store_id,
                'localization_id': s['localization_id'], 'locale': s['locale'],
                'display_type': s['display_type'], 'url': s['url'],
                'width': s['width'], 'height': s['height'], 'platform': s['platform']
            })
    print(f"Saved {len(all_shots)} screenshots for App ID: {app_id}")
    sync_db_to_github()
    return all_shots

def patch_screenshots(app_id, store_id, changes, issuer_id, key_id, private_key):
    """Upload new screenshots and refresh DB."""
    print(f"Uploading {len(changes)} new screenshots for App ID: {app_id}")
    token = generate_jwt(issuer_id, key_id, private_key)
    if not token:
        return False

    success = True
    for key, data in changes.items():
        loc_id = data['localization_id']
        disp = data['display_type']
        new_url = data['new_url']
        print(f"Uploading screenshot: {disp} | Locale ID: {loc_id}")

        sets_url = f"{BASE_URL}/appStoreVersionLocalizations/{loc_id}/appScreenshotSets"
        sets = get(sets_url, token)
        set_id = next((s['id'] for s in sets['data'] if s['attributes']['screenshotDisplayType'] == disp), None)
        if not set_id:
            print(f"Screenshot set not found: {disp}")
            success = False
            continue

        upload_url = f"{BASE_URL}/appScreenshots"
        payload = {
            "data": {
                "type": "appScreenshots",
                "attributes": {"fileName": f"{disp}.jpg", "fileSize": 100000},
                "relationships": {"appScreenshotSet": {"data": {"type": "appScreenshotSets", "id": set_id}}}
            }
        }
        resp = requests.post(upload_url, json=payload, headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"})
        if resp.status_code != 201:
            print(f"Upload init failed: {resp.status_code}")
            success = False
            continue

        op = resp.json()['data']['attributes']['uploadOperations'][0]
        img = requests.get(new_url).content
        up = requests.request(op['method'], op['url'], data=img, headers=op['headers'])
        if up.status_code >= 400:
            print(f"Image upload failed: {up.status_code}")
            success = False

    if success:
        print("All screenshots uploaded. Refreshing DB...")
        fetch_screenshots(app_id, store_id, issuer_id, key_id, private_key)
    else:
        print("Some uploads failed. DB not refreshed.")
    return success

# ===============================
# Process Single App
# ===============================
def process_app(app, store_id, issuer_id, key_id, private_key):
    """Process one app: fetch all data."""
    app_id = app.get("id")
    app_name = app.get("attributes", {}).get("name", "Unknown")
    print(f"Processing App: {app_name} | ID: {app_id}")

    if not app_name or app_name == "Unknown":
        print("App name missing. Skipping.")
        return app_id, False

    with get_db_connection() as conn:
        safe_insert_or_replace(conn, 'apps', {'app_id': app_id, 'store_id': store_id, 'name': app_name})

    info = fetch_app_info(app_id, issuer_id, key_id, private_key)
    if not info or not info['data']:
        print("No app info. Skipping further processing.")
        return app_id, False
    app_info_id = info['data'][1]['id'] if len(info['data']) > 1 else info['data'][0]['id']
    fetch_app_info_localizations(app_info_id, issuer_id, key_id, private_key)

    versions = fetch_app_store_versions(app_id, issuer_id, key_id, private_key)
    if versions and versions['data']:
        for v in versions['data']:
            fetch_app_store_version_localizations(v['id'], issuer_id, key_id, private_key)

    fetch_screenshots(app_id, store_id, issuer_id, key_id, private_key)
    print(f"Completed processing for App: {app_name}")
    sync_db_to_github()
    return app_id, True

# ===============================
# Single App Refresh
# ===============================
def fetch_and_store_single_app(app_id, store_id, issuer_id, key_id, private_key):
    """Refresh data for one app."""
    print(f"Refreshing single app: {app_id}")
    with get_db_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT name FROM apps WHERE app_id = ? AND store_id = ?", (app_id, store_id))
        row = cur.fetchone()
        app_name = row[0] if row else "Unknown"
    dummy = {"id": app_id, "attributes": {"name": app_name}}
    success = process_app(dummy, store_id, issuer_id, key_id, private_key)[1]
    print(f"Single app refresh {'SUCCESS' if success else 'FAILED'}")
    return success

# ===============================
# Fetch All Apps
# ===============================
def fetch_and_store_apps(store_id, issuer_id, key_id, private_key):
    """Fetch and store all apps."""
    print(f"Starting full data fetch for Store ID: {store_id}")
    apps = fetch_all_apps(issuer_id, key_id, private_key)
    if not apps:
        print("No apps to process.")
        return False

    success_count = 0
    for app in apps:
        _, ok = process_app(app, store_id, issuer_id, key_id, private_key)
        if ok:
            success_count += 1
    print(f"Processed {success_count}/{len(apps)} apps successfully.")
    sync_db_to_github()
    return success_count > 0

# ===============================
# Main Test
# ===============================
if __name__ == "__main__":
    print("Running in test mode...")
    with get_db_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT store_id, issuer_id, key_id, private_key FROM stores LIMIT 1")
        result = cur.fetchone()
        if result:
            print(f"Found test store: {result[0]}")
            fetch_and_store_apps(*result)
        else:
            print("No store credentials found in DB.")
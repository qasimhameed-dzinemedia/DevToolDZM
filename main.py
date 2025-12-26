import sqlite3
import requests
import jwt
import time
import contextlib
from concurrent.futures import ThreadPoolExecutor
import streamlit as st
import base64
import os
import traceback

def load_db_from_github():
    """Downloads the latest database file from GitHub repo."""
    token = st.secrets["GITHUB_TOKEN"]
    repo = st.secrets["REPO"]
    db_path = st.secrets["DB_PATH"]
    api_url = f"https://api.github.com/repos/{repo}/contents/{db_path}"

    headers = {"Authorization": f"token {token}"}
    res = requests.get(api_url, headers=headers)

    if res.status_code == 200:
        data = res.json()
        download_url = data.get("download_url")

        if download_url:
            # ✅ Safest way: download raw binary directly
            file_data = requests.get(download_url)
            with open(db_path, "wb") as f:
                f.write(file_data.content)
            print(f"✅ Loaded latest database ({len(file_data.content)} bytes) from GitHub.")
        else:
            # fallback if no download_url provided
            content = base64.b64decode(data["content"])
            with open(db_path, "wb") as f:
                f.write(content)
            print("✅ Loaded DB via Base64 fallback.")
    else:
        print(f"⚠️ Could not load DB from GitHub: {res.text}")

def sync_db_to_github():
    """Uploads or updates the latest database file to GitHub repo."""
    token = st.secrets["GITHUB_TOKEN"]
    repo = st.secrets["REPO"]
    db_path = st.secrets["DB_PATH"]
    api_url = f"https://api.github.com/repos/{repo}/contents/{db_path}"

    headers = {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github.v3+json"
    }

    # 🧠 Safety check: don't push if DB file is empty or missing
    if not os.path.exists(db_path) or os.path.getsize(db_path) < 1000:
        print(f"⚠️ Database file '{db_path}' seems empty or missing — skipping GitHub sync.")
        return

    with open(db_path, "rb") as f:
        content = base64.b64encode(f.read()).decode()

    # Get existing file SHA (needed for update)
    get_res = requests.get(api_url, headers=headers)
    if get_res.status_code == 200:
        sha = get_res.json()["sha"]
        print("🔁 Updating existing DB on GitHub...")
    else:
        sha = None
        print("🆕 Creating new DB file on GitHub...")

    data = {
        "message": "Auto-sync database update",
        "content": content,
        "branch": "main"
    }
    if sha:
        data["sha"] = sha

    res = requests.put(api_url, headers=headers, json=data)
    if res.status_code in [200, 201]:
        print("✅ Database synced to GitHub successfully!")
    else:
        print("❌ Failed to sync DB:", res.text)



# 🔍 Debug check (you can remove later)
print("Repo:", st.secrets["REPO"])
print("DB Path:", st.secrets["DB_PATH"])
print("GitHub Token starts with:", st.secrets["GITHUB_TOKEN"][:8], "...")


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
# Custom Exceptions
# -------------------------------
class AppleAPIError(Exception):
    """Custom exception for Apple API errors."""
    def __init__(self, message, errors=None, status_code=None, traceback_str=None):
        super().__init__(message)
        self.errors = errors or []
        self.status_code = status_code
        self.traceback_str = traceback_str

    def __str__(self):
        base_msg = super().__str__()
        if self.errors:
            error_details = "; ".join([f"{e.get('title')}: {e.get('detail')}" for e in self.errors if e.get('detail')])
            return f"{base_msg} (Details: {error_details})"
        return base_msg

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
        tb = traceback.format_exc()
        print(f"Error generating JWT: {e}\n{tb}")
        return None

# -------------------------------
# Generic GET Helper
# -------------------------------
def get(url, token):
    print(f"Fetching data from {url}...")
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    try:
        response = requests.get(url, headers=headers)
        if response.status_code >= 400:
            tb = traceback.format_exc()
            error_details = []
            try:
                error_details = response.json().get("errors", [])
            except:
                pass
            raise AppleAPIError(f"GET Request failed for {url} with status {response.status_code}", 
                                errors=error_details, status_code=response.status_code, traceback_str=tb)
        
        print(f"Data fetched from {url}.")
        return response.json()
    except requests.exceptions.RequestException as e:
        tb = traceback.format_exc()
        print(f"Failed to fetch data from {url}: {e}\n{tb}")
        raise AppleAPIError(f"HTTP Error: {str(e)}", status_code=getattr(e.response, 'status_code', None), traceback_str=tb)
    except Exception as e:
        tb = traceback.format_exc()
        print(f"Unexpected error fetching data from {url}: {e}\n{tb}")
        raise

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
        if response.status_code >= 400:
            tb = traceback.format_exc()
            error_details = []
            try:
                error_details = response.json().get("errors", [])
            except:
                pass
            raise AppleAPIError(f"PATCH Request failed for {url} with status {response.status_code}", 
                                errors=error_details, status_code=response.status_code, traceback_str=tb)
        
        print(f"Data patched to {url}.")
        return response.json()
    except requests.exceptions.RequestException as e:
        tb = traceback.format_exc()
        print(f"Failed to patch data to {url}: {e}\n{tb}")
        raise AppleAPIError(f"HTTP Error: {str(e)}", status_code=getattr(e.response, 'status_code', None), traceback_str=tb)
    except Exception as e:
        tb = traceback.format_exc()
        print(f"Unexpected error patching data to {url}: {e}\n{tb}")
        raise

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
    sync_db_to_github()
    return apps

# -------------------------------
# Fetch App Info
# -------------------------------
# def fetch_app_info(app_id, issuer_id, key_id, private_key, fields=None):
#     print(f"Fetching app info for app ID {app_id}...")
#     token = generate_jwt(issuer_id, key_id, private_key)
#     if not token:
#         print("JWT generation failed.")
#         return None

#     params = {}
#     if fields:
#         params["fields[appInfos]"] = ",".join(fields)

#     url = f"{BASE_URL}/apps/{app_id}/appInfos"
#     if params:
#         url += "?" + "&".join(f"{k}={v}" for k, v in params.items())

#     print(f"GET: {url}")
#     data = get(url, token)
#     if data:
#         count = len(data.get("data", []))
#         print(f"Fetched {count} app info record(s).")
#     sync_db_to_github()
#     return data
def fetch_app_info(app_id, issuer_id, key_id, private_key, fields=None):
    print(f"Fetching app info for app ID {app_id}...")
    token = generate_jwt(issuer_id, key_id, private_key)
    if not token:
        print("JWT generation failed.")
        return None

    params = {}
    if fields:
        params["fields[appInfos]"] = ",".join(fields)

    url = f"{BASE_URL}/apps/{app_id}/appInfos"
    if params:
        url += "?" + "&".join(f"{k}={v}" for k, v in params.items())

    print(f"GET: {url}")
    raw_data = get(url, token)
    
    if not raw_data or "data" not in raw_data:
        print("No data returned.")
        sync_db_to_github()
        return None

    # ── Sirf PREPARE_FOR_SUBMISSION wala record filter karo ──
    prepare_records = [
        record for record in raw_data["data"]
        if record.get("attributes", {}).get("appStoreState") == "PREPARE_FOR_SUBMISSION"
    ]

    if prepare_records:
        print(f"Found {len(prepare_records)} PREPARE_FOR_SUBMISSION appInfo record(s)")
        filtered_data = {"data": prepare_records}
    else:
        print("No PREPARE_FOR_SUBMISSION appInfo found for this app")
        filtered_data = {"data": []}  # ya warning raise kar sakte ho

    sync_db_to_github()
    return filtered_data
# -------------------------------
# Fetch App Info Localizations
# -------------------------------
def fetch_app_info_localizations(app_info_id, issuer_id, key_id, private_key, fields=None):
    print(f"Fetching app info localizations for app info ID {app_info_id}...")
    token = generate_jwt(issuer_id, key_id, private_key)
    if not token:
        print("JWT generation failed.")
        return None

    params = {}
    if fields:
        params["fields[appInfoLocalizations]"] = ",".join(fields)

    url = f"{BASE_URL}/appInfos/{app_info_id}/appInfoLocalizations"
    if params:
        url += "?" + "&".join(f"{k}={v}" for k, v in params.items())

    print(f"GET: {url}")
    data = get(url, token)
    if data:
        count = len(data.get("data", []))
        print(f"Fetched {count} app info localization(s).")
    sync_db_to_github()
    return data

# -------------------------------
# Fetch App Store Versions with Filter
# -------------------------------
def fetch_app_store_versions(app_id, issuer_id, key_id, private_key, platform=None, fields=None):
    print(f"Fetching app store versions for app ID {app_id}...")
    token = generate_jwt(issuer_id, key_id, private_key)
    if not token:
        print("JWT generation failed.")
        return None

    params = {
        "filter[appStoreState]": "PREPARE_FOR_SUBMISSION"
    }
    if platform:
        params["filter[platform]"] = platform
    if fields:
        params["fields[appStoreVersions]"] = ",".join(fields)

    url = f"{BASE_URL}/apps/{app_id}/appStoreVersions"
    query_string = "&".join(f"{k}={v}" for k, v in params.items())
    full_url = f"{url}?{query_string}"

    print(f"GET: {full_url}")
    data = get(full_url, token)
    if data:
        print(f"Fetched {len(data.get('data', []))} versions.")
    sync_db_to_github()
    return data

# -------------------------------
# Fetch App Store Version Localizations
# -------------------------------
def fetch_app_store_version_localizations(version_id, issuer_id, key_id, private_key, fields=None):
    print(f"Fetching version localizations for version ID {version_id}...")
    token = generate_jwt(issuer_id, key_id, private_key)
    if not token:
        print("JWT generation failed.")
        return None

    params = {}
    if fields:
        params["fields[appStoreVersionLocalizations]"] = ",".join(fields)

    url = f"{BASE_URL}/appStoreVersions/{version_id}/appStoreVersionLocalizations"
    if params:
        url += "?" + "&".join(f"{k}={v}" for k, v in params.items())

    print(f"GET: {url}")
    data = get(url, token)
    if data:
        print(f"Fetched {len(data.get('data', []))} localizations.")
    sync_db_to_github()
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
# Patch App Store Version Localization
# -------------------------------
def patch_app_store_version_localization(localization_id, attributes, issuer_id, key_id, private_key):
    attr = list(attributes.keys())[0] if attributes else "unknown"
    print(f"PATCH App-Info – attribute '{attr}'")
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
# NEW: Fetch Screenshots (Reusable)
# -------------------------------
# -------------------------------
# NEW: Fetch Screenshots (Exact Sync + Platform Filter)
# -------------------------------
def fetch_screenshots(app_id, store_id, issuer_id, key_id, private_key, platform=None):
    """
    Fetches ALL screenshots for an app (iOS/macOS) and EXACTLY mirrors API to DB.
    - Deletes old screenshots (for platform or all)
    - Inserts only latest from API with INSERT OR IGNORE to avoid duplicate ID crash
    """
    print(f"[Screenshots] Starting fetch for app {app_id}, platform: {platform or 'ALL'}")
    token = generate_jwt(issuer_id, key_id, private_key)
    if not token:
        st.error("JWT generation failed for screenshots.")
        return []

    # Step 1: Get versions (filter by platform if needed)
    versions_data = fetch_app_store_versions(
        app_id, issuer_id, key_id, private_key,
        platform=platform,
        fields=['platform', 'appStoreVersionLocalizations']
    )
    if not versions_data or 'data' not in versions_data:
        st.warning(f"No {platform or 'any'} version found in PREPARE_FOR_SUBMISSION.")
        return []

    all_screenshots = []
    seen_ids = set()  # For debugging duplicate IDs from API

    def process_localization(loc, platform_name, token):
        locale = loc['attributes']['locale']
        sets_url = loc['relationships']['appScreenshotSets']['links']['related']
        sets_data = get(sets_url, token)
        if not sets_data or 'data' not in sets_data:
            return []

        locale_shots = []
        for sset in sets_data['data']:
            disp = sset['attributes']['screenshotDisplayType']
            shots_url = sset['relationships']['appScreenshots']['links']['related']
            shots_data = get(shots_url, token)
            if not shots_data or 'data' not in shots_data:
                continue

            for shot in shots_data['data']:
                shot_info = {
                    'id': shot['id'],
                    'app_id': app_id,
                    'store_id': store_id,
                    'localization_id': loc['id'],
                    'locale': locale,
                    'display_type': disp,
                    'url': shot['attributes']['url'],
                    'width': shot['attributes']['imageAsset']['width'],
                    'height': shot['attributes']['imageAsset']['height'],
                    'platform': platform_name
                }
                sid = shot['id']
                if sid in seen_ids:
                    print(f"[WARNING] Duplicate screenshot ID from API: {sid} (locale: {locale}, display: {disp})")
                seen_ids.add(sid)
                locale_shots.append(shot_info)
        return locale_shots

    # Parallel processing for speed
    with ThreadPoolExecutor(max_workers=6) as executor:
        futures = []
        for version in versions_data['data']:
            version_id = version['id']
            platform_name = version['attributes'].get('platform', 'UNKNOWN')
            if platform and platform_name != platform:
                continue
            locs_data = fetch_app_store_version_localizations(
                version_id, issuer_id, key_id, private_key,
                fields=['locale', 'appScreenshotSets']
            )
            if locs_data and 'data' in locs_data:
                for loc in locs_data['data']:
                    futures.append(executor.submit(process_localization, loc, platform_name, token))

        for future in futures:
            try:
                all_screenshots.extend(future.result(timeout=40))
            except Exception as e:
                print(f"[Screenshots] Thread error: {e}")

    print(f"[Screenshots] Total screenshots fetched from API: {len(all_screenshots)}")

    # --- EXACT DB SYNC: DELETE OLD + INSERT NEW (with duplicate protection) ---
    with get_db_connection() as conn:
        cursor = conn.cursor()
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

        # DELETE old data
        if platform:
            cursor.execute(
                "DELETE FROM app_screenshots WHERE app_id = ? AND store_id = ? AND platform = ?",
                (app_id, store_id, platform)
            )
        else:
            cursor.execute(
                "DELETE FROM app_screenshots WHERE app_id = ? AND store_id = ?",
                (app_id, store_id)
            )

        # INSERT with OR IGNORE to prevent crash on duplicate IDs
        inserted_count = 0
        for shot in all_screenshots:
            try:
                cursor.execute("""
                    INSERT OR IGNORE INTO app_screenshots 
                    (id, app_id, store_id, localization_id, locale, display_type, url, width, height, platform)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    shot['id'], shot['app_id'], shot['store_id'], shot['localization_id'],
                    shot['locale'], shot['display_type'], shot['url'], shot['width'], shot['height'], shot['platform']
                ))
                if cursor.rowcount > 0:
                    inserted_count += 1
            except Exception as e:
                print(f"[ERROR] Failed to insert screenshot {shot['id']}: {e}")

        conn.commit()

    # --- UI FEEDBACK ---
    count = len(all_screenshots)
    unique_inserted = inserted_count
    if count > 0:
        msg = f"Fetched {count} screenshot(s) from API, inserted {unique_inserted} unique ({platform or 'all platforms'})"
        if count > unique_inserted:
            msg += f" ({count - unique_inserted} duplicates ignored)"
        st.success(msg)
    else:
        st.info(f"No screenshots found for {platform or 'any platform'}.")

    with st.expander(f"API Response: Screenshots ({platform or 'all'})", expanded=False):
        st.json(all_screenshots)

    sync_db_to_github()
    return all_screenshots

# -------------------------------
# NEW: Upload Screenshots (Dashboard Version) - Replaces patch_screenshots
# -------------------------------
def upload_screenshots_dashboard(issuer_id, key_id, private_key, app_id, locale, platform, display_type, action, files):
    """
    Uploads or replaces screenshots via Apple App Store Connect API
    action: "POST" or "UPDATE"
    files: list of (filename, bytes, format)
    """

    token = generate_jwt(issuer_id, key_id, private_key)
    if not token:
        return False

    BASE = "https://api.appstoreconnect.apple.com/v1"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    try:
        # 1. Get App Store Version (PREPARE_FOR_SUBMISSION)
        versions_resp = requests.get(f"{BASE}/apps/{app_id}/appStoreVersions", headers=headers)
        versions_resp.raise_for_status()
        versions = versions_resp.json()["data"]
        version = next(
            (v for v in versions
             if v["attributes"]["platform"] == platform and v["attributes"]["appStoreState"] == "PREPARE_FOR_SUBMISSION"),
            None
        )
        if not version:
            print(f"[ERROR] No {platform} version in PREPARE_FOR_SUBMISSION")
            return False
        version_id = version["id"]

        # 2. Get Localization
        locs_resp = requests.get(f"{BASE}/appStoreVersions/{version_id}/appStoreVersionLocalizations", headers=headers)
        locs_resp.raise_for_status()
        locs = locs_resp.json()["data"]
        loc = next((l for l in locs if l["attributes"]["locale"].lower() == locale.lower()), None)
        if not loc:
            print(f"[ERROR] Locale {locale} not found")
            return False
        loc_id = loc["id"]

        # 3. Get or Create Screenshot Set
        sets_resp = requests.get(f"{BASE}/appStoreVersionLocalizations/{loc_id}/appScreenshotSets", headers=headers)
        sets_resp.raise_for_status()
        sets = sets_resp.json()["data"]
        sset = next((s for s in sets if s["attributes"]["screenshotDisplayType"] == display_type), None)

        if not sset:
            create_payload = {
                "data": {
                    "type": "appScreenshotSets",
                    "attributes": {"screenshotDisplayType": display_type},
                    "relationships": {
                        "appStoreVersionLocalization": {
                            "data": {"type": "appStoreVersionLocalizations", "id": loc_id}
                        }
                    }
                }
            }
            create_resp = requests.post(f"{BASE}/appScreenshotSets", json=create_payload, headers=headers)
            if create_resp.status_code != 201:
                print(f"[ERROR] Failed to create screenshot set: {create_resp.text}")
                return False
            sset_id = create_resp.json()["data"]["id"]
            print(f"[INFO] Created new screenshot set: {sset_id}")
        else:
            sset_id = sset["id"]
            print(f"[INFO] Using existing screenshot set: {sset_id}")

        # 4. If UPDATE → Delete all existing
        if action == "UPDATE":
            existing_resp = requests.get(f"{BASE}/appScreenshotSets/{sset_id}/appScreenshots", headers=headers)
            existing_resp.raise_for_status()
            existing = existing_resp.json().get("data", [])
            for shot in existing:
                del_resp = requests.delete(f"{BASE}/appScreenshots/{shot['id']}", headers=headers)
                if del_resp.status_code not in [200, 204]:
                    print(f"[WARN] Failed to delete old screenshot {shot['id']}")
            print(f"[INFO] Deleted {len(existing)} existing screenshots.")

        # 5. Upload each file
        uploaded = 0
        for idx, (filename, file_bytes, img_format) in enumerate(files, 1):
            print(f"[UPLOAD] {idx}/{len(files)}: {filename}")

            # Create placeholder
            create_payload = {
                "data": {
                    "type": "appScreenshots",
                    "attributes": {
                        "fileName": filename,
                        "fileSize": len(file_bytes)
                    },
                    "relationships": {
                        "appScreenshotSet": {
                            "data": {"type": "appScreenshotSets", "id": sset_id}
                        }
                    }
                }
            }
            create_resp = requests.post(f"{BASE}/appScreenshots", json=create_payload, headers=headers)
            if create_resp.status_code != 201:
                print(f"[ERROR] Create failed: {create_resp.text}")
                return False

            data = create_resp.json()["data"]
            screenshot_id = data["id"]
            upload_ops = data["attributes"]["uploadOperations"]

            # Upload chunks
            for op in upload_ops:
                start = op["offset"]
                length = op["length"]
                chunk = file_bytes[start:start + length]
                op_headers = {h["name"]: h["value"] for h in op.get("headers", [])}
                up_resp = requests.request(op["method"], op["url"], data=chunk, headers=op_headers, timeout=60)
                if up_resp.status_code >= 400:
                    print(f"[ERROR] Chunk upload failed: {up_resp.status_code}")
                    return False

            # Finalize upload
            finalize_resp = requests.patch(
                f"{BASE}/appScreenshots/{screenshot_id}",
                json={"data": {"type": "appScreenshots", "id": screenshot_id, "attributes": {"uploaded": True}}},
                headers=headers
            )
            if finalize_resp.status_code != 200:
                print(f"[ERROR] Finalize failed: {finalize_resp.text}")
                return False

            uploaded += 1

        print(f"[SUCCESS] Uploaded {uploaded} screenshot(s)")
        return True

    except requests.exceptions.RequestException as e:
        tb = traceback.format_exc()
        print(f"[HTTP ERROR] {e}\n{tb}")
        error_details = []
        if e.response is not None:
            try:
                error_details = e.response.json().get("errors", [])
            except:
                pass
        raise AppleAPIError(f"HTTP Error during screenshot upload: {str(e)}", 
                            errors=error_details, 
                            status_code=getattr(e.response, 'status_code', None), 
                            traceback_str=tb)
    except Exception as e:
        tb = traceback.format_exc()
        print(f"[EXCEPTION] {e}\n{tb}")
        raise AppleAPIError(f"Unexpected error during screenshot upload: {str(e)}", traceback_str=tb)

# -------------------------------
# Process Single App Data (UPDATED: Delete old data first)
# -------------------------------
def process_app(app, store_id, issuer_id, key_id, private_key):
    app_id = app.get("id")
    app_name = app.get("attributes", {}).get("name", "Unknown")
    print(f"Starting fetch for app: {app_name} (ID: {app_id})")

    # === STEP 1: DELETE ALL OLD DATA FOR THIS APP ===
    print(f"[SYNC] Deleting old data for app {app_id}...")
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM app_screenshots WHERE app_id = ? AND store_id = ?", (app_id, store_id))
        cursor.execute("DELETE FROM app_version_localizations WHERE app_id = ? AND store_id = ?", (app_id, store_id))
        cursor.execute("DELETE FROM app_versions WHERE app_id = ? AND store_id = ?", (app_id, store_id))
        cursor.execute("DELETE FROM app_info_localizations WHERE app_id = ? AND store_id = ?", (app_id, store_id))
        conn.commit()
    print(f"[SYNC] Old data deleted for app {app_id}.")

    # === STEP 2: Save app name (always fresh) ===
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "INSERT OR REPLACE INTO apps (app_id, store_id, name) VALUES (?, ?, ?)",
            (app_id, store_id, app_name)
        )
        conn.commit()

    # === STEP 3: Fetch & Insert App Info (Delete + Insert) ===
    app_info_data = fetch_app_info(app_id, issuer_id, key_id, private_key)
    if app_info_data and "data" in app_info_data and app_info_data["data"]:
        app_info_index = 1 if len(app_info_data["data"]) > 1 else 0
        app_info_id = app_info_data["data"][app_info_index].get("id")
        app_info_localizations = fetch_app_info_localizations(app_info_id, issuer_id, key_id, private_key)
        time.sleep(REQUEST_DELAY)

        if app_info_localizations and "data" in app_info_localizations:
            with get_db_connection() as conn:
                cursor = conn.cursor()
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

    # === STEP 4: Fetch & Insert Versions + Localizations (Delete + Insert) ===
    versions_data = fetch_app_store_versions(app_id, issuer_id, key_id, private_key)
    if versions_data and "data" in versions_data:
        for version in versions_data["data"]:
            version_id = version["id"]
            platform = version["attributes"].get("platform", "UNKNOWN")

            # Insert version
            with get_db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "INSERT OR REPLACE INTO app_versions (version_id, app_id, store_id, platform) VALUES (?, ?, ?, ?)",
                    (version_id, app_id, store_id, platform)
                )
                conn.commit()

            # Fetch and insert localizations
            version_localizations = fetch_app_store_version_localizations(version_id, issuer_id, key_id, private_key)
            time.sleep(REQUEST_DELAY)
            if version_localizations and "data" in version_localizations:
                with get_db_connection() as conn:
                    cursor = conn.cursor()
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
                                loc["attributes"].get("supportUrl"), loc["attributes"].get("whatsNew"), platform
                            )
                        )
                    conn.commit()

    # === STEP 5: Fetch Screenshots (already deletes old data) ===
    fetch_screenshots(app_id, store_id, issuer_id, key_id, private_key)

    print(f"Completed fresh sync for app: {app_name} (ID: {app_id})")
    sync_db_to_github()
    return app_id, True

# -------------------------------------------------
# NEW: fetch & store data for ONE app only (DELETE OLD FIRST)
# -------------------------------------------------
def fetch_and_store_single_app(app_id, store_id, issuer_id, key_id, private_key):
    with get_db_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT name FROM apps WHERE app_id = ? AND store_id = ?", (app_id, store_id))
        row = cur.fetchone()
        app_name = row[0] if row else "Unknown"

    print(f"[SYNC SINGLE] Starting FULL refresh for app {app_name} (ID: {app_id}) …")

    # Build dummy app dict
    dummy_app = {"id": app_id, "attributes": {"name": app_name}}

    # Run full delete + fresh insert
    try:
        _, success = process_app(dummy_app, store_id, issuer_id, key_id, private_key)
        if success:
            print(f"[SYNC SINGLE] App {app_name} refreshed 100% fresh.")
            sync_db_to_github()
        else:
            print(f"[SYNC SINGLE] Failed.")
        return success
    except Exception as e:
        print(f"[SYNC SINGLE] Error: {e}")
        return False
    
# -------------------------------
# Fetch and Store All Apps (DELETE OLD + CLEANUP REMOVED APPS)
# -------------------------------
def fetch_and_store_apps(store_id, issuer_id, key_id, private_key):
    print(f"Starting FULL data sync for store_id {store_id}...")
    apps = fetch_all_apps(issuer_id, key_id, private_key)
    if not apps:
        print("No apps found.")
        return False

    current_app_ids = [app.get("id") for app in apps]
    success_count = 0

    for app in apps:
        try:
            app_id, success = process_app(app, store_id, issuer_id, key_id, private_key)
            if success:
                success_count += 1
        except Exception as e:
            print(f"Error processing app {app.get('id')}: {e}")

    # === CLEANUP: Remove data of apps no longer in Apple Store ===
    if current_app_ids:
        print(f"[CLEANUP] Removing data for deleted apps in store {store_id}...")
        with get_db_connection() as conn:
            cursor = conn.cursor()
            placeholders = ','.join('?' for _ in current_app_ids)
            for table in ['app_screenshots', 'app_version_localizations', 'app_versions', 'app_info_localizations', 'apps']:
                query = f"DELETE FROM {table} WHERE store_id = ? AND app_id NOT IN ({placeholders})"
                cursor.execute(query, (store_id, *current_app_ids))
            conn.commit()
        print(f"[CLEANUP] Done. Only current apps remain.")

    print(f"Successfully synced {success_count}/{len(apps)} apps.")
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
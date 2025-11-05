import sqlite3
import requests
import jwt
import time
import contextlib
from concurrent.futures import ThreadPoolExecutor
import streamlit as st
import base64
import os

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
    sync_db_to_github()
    return apps

# -------------------------------
# Fetch App Info
# -------------------------------
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
    data = get(url, token)
    if data:
        count = len(data.get("data", []))
        print(f"Fetched {count} app info record(s).")
    sync_db_to_github()
    return data

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
    - Inserts only latest from API
    - Shows API response in popup
    """
    print(f"[Screenshots] Starting fetch for app {app_id}, platform: {platform}")
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

    def process_localization(loc, platform, token):
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
                asset = shot['attributes'].get('imageAsset')
                if not asset or not isinstance(asset, dict):
                    continue

                template = asset.get('templateUrl')
                width = asset.get('width')
                height = asset.get('height')
                if not template or not width or not height:
                    continue

                try:
                    url = template.format(w=width, h=height, f='jpg')
                except (KeyError, ValueError):
                    continue

                apple_shot_id = shot['id']
                shot_id = f"{app_id}_{apple_shot_id}"

                locale_shots.append({
                    'id': shot_id,
                    'app_id': app_id,
                    'store_id': store_id,
                    'localization_id': loc['id'],
                    'locale': locale,
                    'display_type': disp,
                    'url': url,
                    'width': width,
                    'height': height,
                    'platform': platform
                })
        return locale_shots

    # Parallel fetch
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

    # --- EXACT DB SYNC: DELETE OLD + INSERT NEW ---
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

        # DELETE old data (platform-specific or all)
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

        # INSERT only new
        for shot in all_screenshots:
            cursor.execute("""
                INSERT INTO app_screenshots 
                (id, app_id, store_id, localization_id, locale, display_type, url, width, height, platform)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                shot['id'], shot['app_id'], shot['store_id'], shot['localization_id'],
                shot['locale'], shot['display_type'], shot['url'], shot['width'], shot['height'], shot['platform']
            ))
        conn.commit()

    # --- UI FEEDBACK ---
    count = len(all_screenshots)
    if count > 0:
        st.success(f"Fetched {count} screenshot{'' if count == 1 else 's'} ({platform or 'all platforms'})")
    else:
        st.info(f"No screenshots found for {platform or 'any platform'}.")

    with st.expander(f"API Response: Screenshots ({platform or 'all'})", expanded=False):
        st.json(all_screenshots, expanded=False)

    sync_db_to_github()
    return all_screenshots

# -------------------------------
# NEW: Patch Screenshots (Reusable)
# -------------------------------
def patch_screenshots(app_id, store_id, changes, issuer_id, key_id, private_key):
    token = generate_jwt(issuer_id, key_id, private_key)
    if not token:
        st.error("Could not generate JWT for screenshot upload.")
        return False

    overall_success = True

    for shot_key, data in changes.items():
        loc_id      = data['localization_id']
        disp_type   = data['display_type']
        new_url     = data['new_url'].strip()

        # -----------------------------------------------------------------
        # 1. Find the correct screenshot set for the locale + display type
        # -----------------------------------------------------------------
        sets_url = f"{BASE_URL}/appStoreVersionLocalizations/{loc_id}/appScreenshotSets"
        sets_resp = get(sets_url, token)
        if not sets_resp or 'data' not in sets_resp:
            st.error(f"Could not fetch screenshot sets for locale {loc_id}")
            overall_success = False
            continue

        set_id = None
        for s in sets_resp['data']:
            if s['attributes']['screenshotDisplayType'] == disp_type:
                set_id = s['id']
                break
        if not set_id:
            st.error(f"Screenshot set not found for display type `{disp_type}`")
            overall_success = False
            continue

        # -----------------------------------------------------------------
        # 2. Download the new image (once per screenshot)
        # -----------------------------------------------------------------
        try:
            img_resp = requests.get(new_url, timeout=30)
            img_resp.raise_for_status()
            img_bytes = img_resp.content
        except Exception as e:
            st.error(f"Failed to download image from `{new_url}`: {e}")
            overall_success = False
            continue

        # -----------------------------------------------------------------
        # 3. Create a new screenshot placeholder (returns uploadOperations)
        # -----------------------------------------------------------------
        create_url = f"{BASE_URL}/appScreenshots"
        create_payload = {
            "data": {
                "type": "appScreenshots",
                "attributes": {
                    "fileName": f"{disp_type}.jpg",
                    "fileSize": len(img_bytes)
                },
                "relationships": {
                    "appScreenshotSet": {
                        "data": {"type": "appScreenshotSets", "id": set_id}
                    }
                }
            }
        }
        create_resp = requests.post(
            create_url,
            json=create_payload,
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        )
        if create_resp.status_code != 201:
            st.error(f"Failed to create screenshot placeholder: {create_resp.text}")
            overall_success = False
            continue

        screenshot_data = create_resp.json()['data']
        screenshot_id   = screenshot_data['id']
        upload_ops      = screenshot_data['attributes']['uploadOperations']

        # -----------------------------------------------------------------
        # 4. Upload each chunk (most screenshots are a single part)
        # -----------------------------------------------------------------
        upload_ok = True
        for op in upload_ops:
            # Build proper dict headers
            op_headers = {h["name"]: h["value"] for h in op.get("headers", [])}

            # Slice the image data according to offset/length (handles large files)
            offset = op.get("offset", 0)
            length = op.get("length", len(img_bytes) - offset)
            chunk  = img_bytes[offset:offset + length]

            up_resp = requests.request(
                method=op['method'],
                url=op['url'],
                data=chunk,
                headers=op_headers,
                timeout=60
            )
            if up_resp.status_code >= 400:
                st.error(f"Chunk upload failed: {up_resp.status_code} {up_resp.text}")
                upload_ok = False
                break

        if not upload_ok:
            overall_success = False
            continue

        # -----------------------------------------------------------------
        # 5. Mark the screenshot as uploaded (PATCH …/appScreenshots/{id})
        # -----------------------------------------------------------------
        finish_url = f"{BASE_URL}/appScreenshots/{screenshot_id}"
        finish_payload = {
            "data": {
                "type": "appScreenshots",
                "id": screenshot_id,
                "attributes": {"uploaded": True}
            }
        }
        finish_resp = requests.patch(
            finish_url,
            json=finish_payload,
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        )
        if finish_resp.status_code != 200:
            st.error(f"Failed to finalize screenshot {screenshot_id}: {finish_resp.text}")
            overall_success = False
            continue

        st.success(f"Uploaded {disp_type} screenshot for locale `{loc_id}`")

    # -----------------------------------------------------------------
    # 6. Refresh DB after all uploads (so UI shows new URLs)
    # -----------------------------------------------------------------
    if overall_success:
        fetch_screenshots(app_id, store_id, issuer_id, key_id, private_key)

    return overall_success

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
    # -------------------------------
    # Call: Fetch Screenshots
    # -------------------------------
    fetch_screenshots(app_id, store_id, issuer_id, key_id, private_key)

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
import json
import os
import logging
from datetime import datetime, timedelta

import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()

# Configure logging
script_dir = os.path.dirname(os.path.abspath(__file__))
file_path = os.path.join(script_dir, "log.txt")
logging.basicConfig(
    filename=file_path,
    filemode='a',
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    level=logging.INFO
)

def log(message):
    logging.info(message)
    print(message)

# Constants
BASE_URL_1 = "https://api.frontegg.com"
BASE_URL_2 = "https://api.us.frontegg.com"

CLIENT_ID_1 = ''  # Replace with your actual Client ID
API_KEY_1 = ''    # Replace with your actual API Key

CLIENT_ID_2 = ''  # Replace with your actual Client ID
API_KEY_2 = ''    # Replace with your actual API Key

class FronteggClient:
    def __init__(self, base_url, client_id, secret):
        log(f"Initializing FronteggClient for base URL {base_url}")
        self.base_url = base_url
        self.client_id = client_id
        self.secret = secret
        self.token = None
        self.token_expiry = datetime.utcnow()
        self.session = requests.Session()
        self.authenticate()

    def authenticate(self):
        log(f"Authenticating with client ID {self.client_id}")
        endpoint = self.base_url + '/frontegg/identity/resources/auth/v1/vendor'
        req_body = {
            'clientId': self.client_id,
            'secret': self.secret
        }
        try:
            response = self.session.post(endpoint, json=req_body)
            response.raise_for_status()
            response_json = response.json()
            token = response_json.get("accessToken")
            expires_in = response_json.get("expiresIn", 3600)
            if not token:
                raise ValueError("Authentication failed: No token found in response.")
            self.token = token
            self.token_expiry = datetime.utcnow() + timedelta(seconds=expires_in - 60)
            log("Authentication successful.")
        except requests.exceptions.RequestException as e:
            log(f"Authentication error: {e}")
            raise
        except ValueError as e:
            log(str(e))
            raise

    def get_headers(self):
        if not self.token or datetime.utcnow() >= self.token_expiry:
            self.authenticate()
        return {'Authorization': f'Bearer {self.token}'}

FRONTEGG_ACCOUNT_1 = FronteggClient(
    base_url=BASE_URL_1,
    client_id=CLIENT_ID_1,
    secret=API_KEY_1
)

FRONTEGG_ACCOUNT_2 = FronteggClient(
    base_url=BASE_URL_2,
    client_id=CLIENT_ID_2,
    secret=API_KEY_2
)

def make_request_with_rate_limiting(method, url, client, headers=None, params=None, json_data=None, data=None):
    log(f"Making {method} request to {url}")
    while True:
        try:
            response = client.session.request(method, url, headers=headers, params=params, json=json_data, data=data)
            if response.status_code == 429:
                retry_after = int(response.headers.get('Retry-After', 1))
                log(f"Rate limited. Retrying after {retry_after} seconds.")
                time.sleep(retry_after)
            else:
                response.raise_for_status()
                return response
        except requests.exceptions.RequestException as e:
            log(f"Request error: {e}")
            raise

def get_all_paginated_items(client, endpoint, params=None):
    log(f"Fetching paginated items from {endpoint}")
    headers = client.get_headers()
    items = []
    next_url = endpoint
    while next_url:
        log(f"Requesting page: {next_url}")
        response = make_request_with_rate_limiting('GET', next_url, client, headers=headers, params=params)
        response_json = response.json()
        items.extend(response_json.get('items', []))
        next_link = response_json.get('_links', {}).get('next')
        if next_link:
            next_url = client.base_url + next_link
            params = {}
        else:
            next_url = None
    log(f"Fetched {len(items)} items from {endpoint}")
    return items

def get_tenants(client):
    log("Calling get_tenants")
    endpoint = client.base_url + '/tenants/resources/tenants/v2'
    params = {'_limit': 100}
    tenants = get_all_paginated_items(client, endpoint, params)
    log(f"Retrieved {len(tenants)} tenants")
    return tenants

def create_tenant(client, tenant):
    log(f"Creating tenant {tenant['tenantId']}")
    endpoint = client.base_url + '/tenants/resources/tenants/v1'
    headers = client.get_headers()
    req_body = {
        'tenantId': tenant['tenantId'],
        'name': tenant['name'],
    }
    try:
        response = make_request_with_rate_limiting('POST', endpoint, client, headers=headers, json_data=req_body)
        log(f"Created tenant {tenant['tenantId']}: {response.status_code}")
        return response
    except Exception as e:
        log(f"Error creating tenant {tenant['tenantId']}: {e}")

def set_tenant_metadata(client, tenant_id, metadata):
    log(f"Setting metadata for tenant {tenant_id}")
    endpoint = client.base_url + f'/tenants/resources/tenants/v1/{tenant_id}/metadata'
    headers = client.get_headers()
    try:
        response = make_request_with_rate_limiting('POST', endpoint, client, headers=headers, json_data={'metadata': metadata})
        log(f'Updated metadata for tenant {tenant_id}: {response.status_code}')
        return response
    except Exception as e:
        log(f"Error updating metadata for tenant {tenant_id}: {e}")

def bulk_create_tenants(destination_client, tenants):
    log("Starting bulk creation of tenants")
    existing_tenants = {t['tenantId'] for t in get_tenants(destination_client)}
    for tenant in tenants:
        if tenant['tenantId'] in existing_tenants:
            log(f"Tenant {tenant['tenantId']} already exists. Skipping.")
            continue
        create_tenant(destination_client, tenant)
    log("Completed bulk creation of tenants")

def migrate_tenants(source_client, destination_client):
    log("Starting tenant migration")
    source_tenants = get_tenants(source_client)
    bulk_create_tenants(destination_client, source_tenants)
    for tenant in source_tenants:
        metadata = tenant.get('metadata')
        if metadata:
            try:
                metadata_json = json.loads(metadata)
                set_tenant_metadata(destination_client, tenant['tenantId'], metadata_json)
            except json.JSONDecodeError:
                log(f"Invalid metadata for tenant {tenant['tenantId']}")
    log("Tenant migration completed")

def get_all_users(client):
    log("Calling get_all_users")
    endpoint = client.base_url + '/identity/resources/users/v2'
    params = {'_limit': 100, '_includeSubTenants': True}
    users = get_all_paginated_items(client, endpoint, params)
    log(f"Retrieved {len(users)} users")
    return users

def get_user_roles(client, tenant_id, user_ids, chunk_size=100):
    log(f"Getting user roles for tenant {tenant_id}")
    endpoint = client.base_url + "/identity/resources/users/v3/roles"
    headers = client.get_headers()
    headers['frontegg-tenant-id'] = tenant_id
    all_roles = []
    for i in range(0, len(user_ids), chunk_size):
        chunk = user_ids[i:i + chunk_size]
        log(f"Fetching roles for {len(chunk)} users in tenant {tenant_id}")
        try:
            response = make_request_with_rate_limiting('GET', endpoint, client, headers=headers, params={'ids': ','.join(chunk)})
            roles = response.json()
            all_roles.extend(roles)
        except Exception as e:
            log(f"Error fetching roles for tenant {tenant_id}: {e}")
    log(f"Retrieved roles for {len(all_roles)} users in tenant {tenant_id}")
    return all_roles

def get_all_user_roles(client, users_df):
    log("Getting all user roles")
    tenant_ids = users_df['tenantId'].unique()
    all_role_ids = []
    for tenant_id in tenant_ids:
        tenant_df = users_df[users_df['tenantId'] == tenant_id]
        user_ids = tenant_df['id_source'].tolist()
        log(f"Processing tenant {tenant_id} with {len(user_ids)} users")
        role_ids = get_user_roles(client, tenant_id, user_ids)
        all_role_ids.extend(role_ids)
    log(f"Retrieved roles for all users")
    return all_role_ids

def assign_user_roles(client, users_df):
    log("Assigning user roles")
    endpoint_template = client.base_url + "/identity/resources/users/v1/{userId}/roles"
    for _, row in users_df[users_df['roleIds'].notnull()].iterrows():
        endpoint = endpoint_template.format(userId=row['id_dest'])
        headers = client.get_headers()
        headers['frontegg-tenant-id'] = row['tenantId']
        role_ids = json.loads(row['roleIds'])
        try:
            response = make_request_with_rate_limiting('POST', endpoint, client, headers=headers, json_data={'roleIds': role_ids})
            log(f'Assigned roles to user {row["email"]}: {response.status_code}')
        except Exception as e:
            log(f"Error assigning roles to user {row['email']}: {e}")
    log("User role assignment completed")

def get_roles_mapping(client):
    log("Getting roles mapping")
    endpoint = client.base_url + '/identity/resources/roles/v1'
    headers = client.get_headers()
    try:
        response = make_request_with_rate_limiting('GET', endpoint, client, headers=headers)
        roles_list = response.json()
        log(f"Retrieved {len(roles_list)} roles")
        return {role['key']: role['id'] for role in roles_list}
    except Exception as e:
        log(f"Error fetching roles: {e}")
        return {}

def get_role_id_translations(source_client, destination_client):
    log("Getting role ID translations")
    source_roles = get_roles_mapping(source_client)
    destination_roles = get_roles_mapping(destination_client)
    role_id_translation = {}
    for key, source_id in source_roles.items():
        dest_id = destination_roles.get(key)
        if dest_id:
            role_id_translation[source_id] = dest_id
        else:
            log(f"Role key '{key}' not found in destination account.")
    log(f"Completed role ID translations")
    return role_id_translation

def translate_role_ids(role_id_translation, role_ids):
    translated_role_ids = []
    for role_id in role_ids:
        translated_id = role_id_translation.get(role_id)
        if translated_id:
            translated_role_ids.append(translated_id)
        else:
            log(f"Role ID '{role_id}' not found in translation mapping.")
    return json.dumps(translated_role_ids)

def migrate_users(source_client, destination_client):
    log("Starting user migration")
    source_users = get_all_users(source_client)
    destination_users = get_all_users(destination_client)

    source_users_df = pd.DataFrame(source_users)
    destination_users_df = pd.DataFrame(destination_users)

    # Normalize emails to lowercase
    source_users_df['email'] = source_users_df['email'].str.lower()
    destination_users_df['email'] = destination_users_df['email'].str.lower()

    # Merge DataFrames
    users_df = pd.merge(
        source_users_df,
        destination_users_df,
        on=['email', 'tenantId'],
        how='left',
        suffixes=('_source', '_dest')
    )

    log(f"Merged users dataframe with {len(users_df)} entries")

    # Get all user roles
    all_user_roles = get_all_user_roles(source_client, users_df)
    all_user_roles = [u for u in all_user_roles if isinstance(u, dict)]
    all_user_roles_df = pd.DataFrame(all_user_roles)

    log(f"Retrieved user roles dataframe with {len(all_user_roles_df)} entries")

    # Get role ID translations
    role_id_translations = get_role_id_translations(source_client, destination_client)

    all_user_roles_df['roleIds'] = all_user_roles_df['roleIds'].apply(
        lambda r: translate_role_ids(role_id_translations, r)
    )

    users_df = pd.merge(users_df, all_user_roles_df, left_on='id_source', right_on='userId', how='inner')
    users_df = users_df[['email', 'tenantId', 'id_source', 'id_dest', 'roleIds']]
    assign_user_roles(destination_client, users_df)
    log("User migration completed")

def get_permissions(client):
    log("Getting permissions")
    endpoint = client.base_url + '/identity/resources/permissions/v1'
    headers = client.get_headers()
    try:
        response = make_request_with_rate_limiting('GET', endpoint, client, headers=headers)
        permissions = response.json()
        log(f"Retrieved {len(permissions)} permissions")
        return permissions
    except Exception as e:
        log(f"Error fetching permissions: {e}")
        return []

def create_permissions(client, permissions):
    log("Creating permissions")
    endpoint = client.base_url + '/identity/resources/permissions/v1'
    headers = client.get_headers()
    permissions_data = [{
        'name': p['name'],
        'description': p.get('description', ''),
        'categoryId': p['categoryId'],
        'key': p['key'],
        'assignmentType': p.get('assignmentType', 'Admin'),
    } for p in permissions if p.get('categoryId')]
    try:
        response = make_request_with_rate_limiting('POST', endpoint, client, headers=headers, json_data=permissions_data)
        log(f'Permissions created: {response.status_code}')
        return response.json()
    except Exception as e:
        log(f"Error creating permissions: {e}")

def get_categories(client):
    log("Getting categories")
    endpoint = client.base_url + '/identity/resources/permissions/v1/categories'
    headers = client.get_headers()
    try:
        response = make_request_with_rate_limiting('GET', endpoint, client, headers=headers)
        categories = response.json()
        log(f"Retrieved {len(categories)} categories")
        return categories
    except Exception as e:
        log(f"Error fetching categories: {e}")
        return []

def create_categories(client, categories):
    log("Creating categories")
    endpoint = client.base_url + '/identity/resources/permissions/v1/categories'
    headers = client.get_headers()
    for category in categories:
        category_json = {
            'name': category['name'],
            'description': category.get('description', ''),
        }
        try:
            response = make_request_with_rate_limiting('POST', endpoint, client, headers=headers, json_data=category_json)
            log(f'Category {category["name"]} created: {response.status_code}')
        except Exception as e:
            log(f"Error creating category {category['name']}: {e}")
    log("Categories creation completed")

def migrate_settings(source_client, destination_client):
    log("Starting settings migration")
    source_categories = get_categories(source_client)
    create_categories(destination_client, source_categories)

    # Retrieve destination categories after creation
    destination_categories = get_categories(destination_client)

    # Map category names to IDs
    category_mapping = {
        src_cat['id']: next(
            (dest_cat['id'] for dest_cat in destination_categories if dest_cat['name'] == src_cat['name']), None
        )
        for src_cat in source_categories
    }

    source_permissions = get_permissions(source_client)
    for permission in source_permissions:
        permission['categoryId'] = category_mapping.get(permission['categoryId'])
        if not permission['categoryId']:
            log(f"Category ID for permission {permission['name']} not found. Skipping.")
    create_permissions(destination_client, source_permissions)
    log("Settings migration completed")

def create_migration_csv(users_path, hashes_path, save_path):
    log("Creating migration CSV")
    try:
        users_df = pd.read_csv(users_path)
        hashes_df = pd.read_csv(hashes_path)
    except FileNotFoundError as e:
        log(f"File not found: {e}")
        raise

    # Convert 'createdAt' to datetime
    hashes_df['createdAt'] = pd.to_datetime(hashes_df['createdAt'])

    # Get the latest hash for each userId
    latest_hashes = hashes_df.sort_values('createdAt').groupby('userId').tail(1)

    # Merge DataFrames
    final_df = users_df.merge(latest_hashes, on='userId', how='left')

    columns_to_select = {
        "name": "name",
        "email": "email",
        "tenantId": "tenantId",
        "account": "tenantId",
        "hash": "passwordHash",
        "password": "passwordHash",
        "phone": "phoneNumber",
        "profilePictureUrl": "profilePictureUrl",
        "authenticatorSecret": "authenticatorAppMfaSecret",
        "additionalFields": "metadata",
        "provider": "provider",
        "verified": "verifyUser"
    }
    final_columns = [col for col in columns_to_select.keys() if col in final_df.columns]
    final_df = final_df[final_columns]
    final_df.to_csv(save_path, index=False)
    log(f"Migration CSV created at {save_path}")

def migrate(source_client, destination_client):
    log('Starting migration...')

    # Migrate Tenants
    migrate_tenants(source_client, destination_client)

    # Ensure the account_data directory exists
    account_data_dir = os.path.join(script_dir, 'account_data')
    if not os.path.exists(account_data_dir):
        os.makedirs(account_data_dir)
        log(f"Created directory {account_data_dir}")

    # Create Migration CSV
    create_migration_csv(
        os.path.join(account_data_dir, 'user_details_prod.csv'),
        os.path.join(account_data_dir, 'user_passwords_prod.csv'),
        os.path.join(account_data_dir, 'migration_data.csv')
    )

    # Migrate Settings
    migrate_settings(source_client, destination_client)

    # Migrate Users
    migrate_users(source_client, destination_client)

    log('Migration completed!')

if __name__ == '__main__':
    migrate(source_client=FRONTEGG_ACCOUNT_1, destination_client=FRONTEGG_ACCOUNT_2)

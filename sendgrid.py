import requests
import os
import json
import re
# SENDGRID_API_KEY=""
# HEADERS = {
#     'Authorization': f'Bearer {SENDGRID_API_KEY}',
#     'Content-Type': 'application/json'
# }

# Create output directory
os.makedirs("sendgrid_templates", exist_ok=True)

# Step 1: Get all dynamic templates
template_list_url = 'https://api.sendgrid.com/v3/templates?generations=dynamic'
response = requests.get(template_list_url, headers=HEADERS)

if response.status_code != 200:
    print(f"Error fetching templates: {response.status_code} - {response.text}")
    exit(1)

templates = response.json().get('templates', [])
print(templates)

def sanitize_filename(name):
    # Replace all non-alphanumeric and non-underscore/dash characters (like slashes, colons, etc.)
    return re.sub(r'[^\w\-]', '_', name)
# Step 2: For each template, get details and save HTML + JSON
for template in templates:
    template_id = template['id']
    name = sanitize_filename(template['name'])
    print(f"Processing template: {name} ({template_id})")

    # Get full template data (including versions)
    template_detail_url = f'https://api.sendgrid.com/v3/templates/{template_id}'
    detail_response = requests.get(template_detail_url, headers=HEADERS)

    if detail_response.status_code != 200:
        print(f"  Failed to get template details: {detail_response.status_code}")
        continue

    data = detail_response.json()

    # Save full JSON data
    json_filename = f"sendgrid_templates/{name}_{template_id}.json"
    with open(json_filename, 'w') as f:
        json.dump(data, f, indent=2)

    # Extract and save active version's HTML content
    versions = data.get('versions', [])
    active_versions = [v for v in versions if v.get('active') == 1]

    if active_versions:
        html_content = active_versions[0].get('html_content', '')
        html_filename = f"sendgrid_templates/{name}_{template_id}.html"
        with open(html_filename, 'w', encoding='utf-8') as f:
            f.write(html_content)
    else:
        print(f"  No active version found for {name}")

print("✅ Download complete.")

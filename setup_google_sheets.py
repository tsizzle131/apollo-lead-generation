#!/usr/bin/env python3
"""
Google Sheets Setup Helper

This script helps you set up Google Sheets API authentication for your lead generation system.
"""

import os
import json
import sys
from google.oauth2 import service_account
from googleapiclient.discovery import build

def test_credentials(credentials_path):
    """Test if the credentials file works"""
    try:
        # Load credentials
        credentials = service_account.Credentials.from_service_account_file(
            credentials_path,
            scopes=['https://www.googleapis.com/auth/spreadsheets']
        )
        
        # Build service
        service = build('sheets', 'v4', credentials=credentials)
        
        # Try to get sheet metadata (this will fail if sheet isn't shared)
        sheet_id = input("Enter your Google Sheets ID: ").strip()
        
        result = service.spreadsheets().get(spreadsheetId=sheet_id).execute()
        
        print("✅ Success! Google Sheets API is working.")
        print(f"Sheet title: {result.get('properties', {}).get('title', 'Unknown')}")
        
        # Show service account email
        with open(credentials_path, 'r') as f:
            creds_data = json.load(f)
            service_email = creds_data.get('client_email')
            print(f"\n📧 Service Account Email: {service_email}")
            print("Make sure your Google Sheet is shared with this email address!")
        
        return True
        
    except FileNotFoundError:
        print("❌ Credentials file not found!")
        return False
    except Exception as e:
        if "The caller does not have permission" in str(e):
            print("❌ Permission denied! Make sure to share your Google Sheet with the service account email.")
        else:
            print(f"❌ Error: {e}")
        return False

def create_env_file(sheet_id):
    """Create or update .env file with Google Sheets configuration"""
    env_path = os.path.join(os.path.dirname(__file__), '.env')
    
    env_content = f"""# Google Sheets Configuration
GOOGLE_SHEETS_ID={sheet_id}
SEARCH_URL_SHEET=search urls
LEADS_SHEET=leads

# You can also set your API keys here instead of the UI
# OPENAI_API_KEY=your_openai_key_here
# APIFY_API_KEY=your_apify_key_here
"""
    
    with open(env_path, 'w') as f:
        f.write(env_content)
    
    print(f"✅ Created .env file with Google Sheets ID: {sheet_id}")

def main():
    print("🔧 Google Sheets API Setup Helper")
    print("=" * 40)
    
    # Check if credentials file exists
    credentials_path = os.path.join(os.path.dirname(__file__), 'credentials.json')
    
    if not os.path.exists(credentials_path):
        print(f"❌ Credentials file not found at: {credentials_path}")
        print("\n📋 Setup Instructions:")
        print("1. Go to https://console.cloud.google.com/")
        print("2. Create a new project or select existing")
        print("3. Enable Google Sheets API")
        print("4. Go to 'APIs & Services' → 'Credentials'")
        print("5. Click 'Create Credentials' → 'Service Account'")
        print("6. Download the JSON file and save it as 'credentials.json' in this directory")
        print("7. Run this script again")
        return
    
    print("✅ Found credentials file")
    
    # Test credentials
    if test_credentials(credentials_path):
        sheet_id = input("Enter your Google Sheets ID to save in .env: ").strip()
        if sheet_id:
            create_env_file(sheet_id)
    
    print("\n🎉 Setup complete! Your lead generation script should now work with Google Sheets.")

if __name__ == "__main__":
    main()
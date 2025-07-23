import logging
from typing import List, Dict, Any, Optional
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import json
import os

class GoogleSheetsManager:
    def __init__(self, spreadsheet_id: str, credentials_path: Optional[str] = None):
        self.spreadsheet_id = spreadsheet_id
        self.service = self._authenticate(credentials_path)
        
    def _authenticate(self, credentials_path: Optional[str]):
        """Authenticate with Google Sheets API"""
        try:
            if credentials_path and os.path.exists(credentials_path):
                # Use service account credentials file
                credentials = service_account.Credentials.from_service_account_file(
                    credentials_path,
                    scopes=['https://www.googleapis.com/auth/spreadsheets']
                )
            else:
                # Try to use environment variable for credentials JSON
                creds_json = os.getenv('GOOGLE_CREDENTIALS_JSON')
                if creds_json:
                    credentials_info = json.loads(creds_json)
                    credentials = service_account.Credentials.from_service_account_info(
                        credentials_info,
                        scopes=['https://www.googleapis.com/auth/spreadsheets']
                    )
                else:
                    raise ValueError("No Google credentials found. Set GOOGLE_CREDENTIALS_JSON environment variable or provide credentials_path")
            
            return build('sheets', 'v4', credentials=credentials)
        except Exception as e:
            logging.error(f"Failed to authenticate with Google Sheets: {e}")
            raise

    def read_sheet(self, sheet_name: str, range_name: str = None) -> List[List[str]]:
        """Read data from a Google Sheet"""
        try:
            if range_name:
                range_str = f"{sheet_name}!{range_name}"
            else:
                range_str = sheet_name
                
            result = self.service.spreadsheets().values().get(
                spreadsheetId=self.spreadsheet_id,
                range=range_str
            ).execute()
            
            return result.get('values', [])
        except HttpError as e:
            logging.error(f"Failed to read sheet {sheet_name}: {e}")
            return []

    def get_search_urls(self, sheet_name: str) -> List[Dict[str, str]]:
        """Get search URLs from the first sheet"""
        try:
            data = self.read_sheet(sheet_name)
            if not data or len(data) < 2:  # Need header + at least one row
                return []
            
            headers = data[0]
            urls = []
            
            for row in data[1:]:  # Skip header
                if row:  # Skip empty rows
                    url_dict = {}
                    for i, header in enumerate(headers):
                        if i < len(row):
                            url_dict[header] = row[i]
                        else:
                            url_dict[header] = ""
                    urls.append(url_dict)
            
            return urls
        except Exception as e:
            logging.error(f"Failed to get search URLs: {e}")
            return []

    def append_lead(self, sheet_name: str, lead_data: Dict[str, Any]) -> bool:
        """Append a lead to the leads sheet"""
        try:
            # Define the expected column order
            columns = [
                'first_name', 'last_name', 'email', 'website_url', 
                'phone_number', 'location', 'mutiline_icebreaker'
            ]
            
            # Create row data in the correct order
            row_data = []
            for col in columns:
                value = lead_data.get(col, '')
                # Handle None values
                if value is None:
                    value = ''
                row_data.append(str(value))
            
            # Append the row
            body = {
                'values': [row_data]
            }
            
            result = self.service.spreadsheets().values().append(
                spreadsheetId=self.spreadsheet_id,
                range=f"{sheet_name}!A:G",  # Assuming 7 columns
                valueInputOption='RAW',
                insertDataOption='INSERT_ROWS',
                body=body
            ).execute()
            
            logging.info(f"Added lead: {lead_data.get('first_name', '')} {lead_data.get('last_name', '')}")
            return True
            
        except HttpError as e:
            logging.error(f"Failed to append lead: {e}")
            return False
        except Exception as e:
            logging.error(f"Unexpected error appending lead: {e}")
            return False

    def batch_append_leads(self, sheet_name: str, leads_data: List[Dict[str, Any]]) -> int:
        """Append multiple leads at once"""
        try:
            if not leads_data:
                return 0
                
            columns = [
                'first_name', 'last_name', 'email', 'website_url', 
                'phone_number', 'location', 'mutiline_icebreaker'
            ]
            
            # Prepare batch data
            batch_data = []
            for lead_data in leads_data:
                row_data = []
                for col in columns:
                    value = lead_data.get(col, '')
                    if value is None:
                        value = ''
                    row_data.append(str(value))
                batch_data.append(row_data)
            
            # Batch append
            body = {
                'values': batch_data
            }
            
            result = self.service.spreadsheets().values().append(
                spreadsheetId=self.spreadsheet_id,
                range=f"{sheet_name}!A:G",
                valueInputOption='RAW',
                insertDataOption='INSERT_ROWS',
                body=body
            ).execute()
            
            added_rows = len(batch_data)
            logging.info(f"Added {added_rows} leads to sheet")
            return added_rows
            
        except Exception as e:
            logging.error(f"Failed to batch append leads: {e}")
            return 0
"""
WSR Parser - ENHANCED VERSION
Processes Jimmy John's Weekly Sales Reports AND Expected Deposits
Uploads to Supabase and creates Google Sheets tabs by Legal Entity

Part 1: WSR line-item parsing -> Supabase (services_wsr) + Google Sheets (by entity+week)
Part 2: Expected Deposits extraction -> Supabase (weekly_sales_expected) + CSV export
"""

import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime
import datetime as dt
from pathlib import Path
from dotenv import load_dotenv
import logging
from typing import List, Dict, Any, Optional
import re
import zipfile
import shutil
import openpyxl

# Google Sheets imports
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Supabase import
from supabase import create_client, Client

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('wsr_parser.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# ============================================================================
# HELPER FUNCTIONS FOR WEEKLY SALES EXPECTED DEPOSITS
# ============================================================================

def _to_iso_date(x: Any) -> Optional[str]:
    """Convert various date formats to ISO date string"""
    if x is None:
        return None
    if isinstance(x, dt.datetime):
        return x.date().isoformat()
    if isinstance(x, dt.date):
        return x.isoformat()
    if isinstance(x, float):
        return None
    if isinstance(x, str):
        s = x.strip()
        # Match MM/DD/YYYY
        m = re.match(r"^(\d{1,2})/(\d{1,2})/(\d{4})$", s)
        if m:
            mm, dd, yyyy = m.groups()
            return f"{yyyy}-{int(mm):02d}-{int(dd):02d}"
        # Match YYYY-MM-DD
        m2 = re.match(r"^\d{4}-\d{2}-\d{2}$", s)
        if m2:
            return s
        return s
    return str(x).strip()


def _to_float(x: Any) -> Optional[float]:
    """Convert various formats to float, handling None and empty strings"""
    if x is None:
        return 0.0
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).strip().replace(",", "")
    if s == "":
        return 0.0
    try:
        return float(s)
    except ValueError:
        return None


# ============================================================================
# MAIN PARSER CLASS
# ============================================================================

class WSRParser:
    """Parse WSR files and Weekly Sales Expected Deposits, upload to Supabase & Google Sheets"""
    
    def __init__(self):
        load_dotenv()
        
        # Supabase configuration
        self.supabase_url = os.getenv('SUPABASE_URL')
        self.supabase_key = os.getenv('SUPABASE_SERVICE_ROLE_KEY')
        
        if self.supabase_url and self.supabase_key:
            self.supabase = create_client(self.supabase_url, self.supabase_key)
            logger.info("Supabase client initialized")
        else:
            self.supabase = None
            logger.warning("Supabase credentials not found - will skip upload")
        
        # Google Sheets configuration
        self.spreadsheet_id = os.getenv('GOOGLE_SHEET_ID')
        self.credentials_path = os.getenv('GOOGLE_CREDENTIALS_PATH')
        
        if self.spreadsheet_id and self.credentials_path and os.path.exists(self.credentials_path):
            try:
                scopes = ['https://www.googleapis.com/auth/spreadsheets']
                creds = Credentials.from_service_account_file(
                    self.credentials_path, 
                    scopes=scopes
                )
                self.sheets_service = build('sheets', 'v4', credentials=creds)
                logger.info("Google Sheets API initialized")
            except Exception as e:
                logger.error(f"Failed to initialize Google Sheets: {e}")
                self.sheets_service = None
        else:
            self.sheets_service = None
            logger.warning("Google Sheets not configured - will skip sheet creation")
        
        # Load store mapping
        self.store_mapping = self.load_store_mapping()
        
        # Batch size for Supabase uploads
        self.batch_size = 1000
        
        # Load account mapping from Google Sheets "Key" tab
        self.account_mapping = self.load_account_mapping()
        
        # Expected Deposits table name
        self.expected_deposits_table = os.getenv('EXPECTED_DEPOSITS_TABLE', 'weekly_sales_expected')
    
    # ========================================================================
    # STORE & ACCOUNT MAPPING
    # ========================================================================
    
    def load_account_mapping(self) -> Dict:
        """Load WSR to QBO account mapping from Google Sheets 'Key' tab"""
        if not self.sheets_service or not self.spreadsheet_id:
            logger.warning("Google Sheets not configured, using WSR names as-is")
            return {}
        
        try:
            logger.info("Loading account mapping from 'Key' tab...")
            
            # Read the Key tab
            result = self.sheets_service.spreadsheets().values().get(
                spreadsheetId=self.spreadsheet_id,
                range='Key!A:D'
            ).execute()
            
            values = result.get('values', [])
            
            if not values:
                logger.warning("Key tab is empty")
                return {}
            
            # Build mapping dictionary
            # Skip header row (row 0)
            mapping = {}
            logger.info(f"Reading Key tab rows (showing first 10):")
            
            for idx, row in enumerate(values[1:], start=2):  # Start at 2 to match spreadsheet rows
                if len(row) >= 2:  # At minimum need WSR Name and QBO Name
                    wsr_name = row[0].strip() if row[0] else ''
                    qbo_name = row[1].strip() if len(row) > 1 and row[1] else ''
                    
                    # Show raw row data for first 10 rows
                    if idx <= 11:
                        logger.info(f"  Row {idx}: {row}")
                    
                    # Get Name from column C (index 2)
                    name = ''
                    if len(row) > 2 and row[2]:
                        val = row[2].strip()
                        # Only use it as a name if it's NOT "Debit", "Credit", or "Reverse"
                        if val.lower() not in ['debit', 'credit', 'reverse']:
                            name = val
                    
                    # Find Debit/Credit/Reverse - could be in column C or D
                    debit_credit = 'Debit'  # Default
                    
                    # Check index 3 first (Column D)
                    if len(row) > 3 and row[3]:
                        debit_credit = row[3].strip()
                    # If not found, check index 2 (Column C might have Debit/Credit/Reverse)
                    elif len(row) > 2 and row[2]:
                        val = row[2].strip()
                        if val.lower() in ['debit', 'credit', 'reverse']:
                            debit_credit = val
                    
                    if wsr_name and qbo_name:
                        mapping[wsr_name] = {
                            'qbo_account': qbo_name,
                            'debit_credit': debit_credit,
                            'name': name
                        }
                        if len(mapping) <= 5:
                            name_info = f" -> {name}" if name else ""
                            logger.info(f"  ✓ {wsr_name} -> {qbo_name} ({debit_credit}){name_info}")
            
            logger.info(f"✓ Loaded {len(mapping)} account mappings from Key tab")
            return mapping
            
        except Exception as e:
            logger.warning(f"Could not load Key tab: {e}")
            return {}
    
    def load_store_mapping(self) -> Dict:
        """Load store to legal entity mapping"""
        mapping = {
            # ---- Atlas East - Pennsylvania ----
            2682: {"legal_entity": "Atlas East", "class_code": "2682 - North Fayatte", "store_name": "North Fayette"},
            2683: {"legal_entity": "Atlas East", "class_code": "2683 - Bridgeville", "store_name": "Bridgeville"},
            2749: {"legal_entity": "Atlas East", "class_code": "2749 - Cannonsburg", "store_name": "Southpointe"},
            3686: {"legal_entity": "Atlas East", "class_code": "3686 - Homestead", "store_name": "Homestead"},
            4586: {"legal_entity": "Atlas East", "class_code": "4586 - Pittsburgh Airport", "store_name": "Pittsburgh Airport"},
            # ---- Atlas NGC - Minnesota ----
            746: {"legal_entity": "Atlas NGC", "class_code": "0746 - Burnsville", "store_name": "Burnsville"},
            833: {"legal_entity": "Atlas NGC", "class_code": "0833 - Shakopee", "store_name": "Shakopee"},
            1061: {"legal_entity": "Atlas NGC", "class_code": "1061 - Wayzata", "store_name": "Wayzata"},
            1206: {"legal_entity": "Atlas NGC", "class_code": "1206 - Savage", "store_name": "Savage"},
            1337: {"legal_entity": "Atlas NGC", "class_code": "1337 - Carriage", "store_name": "Shakopee II"},
            # ---- Atlas 0519 - Minnesota ----
            522: {"legal_entity": "Atlas 0519", "class_code": "0522 - Warren", "store_name": "Mankato"},
            1342: {"legal_entity": "Atlas 0519", "class_code": "1342 - Western", "store_name": "Fairbault"},
            2021: {"legal_entity": "Atlas 0519", "class_code": "2021 - Holly", "store_name": "Holly"},
            # ---- Atlas NGC - California ----
            2807: {"legal_entity": "Atlas NGC", "class_code": "2807 - MacArthur", "store_name": "MacArthur"},
            2808: {"legal_entity": "Atlas NGC", "class_code": "2808 - Marguerite", "store_name": "Mission Viejo"},
            # ---- Atlas West - California ----
            2811: {"legal_entity": "Atlas West", "class_code": "2811 - Edinger", "store_name": "Edinger"},
            2812: {"legal_entity": "Atlas West", "class_code": "2812 - Newhope", "store_name": "New Hope"},
            3260: {"legal_entity": "Atlas West", "class_code": "3260 - Irvine", "store_name": "Irvine"},
            2821: {"legal_entity": "Atlas West", "class_code": "2821 - Lake Forest", "store_name": "Lake Forest"},
            2873: {"legal_entity": "Atlas West", "class_code": "2873 - La Verne", "store_name": "La Verne"},
            2874: {"legal_entity": "Atlas West", "class_code": "2874 - Upland", "store_name": "Upland"},
            3391: {"legal_entity": "Atlas West", "class_code": "3391 - 4th & Haven", "store_name": "4th & Haven"},
            2876: {"legal_entity": "Atlas West", "class_code": "2876 - Irwindale", "store_name": "Irwindale"},
            4018: {"legal_entity": "Atlas West", "class_code": "4018 - Beverly Hills", "store_name": "Beverly"},
            4022: {"legal_entity": "Atlas West", "class_code": "4022 - Raymond", "store_name": "Raymond"},
            4024: {"legal_entity": "Atlas West", "class_code": "4024 - Figueroa", "store_name": "Fig"},
            # ---- Atlas 0519 - Arizona (Scottsdale/Hayden) ----
            1694: {"legal_entity": "Atlas 0519", "class_code": "1694 - Hayden", "store_name": "Hayden"},
            1695: {"legal_entity": "Atlas 0519", "class_code": "1695 - Cactus", "store_name": "Cactus"},
            2503: {"legal_entity": "Atlas 0519", "class_code": "2503 - Scottsdale", "store_name": "Scottsdale"},
            2504: {"legal_entity": "Atlas 0519", "class_code": "2504 - 90th", "store_name": "90th"},
            # ---- Atlas NGC - Arizona (West Valley) ----
            2006: {"legal_entity": "Atlas NGC", "class_code": "2006 - McDowell", "store_name": "Goodyear"},
            2391: {"legal_entity": "Atlas NGC", "class_code": "2391 - Camelback", "store_name": "W Camelback"},
            2883: {"legal_entity": "Atlas NGC", "class_code": "2883 - Payson", "store_name": "Payson"},
            1762: {"legal_entity": "Atlas NGC", "class_code": "1762 - Avondale", "store_name": "Avondale"},
            2884: {"legal_entity": "Atlas NGC", "class_code": "2884 - Estrella", "store_name": "Estrella"},
            3635: {"legal_entity": "Atlas NGC", "class_code": "3635 - Buckeye", "store_name": "Buckeye"},
            # ---- Atlas 0519 - Arizona (Phoenix/Scottsdale) ----
            1556: {"legal_entity": "Atlas 0519", "class_code": "1556 - Camelback", "store_name": "E Camelback"},
            1635: {"legal_entity": "Atlas 0519", "class_code": "1635 - Washington", "store_name": "Washington"},
            2180: {"legal_entity": "Atlas 0519", "class_code": "2180 - N 16th", "store_name": "16th"},
            2500: {"legal_entity": "Atlas 0519", "class_code": "2500 - Roosevelt", "store_name": "Roosevelt"},
            2502: {"legal_entity": "Atlas 0519", "class_code": "2502 - Central Ave", "store_name": "Central"},
            1696: {"legal_entity": "Atlas 0519", "class_code": "1696 - Agua Fria", "store_name": "Agua Fria"},
            1955: {"legal_entity": "Atlas 0519", "class_code": "1955 - East Bell", "store_name": "Bell 1"},
            1956: {"legal_entity": "Atlas 0519", "class_code": "1956 - Thunderbird", "store_name": "Thunderbird"},
            2176: {"legal_entity": "Atlas 0519", "class_code": "2176 - Tatum", "store_name": "Tatum"},
            3972: {"legal_entity": "Atlas 0519", "class_code": "3972 - Deer Valley", "store_name": "Deer Valley"},
            1554: {"legal_entity": "Atlas 0519", "class_code": "1554 - Scottsdale", "store_name": "N Scottsdale"},
            1957: {"legal_entity": "Atlas 0519", "class_code": "1957 - 44th", "store_name": "44th"},
            2178: {"legal_entity": "Atlas 0519", "class_code": "2178 - EastBell", "store_name": "Bell 2"},
            2501: {"legal_entity": "Atlas 0519", "class_code": "2501 - North Cave", "store_name": "Cave Creek"},
            # ---- Atlas East - Florida ----
            1127: {"legal_entity": "Atlas East", "class_code": "1127 - St Pete", "store_name": "St Pete"},
            1441: {"legal_entity": "Atlas East", "class_code": "1441 - Carrollwood", "store_name": "Carrollwood"},
            3030: {"legal_entity": "Atlas East", "class_code": "3030 - Waters", "store_name": "Waters"},
            3187: {"legal_entity": "Atlas East", "class_code": "3187 - Bay Pines", "store_name": "Bay Pines"},
            3613: {"legal_entity": "Atlas East", "class_code": "3613 - Odessa", "store_name": "Odessa"},
            1307: {"legal_entity": "Atlas East", "class_code": "1307 - Howard", "store_name": "Howard"},
            1440: {"legal_entity": "Atlas East", "class_code": "1440 - Stadium", "store_name": "Stadium"},
            1562: {"legal_entity": "Atlas East", "class_code": "1562 - West Shore", "store_name": "West Shore"},
            3029: {"legal_entity": "Atlas East", "class_code": "3029 - South Tampa", "store_name": "South Tampa"},
            1789: {"legal_entity": "Atlas East", "class_code": "1789 - Brandon", "store_name": "Brandon"},
            3612: {"legal_entity": "Atlas East", "class_code": "3612 - Causeway", "store_name": "Causeway"},
            4105: {"legal_entity": "Atlas East", "class_code": "4105 - Wesley Chapel", "store_name": "Wesley Chapel"},
            # ---- Atlas East - Nebraska ----
            838: {"legal_entity": "Atlas East", "class_code": "0838 - W Broadway", "store_name": "W Broadway"},
            1111: {"legal_entity": "Atlas East", "class_code": "1111 - E Broadway", "store_name": "E Broadway"},
            2712: {"legal_entity": "Atlas East", "class_code": "2712 - Lake Manawa", "store_name": "Manawa"},
            1261: {"legal_entity": "Atlas East", "class_code": "1261 - S 13th", "store_name": "S 13th"},
            799: {"legal_entity": "Atlas East", "class_code": "0799 - Farnam", "store_name": "Farnam"},
            877: {"legal_entity": "Atlas East", "class_code": "0877 - Harlan", "store_name": "Harlan"},
            1018: {"legal_entity": "Atlas East", "class_code": "1018 - Twin Creek", "store_name": "Twin Creek"},
            1019: {"legal_entity": "Atlas East", "class_code": "1019 - Giles", "store_name": "Giles"},
            1779: {"legal_entity": "Atlas East", "class_code": "1779 - Shadow Lake", "store_name": "Midlands"},
            2601: {"legal_entity": "Atlas East", "class_code": "2601 - L Street", "store_name": "L Street"},
            2711: {"legal_entity": "Atlas East", "class_code": "2711 - Gretna", "store_name": "Gretna"},
            965: {"legal_entity": "Atlas East", "class_code": "0965 - Sorenson", "store_name": "Sorenson"},
            1002: {"legal_entity": "Atlas East", "class_code": "1002 - Irvington", "store_name": "Irvington"},
            1355: {"legal_entity": "Atlas East", "class_code": "1355 - N 30th", "store_name": "N 30th"},
            4330: {"legal_entity": "Atlas East", "class_code": "4330 - Blair", "store_name": "Blair"},
            930: {"legal_entity": "Atlas East", "class_code": "0930 - Elkhorn", "store_name": "Elkhorn"},
            4358: {"legal_entity": "Atlas East", "class_code": "4358 - Indian Creek", "store_name": "Elkhorn"},
        }
        
        logger.info(f"Loaded mapping for {len(mapping)} stores")
        return mapping
    
    # ========================================================================
    # ZIP EXTRACTION
    # ========================================================================
    
    def extract_zip_files(self, directory: str) -> List[str]:
        """Extract all ZIP files in directory and return list of extracted .xls files"""
        logger.info(f"\n{'='*80}")
        logger.info(f"Extracting ZIP files from: {directory}")
        
        extracted_files = []
        zip_files = [f for f in os.listdir(directory) if f.endswith('.zip')]
        
        if not zip_files:
            logger.info("No ZIP files found")
            return extracted_files
        
        logger.info(f"Found {len(zip_files)} ZIP file(s)")
        
        for zip_filename in zip_files:
            zip_path = os.path.join(directory, zip_filename)
            logger.info(f"\n-> Extracting: {zip_filename}")
            
            try:
                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    file_list = zip_ref.namelist()
                    xls_files = [f for f in file_list if f.endswith('.xls') or f.endswith('.xlsx')]
                    
                    logger.info(f"  Contains {len(xls_files)} Excel file(s)")
                    
                    # Extract all files to the same directory
                    zip_ref.extractall(directory)
                    
                    for xls_file in xls_files:
                        extracted_path = os.path.join(directory, xls_file)
                        if os.path.exists(extracted_path):
                            extracted_files.append(extracted_path)
                            logger.info(f"  ✓ Extracted: {xls_file}")
                
                logger.info(f"✓ Successfully extracted {zip_filename}")
                
            except Exception as e:
                logger.error(f"✗ Failed to extract {zip_filename}: {e}")
        
        logger.info(f"\n✓ Total Excel files extracted: {len(extracted_files)}")
        return extracted_files
    
    # ========================================================================
    # PART 1: WSR LINE-ITEM PARSING
    # ========================================================================
    
    def parse_wsr_file(self, filepath: str) -> List[Dict]:
        """Parse a single WSR file and extract all account data"""
        logger.info(f"\n{'='*80}")
        logger.info(f"Parsing WSR file: {os.path.basename(filepath)}")
        
        try:
            # Read the Weekly Sales sheet
            df = pd.read_excel(filepath, sheet_name='Weekly Sales')
            
            # Extract metadata from header rows
            week_ending = None
            store_number = None
            
            # Get week ending date from row 0
            if df.shape[0] > 0:
                week_text = str(df.iloc[0, 2])  # Column C (index 2)
                if pd.notna(week_text) and week_text != 'nan':
                    try:
                        week_ending = pd.to_datetime(week_text).strftime('%m/%d/%Y')
                    except:
                        logger.warning(f"Could not parse week ending date: {week_text}")
            
            # Get store number from row 2
            if df.shape[0] > 2:
                store_text = str(df.iloc[2, 2])  # Column C (index 2)
                if pd.notna(store_text) and store_text != 'nan':
                    try:
                        store_number = int(float(store_text))
                    except:
                        logger.warning(f"Could not parse store number: {store_text}")
            
            if not week_ending or not store_number:
                logger.error(f"Missing required metadata: week_ending={week_ending}, store_number={store_number}")
                return []
            
            logger.info(f"Week Ending: {week_ending}")
            logger.info(f"Store Number: {store_number}")
            
            # Get store info from mapping
            store_info = self.store_mapping.get(store_number)
            if not store_info:
                logger.warning(f"Store {store_number} not found in mapping")
                legal_entity = "Unknown"
                class_code = f"{store_number} - Unknown"
                store_name = f"Store {store_number}"
            else:
                legal_entity = store_info['legal_entity']
                class_code = store_info['class_code']
                store_name = store_info['store_name']
            
            logger.info(f"Legal Entity: {legal_entity}")
            logger.info(f"Class Code: {class_code}")
            
            # Find the header row (contains "Sales Item" and "Summary")
            header_row = None
            for idx in range(min(10, len(df))):
                row_vals = df.iloc[idx].astype(str).tolist()
                if 'Sales Item' in row_vals and 'Summary' in row_vals:
                    header_row = idx
                    break
            
            if header_row is None:
                logger.error("Could not find header row with 'Sales Item' and 'Summary'")
                return []
            
            logger.info(f"Header row found at index: {header_row}")
            
            # Extract account data (starts after header row + 2 date/time rows)
            records = []
            for idx in range(header_row + 3, len(df)):
                row = df.iloc[idx]
                
                # Column 0 is Sales Item (account name)
                # Column 1 is Summary (amount)
                sales_item = row.iloc[0]
                summary = row.iloc[1]
                
                # Skip if sales item is empty or NaN
                if pd.isna(sales_item) or str(sales_item).strip() == '' or str(sales_item) == 'nan':
                    continue
                
                # Skip special rows
                sales_item_str = str(sales_item).strip()
                if sales_item_str in ['Total of Above', '- OVER-RINGS', '= Adjusted Sales']:
                    continue
                
                # Convert summary to float
                try:
                    amount = float(summary) if pd.notna(summary) else 0.0
                except:
                    amount = 0.0
                
                # Create record with ORIGINAL data (for Supabase)
                record = {
                    'store_number': store_number,
                    'store_name': store_name,
                    'legal_entity': legal_entity,
                    'class_code': class_code,
                    'week_ending': week_ending,
                    'sales_item': sales_item_str,  # Original WSR name
                    'amount': amount,              # Original amount (not adjusted)
                    'description': f"{week_ending} WSR Entry",
                    'created_at': datetime.now().isoformat()
                }
                
                records.append(record)
            
            logger.info(f"Extracted {len(records)} account records")
            return records
            
        except Exception as e:
            logger.error(f"Failed to parse file: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    def upload_to_supabase(self, records: List[Dict]):
        """Upload WSR records to Supabase services_wsr table"""
        if not self.supabase:
            logger.warning("Supabase not configured, skipping upload")
            return
        
        logger.info(f"\n{'='*80}")
        logger.info(f"Uploading {len(records)} WSR records to Supabase")
        
        try:
            total_uploaded = 0
            for i in range(0, len(records), self.batch_size):
                batch = records[i:i + self.batch_size]
                
                response = self.supabase.table('services_wsr').insert(batch).execute()
                
                total_uploaded += len(batch)
                logger.info(f"Uploaded batch: {total_uploaded}/{len(records)} records")
            
            logger.info(f"✓ Successfully uploaded {total_uploaded} records to Supabase")
            
        except Exception as e:
            logger.error(f"Failed to upload to Supabase: {e}")
            import traceback
            traceback.print_exc()
    
    # ========================================================================
    # GOOGLE SHEETS TAB CREATION
    # ========================================================================
    
    def create_google_sheets_tabs(self, records: List[Dict], week_ending: str = None):
        """Create Google Sheets tabs by Legal Entity AND Week Ending"""
        if not self.sheets_service:
            logger.warning("Google Sheets not configured, skipping tab creation")
            return
        
        logger.info(f"\n{'='*80}")
        logger.info(f"Creating Google Sheets tabs")
        
        try:
            # Group records by legal entity AND week ending
            by_entity_week = {}
            for record in records:
                entity = record['legal_entity']
                week = record['week_ending']
                key = f"{entity}|{week}"
                
                if key not in by_entity_week:
                    by_entity_week[key] = []
                by_entity_week[key].append(record)
            
            logger.info(f"Found {len(by_entity_week)} legal entity + week combinations")
            
            for key, entity_records in by_entity_week.items():
                entity, week = key.split('|')
                tab_name = f"{entity} {week}"
                logger.info(f"\nCreating tab: {tab_name}")
                self.create_sheet_tab(tab_name, entity_records)
            
            logger.info(f"✓ Successfully created {len(by_entity_week)} tabs")
            
        except Exception as e:
            logger.error(f"Failed to create Google Sheets tabs: {e}")
            import traceback
            traceback.print_exc()
    
    def create_sheet_tab(self, tab_name: str, records: List[Dict]):
        """Create a single tab in Google Sheets with QBO account mapping"""
        try:
            # Check if tab exists
            spreadsheet = self.sheets_service.spreadsheets().get(
                spreadsheetId=self.spreadsheet_id
            ).execute()
            
            sheet_id = None
            sheets_list = spreadsheet.get('sheets', [])
            
            for sheet in sheets_list:
                if sheet['properties']['title'] == tab_name:
                    sheet_id = sheet['properties']['sheetId']
                    logger.info(f"Tab '{tab_name}' already exists, will clear and update")
                    break
            
            # Create tab if it doesn't exist - ADD TO THE LEFT (index 0)
            if sheet_id is None:
                request = {
                    'requests': [{
                        'addSheet': {
                            'properties': {
                                'title': tab_name,
                                'index': 0
                            }
                        }
                    }]
                }
                response = self.sheets_service.spreadsheets().batchUpdate(
                    spreadsheetId=self.spreadsheet_id,
                    body=request
                ).execute()
                sheet_id = response['replies'][0]['addSheet']['properties']['sheetId']
                logger.info(f"Created new tab: {tab_name} (added to left)")
            else:
                # Clear existing data
                clear_request = {
                    'requests': [{
                        'updateCells': {
                            'range': {
                                'sheetId': sheet_id
                            },
                            'fields': 'userEnteredValue'
                        }
                    }]
                }
                self.sheets_service.spreadsheets().batchUpdate(
                    spreadsheetId=self.spreadsheet_id,
                    body=clear_request
                ).execute()
            
            # Headers: Account | Amount | Journal Date | Description | Name | Class
            header_row = [['Account', 'Amount', 'Journal Date', 'Description', 'Name', 'Class']]
            
            data_rows = []
            skipped_count = 0
            
            for record in records:
                sales_item = record['sales_item']
                amount = record['amount']
                
                # Try to find mapping
                mapping_info = None
                
                # First try exact match
                if sales_item in self.account_mapping:
                    mapping_info = self.account_mapping[sales_item]
                else:
                    # Try stripping common prefixes: "- ", "+ ", "= "
                    stripped_item = sales_item.lstrip('- ').lstrip('+ ').lstrip('= ')
                    if stripped_item in self.account_mapping:
                        mapping_info = self.account_mapping[stripped_item]
                        if len(data_rows) < 3:
                            logger.info(f"  Matched '{sales_item}' using stripped name '{stripped_item}'")
                
                # Skip if no mapping exists (ONLY for Google Sheets)
                if not mapping_info:
                    skipped_count += 1
                    continue
                
                qbo_account = mapping_info['qbo_account']
                debit_credit = mapping_info['debit_credit']
                name = mapping_info.get('name', '')
                
                # DEBUG: Log first few transformations
                if len(data_rows) < 3:
                    logger.info(f"  DEBUG: {sales_item}")
                    logger.info(f"    Original amount: {amount}")
                    logger.info(f"    Debit/Credit/Reverse: '{debit_credit}' (lower: '{debit_credit.lower()}')")
                
                # Apply debit/credit/reverse logic
                adjusted_amount = amount
                
                if debit_credit.lower() == 'reverse':
                    adjusted_amount = -amount
                    if len(data_rows) < 3:
                        logger.info(f"    APPLIED REVERSE LOGIC: {amount} -> {adjusted_amount}")
                elif debit_credit.lower() == 'debit' and amount > 0:
                    adjusted_amount = -amount
                    if len(data_rows) < 3:
                        logger.info(f"    APPLIED DEBIT LOGIC: {amount} -> {adjusted_amount}")
                elif debit_credit.lower() == 'credit' and amount < 0:
                    adjusted_amount = -amount
                    if len(data_rows) < 3:
                        logger.info(f"    APPLIED CREDIT LOGIC: {amount} -> {adjusted_amount}")
                else:
                    if len(data_rows) < 3:
                        logger.info(f"    NO CHANGE: {amount} -> {adjusted_amount}")
                
                data_rows.append([
                    qbo_account,               # A: Account
                    adjusted_amount,           # B: Amount (adjusted)
                    record['week_ending'],     # C: Journal Date
                    record['description'],     # D: Description
                    name,                      # E: Name
                    record['class_code']       # F: Class
                ])
            
            if skipped_count > 0:
                logger.info(f"  Skipped {skipped_count} unmapped account(s)")
            
            all_rows = header_row + data_rows
            
            body = {
                'values': all_rows
            }
            
            self.sheets_service.spreadsheets().values().update(
                spreadsheetId=self.spreadsheet_id,
                range=f"{tab_name}!A1",
                valueInputOption='RAW',
                body=body
            ).execute()
            
            logger.info(f"✓ Wrote {len(data_rows)} rows to tab '{tab_name}'")
            
            # Format the header row (green background, white text, bold)
            format_request = {
                'requests': [{
                    'repeatCell': {
                        'range': {
                            'sheetId': sheet_id,
                            'startRowIndex': 0,
                            'endRowIndex': 1
                        },
                        'cell': {
                            'userEnteredFormat': {
                                'backgroundColor': {'red': 0.2, 'green': 0.6, 'blue': 0.2},
                                'textFormat': {
                                    'foregroundColor': {'red': 1, 'green': 1, 'blue': 1},
                                    'bold': True
                                }
                            }
                        },
                        'fields': 'userEnteredFormat(backgroundColor,textFormat)'
                    }
                }]
            }
            self.sheets_service.spreadsheets().batchUpdate(
                spreadsheetId=self.spreadsheet_id,
                body=format_request
            ).execute()
            
        except Exception as e:
            logger.error(f"Failed to create/update tab '{tab_name}': {e}")
            import traceback
            traceback.print_exc()
    
    # ========================================================================
    # PART 2: WEEKLY SALES EXPECTED DEPOSITS PARSING
    # ========================================================================
    
    def parse_weekly_sales_xlsx(self, path: str) -> List[Dict[str, Any]]:
        """Parse Weekly Sales sheet from .xlsx file for Expected Deposits"""
        logger.info(f"Parsing Weekly Sales (xlsx): {os.path.basename(path)}")
        
        wb = openpyxl.load_workbook(path, data_only=True)
        if "Weekly Sales" not in wb.sheetnames:
            logger.warning(f'"Weekly Sales" sheet not found in {path}. Sheets: {wb.sheetnames}')
            return []
        
        ws = wb["Weekly Sales"]
        
        store_number = str(ws.cell(4, 3).value).strip()   # row 4 col C
        week_ending = _to_iso_date(ws.cell(2, 3).value)   # row 2 col C
        EXPECTED_ROW = 60
        
        logger.info(f"  Store: {store_number}, Week Ending: {week_ending}")
        
        out: List[Dict[str, Any]] = []
        
        for c in range(4, 18, 2):  # D..Q, step 2 (7 days)
            date = _to_iso_date(ws.cell(9, c).value)      # row 9
            if not date:
                logger.warning(f"  No date found in column {c}, skipping")
                continue
            
            am = _to_float(ws.cell(EXPECTED_ROW, c).value)
            pm = _to_float(ws.cell(EXPECTED_ROW, c + 1).value)
            total = None if (am is None or pm is None) else am + pm
            
            out.append({
                "store_number": store_number,
                "week_ending": week_ending,
                "date": date,
                "am_expected": am,
                "pm_expected": pm,
                "expected_deposit": total,
                "source_file": os.path.basename(path),
            })
        
        if len(out) != 7:
            logger.warning(f"Expected 7 day rows, got {len(out)} for {path}")
        else:
            logger.info(f"  ✓ Extracted {len(out)} days of expected deposits")
        
        return out
    
    def parse_weekly_sales_xls(self, path: str) -> List[Dict[str, Any]]:
        """Parse Weekly Sales sheet from .xls file for Expected Deposits"""
        logger.info(f"Parsing Weekly Sales (xls): {os.path.basename(path)}")
        
        df = pd.read_excel(path, sheet_name="Weekly Sales", header=None, engine="xlrd")
        
        store_number = str(df.iat[3, 2]).strip()   # row 4 col C => (3,2)
        week_ending = _to_iso_date(df.iat[1, 2])   # row 2 col C => (1,2)
        
        logger.info(f"  Store: {store_number}, Week Ending: {week_ending}")
        
        EXPECTED_ROW = 59  # row 60 => index 59
        out: List[Dict[str, Any]] = []
        
        for col_excel_1based in range(4, 18, 2):
            c0 = col_excel_1based - 1  # 0-based col
            date = _to_iso_date(df.iat[8, c0])  # row 9 => index 8
            if not date:
                logger.warning(f"  No date found in column {col_excel_1based}, skipping")
                continue
            
            am = _to_float(df.iat[EXPECTED_ROW, c0])
            pm = _to_float(df.iat[EXPECTED_ROW, c0 + 1])
            total = None if (am is None or pm is None) else am + pm
            
            out.append({
                "store_number": store_number,
                "week_ending": week_ending,
                "date": date,
                "am_expected": am,
                "pm_expected": pm,
                "expected_deposit": total,
                "source_file": os.path.basename(path),
            })
        
        if len(out) != 7:
            logger.warning(f"Expected 7 day rows, got {len(out)} for {path}")
        else:
            logger.info(f"  ✓ Extracted {len(out)} days of expected deposits")
        
        return out
    
    def parse_weekly_sales_file(self, path: str) -> List[Dict[str, Any]]:
        """Parse Weekly Sales file - auto-detect .xlsx or .xls"""
        lower = path.lower()
        try:
            if lower.endswith(".xlsx"):
                return self.parse_weekly_sales_xlsx(path)
            elif lower.endswith(".xls"):
                return self.parse_weekly_sales_xls(path)
            else:
                logger.warning(f"Unsupported file type for Weekly Sales: {path}")
                return []
        except Exception as e:
            logger.error(f"Failed to parse Weekly Sales from {path}: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    def upload_expected_deposits_to_supabase(self, records: List[Dict[str, Any]]):
        """Upload Expected Deposits to Supabase table"""
        if not self.supabase:
            logger.warning("Supabase not configured - skipping expected deposits upload")
            return
        
        if not records:
            logger.warning("No expected deposits records to upload")
            return
        
        logger.info(f"\n{'='*80}")
        logger.info(f"UPLOADING EXPECTED DEPOSITS TO SUPABASE")
        logger.info(f"Table: {self.expected_deposits_table}")
        logger.info(f"Total records: {len(records)}")
        logger.info(f"{'='*80}")
        
        try:
            batch_size = self.batch_size
            total_batches = (len(records) + batch_size - 1) // batch_size
            
            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]
                batch_num = (i // batch_size) + 1
                
                logger.info(f"Uploading batch {batch_num}/{total_batches} ({len(batch)} records)...")
                
                response = self.supabase.table(self.expected_deposits_table).upsert(
                    batch,
                    on_conflict='store_number,date'
                ).execute()
                
                logger.info(f"  ✓ Batch {batch_num} uploaded successfully")
            
            logger.info(f"\n✓ All {len(records)} expected deposits records uploaded to Supabase")
            
            # Show sample
            logger.info(f"\nSample records (first 3):")
            for i, record in enumerate(records[:3]):
                logger.info(f"  {i+1}. Store {record['store_number']}, {record['date']}: "
                          f"AM=${record['am_expected']:.2f}, PM=${record['pm_expected']:.2f}, "
                          f"Total=${record['expected_deposit']:.2f}")
        
        except Exception as e:
            logger.error(f"Failed to upload expected deposits to Supabase: {e}")
            import traceback
            traceback.print_exc()
    
    def process_weekly_sales_files(self, directory: str) -> List[Dict[str, Any]]:
        """Find and process all Weekly Sales files in directory for Expected Deposits"""
        if not os.path.exists(directory):
            logger.error(f"Directory not found: {directory}")
            return []
        
        logger.info(f"\n{'='*80}")
        logger.info(f"SCANNING FOR WEEKLY SALES FILES")
        logger.info(f"Directory: {directory}")
        logger.info(f"{'='*80}")
        
        all_files = [f for f in os.listdir(directory) 
                     if (f.endswith('.xls') or f.endswith('.xlsx')) 
                     and not f.startswith('~$')]
        
        logger.info(f"\nFound {len(all_files)} Excel file(s) total")
        
        all_records = []
        parsed_count = 0
        
        for filename in all_files:
            filepath = os.path.join(directory, filename)
            records = self.parse_weekly_sales_file(filepath)
            
            if records:
                all_records.extend(records)
                parsed_count += 1
        
        logger.info(f"\n{'='*80}")
        logger.info(f"WEEKLY SALES PARSING COMPLETE")
        logger.info(f"{'='*80}")
        logger.info(f"Files with Weekly Sales sheet: {parsed_count}/{len(all_files)}")
        logger.info(f"Total expected deposits records: {len(all_records)}")
        
        if all_records:
            unique_stores = set(r['store_number'] for r in all_records)
            unique_weeks = set(r['week_ending'] for r in all_records)
            logger.info(f"Stores found: {', '.join(sorted(unique_stores))}")
            logger.info(f"Week endings: {', '.join(sorted(unique_weeks))}")
        
        return all_records


# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """Main entry point - processes BOTH WSR and Weekly Sales files
    
    Usage:
        python wsr_parser.py                  # Interactive mode (prompts for confirmation)
        python wsr_parser.py --auto-confirm   # CI mode (skips all prompts)
    """
    # Check for auto-confirm flag (for CI / GitHub Actions)
    auto_confirm = '--auto-confirm' in sys.argv
    
    parser = WSRParser()
    
    # Get directories
    download_dir = os.getenv('PROCESSED_DIR', './processed')
    
    if not os.path.exists(download_dir):
        logger.error(f"Directory not found: {download_dir}")
        return
    
    # ========================================================================
    # PART 1: PROCESS WSR FILES (line-item data -> Supabase + Google Sheets)
    # ========================================================================
    logger.info(f"\n{'='*80}")
    logger.info(f"PART 1: WSR PROCESSING")
    logger.info(f"Scanning for WSR files in: {download_dir}")
    logger.info(f"{'='*80}")
    
    # Extract ZIP files first
    parser.extract_zip_files(download_dir)
    
    # Find all Excel files
    wsr_files = [f for f in os.listdir(download_dir) 
                 if (f.endswith('.xls') or f.endswith('.xlsx')) 
                 and not f.startswith('~$')]
    
    if wsr_files:
        logger.info(f"\nFound {len(wsr_files)} Excel file(s):")
        for f in wsr_files:
            logger.info(f"  - {f}")
        
        # Confirm WSR processing
        proceed = auto_confirm
        if not proceed:
            print(f"\nProcess {len(wsr_files)} file(s) for WSR data? (y/n): ", end='')
            proceed = input().lower() == 'y'
        
        if proceed:
            all_wsr_records = []
            for wsr_file in wsr_files:
                filepath = os.path.join(download_dir, wsr_file)
                records = parser.parse_wsr_file(filepath)
                if records:
                    all_wsr_records.extend(records)
            
            if all_wsr_records:
                logger.info(f"\nTotal WSR records extracted: {len(all_wsr_records)}")
                
                unique_weeks = set(r['week_ending'] for r in all_wsr_records)
                logger.info(f"Week endings found: {', '.join(sorted(unique_weeks))}")
                
                # Upload to Supabase
                logger.info("\nUploading WSR data to Supabase...")
                parser.upload_to_supabase(all_wsr_records)
                
                # Create Google Sheets tabs
                logger.info("\nCreating Google Sheets tabs for WSR data...")
                parser.create_google_sheets_tabs(all_wsr_records)
            else:
                logger.warning("No WSR records extracted from any files")
        else:
            logger.info("WSR processing skipped by user")
    else:
        logger.info("No Excel files found, skipping WSR processing")
    
    # ========================================================================
    # PART 2: PROCESS WEEKLY SALES EXPECTED DEPOSITS
    # ========================================================================
    logger.info(f"\n{'='*80}")
    logger.info(f"PART 2: WEEKLY SALES EXPECTED DEPOSITS")
    logger.info(f"{'='*80}")
    
    expected_deposits = parser.process_weekly_sales_files(download_dir)
    
    if expected_deposits:
        # Confirm upload
        proceed = auto_confirm
        if not proceed:
            print(f"\nUpload {len(expected_deposits)} expected deposits records to Supabase? (y/n): ", end='')
            proceed = input().lower() == 'y'
        
        if proceed:
            parser.upload_expected_deposits_to_supabase(expected_deposits)
        else:
            logger.info("Expected deposits upload skipped by user")
        
        # Always export to CSV
        csv_path = os.path.join(download_dir, 'weekly_expected_deposits.csv')
        df = pd.DataFrame(expected_deposits).sort_values(['store_number', 'date'])
        df.to_csv(csv_path, index=False)
        logger.info(f"\n✓ Exported to CSV: {csv_path}")
    else:
        logger.info("No Weekly Sales files found or no data extracted")
    
    # ========================================================================
    # SUMMARY
    # ========================================================================
    logger.info(f"\n{'='*80}")
    logger.info(f"ALL PROCESSING COMPLETE!")
    logger.info(f"{'='*80}")


if __name__ == "__main__":
    main()

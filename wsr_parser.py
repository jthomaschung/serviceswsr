"""
WSR Parser - ENHANCED VERSION
Processes Jimmy John's Weekly Sales Reports AND Expected Deposits
Uploads to Supabase and creates Google Sheets tabs by Legal Entity
"""

import os
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
    # EXISTING WSR METHODS (abbreviated - keep all your existing methods)
    # ========================================================================
    
    def load_account_mapping(self) -> Dict:
        """Load WSR to QBO account mapping from Google Sheets 'Key' tab"""
        # ... KEEP YOUR EXISTING CODE ...
        return {}
    
    def load_store_mapping(self) -> Dict:
        """Load store to legal entity mapping"""
        # ... KEEP YOUR EXISTING CODE ...
        return {}
    
    def extract_zip_files(self, directory: str):
        """Extract all ZIP files in directory"""
        # ... KEEP YOUR EXISTING CODE ...
        pass
    
    def parse_wsr_file(self, filepath: str) -> List[Dict]:
        """Parse a single WSR file"""
        # ... KEEP YOUR EXISTING CODE ...
        return []
    
    def upload_to_supabase(self, records: List[Dict]):
        """Upload WSR records to Supabase"""
        # ... KEEP YOUR EXISTING CODE ...
        pass
    
    def create_google_sheets_tabs(self, records: List[Dict]):
        """Create Google Sheets tabs by entity and week"""
        # ... KEEP YOUR EXISTING CODE ...
        pass
    
    # ========================================================================
    # NEW: WEEKLY SALES EXPECTED DEPOSITS PARSING
    # ========================================================================
    
    def parse_weekly_sales_xlsx(self, path: str) -> List[Dict[str, Any]]:
        """Parse Weekly Sales sheet from .xlsx file for Expected Deposits"""
        logger.info(f"Parsing Weekly Sales (xlsx): {os.path.basename(path)}")
        
        wb = openpyxl.load_workbook(path, data_only=True)
        if "Weekly Sales" not in wb.sheetnames:
            logger.warning(f'"Weekly Sales" sheet not found in {path}. Sheets: {wb.sheetnames}')
            return []
        
        ws = wb["Weekly Sales"]
        
        # Extract metadata
        store_number = str(ws.cell(4, 3).value).strip()   # row 4 col C
        week_ending = _to_iso_date(ws.cell(2, 3).value)   # row 2 col C
        EXPECTED_ROW = 60
        
        logger.info(f"  Store: {store_number}, Week Ending: {week_ending}")
        
        out: List[Dict[str, Any]] = []
        
        # Parse 7 days: columns D, F, H, J, L, N, P (4, 6, 8, 10, 12, 14, 16)
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
        
        # Read raw grid (no header) so we can use absolute row/col positions
        df = pd.read_excel(path, sheet_name="Weekly Sales", header=None, engine="xlrd")
        
        # Convert Excel-style 1-based row/col to 0-based for pandas
        store_number = str(df.iat[3, 2]).strip()   # row 4 col C => (3,2)
        week_ending = _to_iso_date(df.iat[1, 2])   # row 2 col C => (1,2)
        
        logger.info(f"  Store: {store_number}, Week Ending: {week_ending}")
        
        EXPECTED_ROW = 59  # row 60 => index 59
        out: List[Dict[str, Any]] = []
        
        # Columns D..Q => 4..17 in Excel 1-based; 0-based => 3..16
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
            # Upload in batches
            batch_size = self.batch_size
            total_batches = (len(records) + batch_size - 1) // batch_size
            
            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]
                batch_num = (i // batch_size) + 1
                
                logger.info(f"Uploading batch {batch_num}/{total_batches} ({len(batch)} records)...")
                
                # Use upsert to handle duplicates based on (store_number, date)
                response = self.supabase.table(self.expected_deposits_table).upsert(
                    batch,
                    on_conflict='store_number,date'
                ).execute()
                
                logger.info(f"  ✓ Batch {batch_num} uploaded successfully")
            
            logger.info(f"\n✓ All {len(records)} expected deposits records uploaded to Supabase")
            
            # Show sample of what was uploaded
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
        """Find and process all Weekly Sales files in directory"""
        if not os.path.exists(directory):
            logger.error(f"Directory not found: {directory}")
            return []
        
        logger.info(f"\n{'='*80}")
        logger.info(f"SCANNING FOR WEEKLY SALES FILES")
        logger.info(f"Directory: {directory}")
        logger.info(f"{'='*80}")
        
        # Find all Excel files
        all_files = [f for f in os.listdir(directory) 
                     if (f.endswith('.xls') or f.endswith('.xlsx')) 
                     and not f.startswith('~$')]
        
        logger.info(f"\nFound {len(all_files)} Excel file(s) total")
        
        # Try to parse each file for Weekly Sales sheet
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
            # Show unique stores and weeks
            unique_stores = set(r['store_number'] for r in all_records)
            unique_weeks = set(r['week_ending'] for r in all_records)
            logger.info(f"Stores found: {', '.join(sorted(unique_stores))}")
            logger.info(f"Week endings: {', '.join(sorted(unique_weeks))}")
        
        return all_records


# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """Main entry point - processes BOTH WSR and Weekly Sales files"""
    parser = WSRParser()
    
    # Get directories
    download_dir = os.getenv('PROCESSED_DIR', './processed')
    
    if not os.path.exists(download_dir):
        logger.error(f"Directory not found: {download_dir}")
        return
    
    # ========================================================================
    # PART 1: PROCESS WSR FILES (existing logic)
    # ========================================================================
    logger.info(f"\n{'='*80}")
    logger.info(f"PART 1: WSR PROCESSING")
    logger.info(f"Scanning for WSR files in: {download_dir}")
    logger.info(f"{'='*80}")
    
    # Extract ZIP files
    parser.extract_zip_files(download_dir)
    
    # Find WSR files
    wsr_files = [f for f in os.listdir(download_dir) 
                 if (f.endswith('.xls') or f.endswith('.xlsx')) 
                 and not f.startswith('~$')
                 and 'WSR' in f.upper()]  # Filter for WSR files
    
    if wsr_files:
        logger.info(f"\nFound {len(wsr_files)} WSR file(s):")
        for f in wsr_files:
            logger.info(f"  - {f}")
        
        # Ask user to confirm WSR processing
        print(f"\nProcess {len(wsr_files)} WSR file(s)? (y/n): ", end='')
        if input().lower() == 'y':
            # Process WSR files
            all_wsr_records = []
            for wsr_file in wsr_files:
                filepath = os.path.join(download_dir, wsr_file)
                records = parser.parse_wsr_file(filepath)
                if records:
                    all_wsr_records.extend(records)
            
            if all_wsr_records:
                logger.info(f"\nTotal WSR records extracted: {len(all_wsr_records)}")
                
                # Upload to Supabase
                logger.info("\nUploading WSR data to Supabase...")
                parser.upload_to_supabase(all_wsr_records)
                
                # Create Google Sheets tabs
                logger.info("\nCreating Google Sheets tabs for WSR data...")
                parser.create_google_sheets_tabs(all_wsr_records)
        else:
            logger.info("WSR processing skipped by user")
    else:
        logger.info("No WSR files found, skipping WSR processing")
    
    # ========================================================================
    # PART 2: PROCESS WEEKLY SALES EXPECTED DEPOSITS (new logic)
    # ========================================================================
    logger.info(f"\n{'='*80}")
    logger.info(f"PART 2: WEEKLY SALES EXPECTED DEPOSITS")
    logger.info(f"{'='*80}")
    
    # Process all Weekly Sales files
    expected_deposits = parser.process_weekly_sales_files(download_dir)
    
    if expected_deposits:
        # Ask user to confirm upload
        print(f"\nUpload {len(expected_deposits)} expected deposits records to Supabase? (y/n): ", end='')
        if input().lower() == 'y':
            parser.upload_expected_deposits_to_supabase(expected_deposits)
        else:
            logger.info("Expected deposits upload skipped by user")
        
        # Optionally export to CSV
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

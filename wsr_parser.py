"""
WSR Parser - ENHANCED VERSION with Parse Audit & Supabase Verification
Processes Jimmy John's Weekly Sales Reports AND Expected Deposits
Uploads to Supabase and creates Google Sheets tabs by Legal Entity
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
from typing import List, Dict, Any, Optional, Set
import re
import zipfile
import shutil
import openpyxl

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from supabase import create_client, Client

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('wsr_parser.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Full expected store list — keep in sync with jj_wsr_bot.py
EXPECTED_STORES: Set[int] = {
    522, 746, 799, 833, 838, 877, 930, 965, 1002, 1018, 1019, 1061, 1111,
    1127, 1206, 1261, 1307, 1337, 1342, 1355, 1440, 1441, 1554, 1556, 1562,
    1635, 1694, 1695, 1696, 1762, 1779, 1789, 1955, 1956, 1957, 2006, 2021,
    2176, 2178, 2180, 2391, 2500, 2501, 2502, 2503, 2504, 2601, 2682, 2683,
    2711, 2712, 2749, 2807, 2808, 2811, 2812, 2821, 2873, 2874, 2876, 2883,
    2884, 3029, 3030, 3187, 3260, 3391, 3612, 3613, 3635, 3686, 3972, 4018,
    4022, 4024, 4105, 4330, 4358, 4586,
}

MAX_PARSE_RETRIES = 2   # How many times to retry parsing a failing file


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def _to_iso_date(x: Any) -> Optional[str]:
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
        m = re.match(r"^(\d{1,2})/(\d{1,2})/(\d{4})$", s)
        if m:
            mm, dd, yyyy = m.groups()
            return f"{yyyy}-{int(mm):02d}-{int(dd):02d}"
        m2 = re.match(r"^\d{4}-\d{2}-\d{2}$", s)
        if m2:
            return s
        return s
    return str(x).strip()


def _to_float(x: Any) -> Optional[float]:
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

    def __init__(self):
        load_dotenv()

        self.supabase_url = os.getenv('SUPABASE_URL')
        self.supabase_key = os.getenv('SUPABASE_SERVICE_ROLE_KEY')

        if self.supabase_url and self.supabase_key:
            self.supabase = create_client(self.supabase_url, self.supabase_key)
            logger.info("Supabase client initialized")
        else:
            self.supabase = None
            logger.warning("Supabase credentials not found - will skip upload")

        self.spreadsheet_id = os.getenv('GOOGLE_SHEET_ID')
        self.credentials_path = os.getenv('GOOGLE_CREDENTIALS_PATH')

        if self.spreadsheet_id and self.credentials_path and os.path.exists(self.credentials_path):
            try:
                scopes = ['https://www.googleapis.com/auth/spreadsheets']
                creds = Credentials.from_service_account_file(self.credentials_path, scopes=scopes)
                self.sheets_service = build('sheets', 'v4', credentials=creds)
                logger.info("Google Sheets API initialized")
            except Exception as e:
                logger.error(f"Failed to initialize Google Sheets: {e}")
                self.sheets_service = None
        else:
            self.sheets_service = None
            logger.warning("Google Sheets not configured")

        self.store_mapping = self.load_store_mapping()
        self.batch_size = 1000
        self.account_mapping = self.load_account_mapping()
        self.expected_deposits_table = os.getenv('EXPECTED_DEPOSITS_TABLE', 'weekly_sales_expected')

    # ========================================================================
    # AUDIT & VERIFICATION
    # ========================================================================

    def audit_parsed_records(self, records: List[Dict], label: str = "WSR") -> Set[int]:
        """
        Compare store numbers in parsed records against EXPECTED_STORES.
        Returns the set of missing store numbers.
        """
        parsed_stores = set(int(r['store_number']) for r in records if r.get('store_number'))
        missing = EXPECTED_STORES - parsed_stores

        logger.info(f"\n{'─'*60}")
        logger.info(f"PARSE AUDIT — {label}")
        logger.info(f"  Expected : {len(EXPECTED_STORES)} stores")
        logger.info(f"  Parsed   : {len(parsed_stores)} stores")

        if missing:
            logger.warning(f"  ⚠️  MISSING {len(missing)} stores: {sorted(missing)}")
        else:
            logger.info(f"  ✓ All stores parsed successfully")

        return missing

    def audit_expected_deposits(self, records: List[Dict]) -> Dict[str, Any]:
        """
        Audit expected deposits records.
        Returns a dict with missing stores and days-per-store issues.
        """
        issues = {"missing_stores": set(), "incomplete_days": {}}

        if not records:
            issues["missing_stores"] = EXPECTED_STORES
            return issues

        from collections import defaultdict
        store_days: Dict[str, Set[str]] = defaultdict(set)
        for r in records:
            sn = str(r.get('store_number', ''))
            d = r.get('date', '')
            if sn and d:
                store_days[sn].add(d)

        parsed_stores = set(int(sn) for sn in store_days.keys() if sn.isdigit())
        issues["missing_stores"] = EXPECTED_STORES - parsed_stores

        for sn, days in store_days.items():
            if len(days) < 7:
                issues["incomplete_days"][sn] = len(days)

        logger.info(f"\n{'─'*60}")
        logger.info(f"EXPECTED DEPOSITS AUDIT")
        logger.info(f"  Stores with data : {len(parsed_stores)}")
        if issues["missing_stores"]:
            logger.warning(f"  ⚠️  Missing stores: {sorted(issues['missing_stores'])}")
        if issues["incomplete_days"]:
            logger.warning(f"  ⚠️  Stores with <7 days: {issues['incomplete_days']}")
        if not issues["missing_stores"] and not issues["incomplete_days"]:
            logger.info(f"  ✓ All stores have 7 days of expected deposits")

        return issues

    def verify_supabase_upload(self, week_endings: List[str]) -> bool:
        """
        Query Supabase after upload to confirm row counts look correct.
        Returns True if all checks pass.
        """
        if not self.supabase:
            logger.warning("Supabase not configured — skipping verification")
            return True

        logger.info(f"\n{'='*60}")
        logger.info("SUPABASE UPLOAD VERIFICATION")
        logger.info(f"{'='*60}")

        all_passed = True

        for week in week_endings:
            logger.info(f"\n  Week: {week}")

            # ── services_wsr ──────────────────────────────────────────────
            try:
                resp = (
                    self.supabase.table('services_wsr')
                    .select('store_number', count='exact')
                    .eq('week_ending', week)
                    .execute()
                )
                total_rows = resp.count or len(resp.data)
                unique_stores = len(set(r['store_number'] for r in resp.data))

                avg = total_rows / unique_stores if unique_stores else 0
                wsr_ok = unique_stores >= len(EXPECTED_STORES) and avg >= 20

                status = "✓" if wsr_ok else "⚠️"
                logger.info(
                    f"  {status} services_wsr      : {total_rows:,} rows, "
                    f"{unique_stores} stores, avg {avg:.1f} rows/store"
                )
                if not wsr_ok:
                    all_passed = False
                    if unique_stores < len(EXPECTED_STORES):
                        logger.warning(f"    Only {unique_stores}/{len(EXPECTED_STORES)} stores in DB")
                    if avg < 20:
                        logger.warning(f"    Low avg rows/store ({avg:.1f}) — possible parse issue")

            except Exception as e:
                logger.error(f"  ✗ services_wsr query failed: {e}")
                all_passed = False

            # ── weekly_sales_expected ─────────────────────────────────────
            try:
                # week_ending in this table may be in YYYY-MM-DD format
                # Try both the raw value and ISO-converted value
                iso_week = week
                if '/' in week:
                    parts = week.split('/')
                    if len(parts) == 3:
                        iso_week = f"{parts[2]}-{int(parts[0]):02d}-{int(parts[1]):02d}"

                resp2 = (
                    self.supabase.table(self.expected_deposits_table)
                    .select('store_number,date', count='exact')
                    .eq('week_ending', iso_week)
                    .execute()
                )
                total_exp = resp2.count or len(resp2.data)
                exp_stores = len(set(str(r['store_number']) for r in resp2.data))
                exp_days = len(set(r['date'] for r in resp2.data))

                exp_ok = exp_stores >= len(EXPECTED_STORES) and total_exp >= len(EXPECTED_STORES) * 7
                status = "✓" if exp_ok else "⚠️"
                logger.info(
                    f"  {status} weekly_sales_expected: {total_exp:,} rows, "
                    f"{exp_stores} stores, {exp_days} distinct days"
                )
                if not exp_ok:
                    all_passed = False
                    if exp_stores < len(EXPECTED_STORES):
                        logger.warning(f"    Only {exp_stores}/{len(EXPECTED_STORES)} stores in expected deposits")
                    if exp_days < 7:
                        logger.warning(f"    Only {exp_days}/7 days covered")

            except Exception as e:
                logger.error(f"  ✗ weekly_sales_expected query failed: {e}")
                all_passed = False

        logger.info(f"\n{'─'*60}")
        if all_passed:
            logger.info("  ✓ All Supabase verification checks passed")
        else:
            logger.error("  ❌ One or more Supabase verification checks FAILED")

        return all_passed

    # ========================================================================
    # STORE & ACCOUNT MAPPING
    # ========================================================================

    def load_account_mapping(self) -> Dict:
        if not self.sheets_service or not self.spreadsheet_id:
            logger.warning("Google Sheets not configured, using WSR names as-is")
            return {}

        try:
            logger.info("Loading account mapping from 'Key' tab...")
            result = self.sheets_service.spreadsheets().values().get(
                spreadsheetId=self.spreadsheet_id,
                range='Key!A:D'
            ).execute()

            values = result.get('values', [])
            if not values:
                logger.warning("Key tab is empty")
                return {}

            mapping = {}
            logger.info("Reading Key tab rows (showing first 10):")

            for idx, row in enumerate(values[1:], start=2):
                if len(row) >= 2:
                    wsr_name = row[0].strip() if row[0] else ''
                    qbo_name = row[1].strip() if len(row) > 1 and row[1] else ''

                    if idx <= 11:
                        logger.info(f"  Row {idx}: {row}")

                    name = ''
                    if len(row) > 2 and row[2]:
                        val = row[2].strip()
                        if val.lower() not in ['debit', 'credit', 'reverse']:
                            name = val

                    debit_credit = 'Debit'
                    if len(row) > 3 and row[3]:
                        debit_credit = row[3].strip()
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
                            logger.info(f"  ✓ {wsr_name} -> {qbo_name} ({debit_credit})")

            logger.info(f"✓ Loaded {len(mapping)} account mappings from Key tab")
            return mapping

        except Exception as e:
            logger.warning(f"Could not load Key tab: {e}")
            return {}

    def load_store_mapping(self) -> Dict:
        mapping = {
            2682: {"legal_entity": "Atlas East", "class_code": "2682 - North Fayatte", "store_name": "North Fayette"},
            2683: {"legal_entity": "Atlas East", "class_code": "2683 - Bridgeville", "store_name": "Bridgeville"},
            2749: {"legal_entity": "Atlas East", "class_code": "2749 - Cannonsburg", "store_name": "Southpointe"},
            3686: {"legal_entity": "Atlas East", "class_code": "3686 - Homestead", "store_name": "Homestead"},
            4586: {"legal_entity": "Atlas East", "class_code": "4586 - Pittsburgh Airport", "store_name": "Pittsburgh Airport"},
            746:  {"legal_entity": "Atlas NGC",  "class_code": "0746 - Burnsville", "store_name": "Burnsville"},
            833:  {"legal_entity": "Atlas NGC",  "class_code": "0833 - Shakopee", "store_name": "Shakopee"},
            1061: {"legal_entity": "Atlas NGC",  "class_code": "1061 - Wayzata", "store_name": "Wayzata"},
            1206: {"legal_entity": "Atlas NGC",  "class_code": "1206 - Savage", "store_name": "Savage"},
            1337: {"legal_entity": "Atlas NGC",  "class_code": "1337 - Carriage", "store_name": "Shakopee II"},
            522:  {"legal_entity": "Atlas 0519", "class_code": "0522 - Warren", "store_name": "Mankato"},
            1342: {"legal_entity": "Atlas 0519", "class_code": "1342 - Western", "store_name": "Fairbault"},
            2021: {"legal_entity": "Atlas 0519", "class_code": "2021 - Holly", "store_name": "Holly"},
            2807: {"legal_entity": "Atlas NGC",  "class_code": "2807 - MacArthur", "store_name": "MacArthur"},
            2808: {"legal_entity": "Atlas NGC",  "class_code": "2808 - Marguerite", "store_name": "Mission Viejo"},
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
            1694: {"legal_entity": "Atlas 0519", "class_code": "1694 - Hayden", "store_name": "Hayden"},
            1695: {"legal_entity": "Atlas 0519", "class_code": "1695 - Cactus", "store_name": "Cactus"},
            2503: {"legal_entity": "Atlas 0519", "class_code": "2503 - Scottsdale", "store_name": "Scottsdale"},
            2504: {"legal_entity": "Atlas 0519", "class_code": "2504 - 90th", "store_name": "90th"},
            2006: {"legal_entity": "Atlas NGC",  "class_code": "2006 - McDowell", "store_name": "Goodyear"},
            2391: {"legal_entity": "Atlas NGC",  "class_code": "2391 - Camelback", "store_name": "W Camelback"},
            2883: {"legal_entity": "Atlas NGC",  "class_code": "2883 - Payson", "store_name": "Payson"},
            1762: {"legal_entity": "Atlas NGC",  "class_code": "1762 - Avondale", "store_name": "Avondale"},
            2884: {"legal_entity": "Atlas NGC",  "class_code": "2884 - Estrella", "store_name": "Estrella"},
            3635: {"legal_entity": "Atlas NGC",  "class_code": "3635 - Buckeye", "store_name": "Buckeye"},
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
            838:  {"legal_entity": "Atlas East", "class_code": "0838 - W Broadway", "store_name": "W Broadway"},
            1111: {"legal_entity": "Atlas East", "class_code": "1111 - E Broadway", "store_name": "E Broadway"},
            2712: {"legal_entity": "Atlas East", "class_code": "2712 - Lake Manawa", "store_name": "Manawa"},
            1261: {"legal_entity": "Atlas East", "class_code": "1261 - S 13th", "store_name": "S 13th"},
            799:  {"legal_entity": "Atlas East", "class_code": "0799 - Farnam", "store_name": "Farnam"},
            877:  {"legal_entity": "Atlas East", "class_code": "0877 - Harlan", "store_name": "Harlan"},
            1018: {"legal_entity": "Atlas East", "class_code": "1018 - Twin Creek", "store_name": "Twin Creek"},
            1019: {"legal_entity": "Atlas East", "class_code": "1019 - Giles", "store_name": "Giles"},
            1779: {"legal_entity": "Atlas East", "class_code": "1779 - Shadow Lake", "store_name": "Midlands"},
            2601: {"legal_entity": "Atlas East", "class_code": "2601 - L Street", "store_name": "L Street"},
            2711: {"legal_entity": "Atlas East", "class_code": "2711 - Gretna", "store_name": "Gretna"},
            965:  {"legal_entity": "Atlas East", "class_code": "0965 - Sorenson", "store_name": "Sorenson"},
            1002: {"legal_entity": "Atlas East", "class_code": "1002 - Irvington", "store_name": "Irvington"},
            1355: {"legal_entity": "Atlas East", "class_code": "1355 - N 30th", "store_name": "N 30th"},
            4330: {"legal_entity": "Atlas East", "class_code": "4330 - Blair", "store_name": "Blair"},
            930:  {"legal_entity": "Atlas East", "class_code": "0930 - Elkhorn", "store_name": "Elkhorn"},
            4358: {"legal_entity": "Atlas East", "class_code": "4358 - Indian Creek", "store_name": "Elkhorn"},
        }
        logger.info(f"Loaded mapping for {len(mapping)} stores")
        return mapping

    # ========================================================================
    # ZIP EXTRACTION
    # ========================================================================

    def extract_zip_files(self, directory: str) -> List[str]:
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

    def parse_wsr_file(self, filepath: str, attempt: int = 1) -> List[Dict]:
        """Parse a single WSR file with retry wrapper"""
        logger.info(f"\n{'='*80}")
        logger.info(f"Parsing WSR file (attempt {attempt}): {os.path.basename(filepath)}")

        try:
            df = pd.read_excel(filepath, sheet_name='Weekly Sales')

            week_ending = None
            store_number = None

            if df.shape[0] > 0:
                week_text = str(df.iloc[0, 2])
                if pd.notna(week_text) and week_text != 'nan':
                    try:
                        week_ending = pd.to_datetime(week_text).strftime('%m/%d/%Y')
                    except:
                        logger.warning(f"Could not parse week ending date: {week_text}")

            if df.shape[0] > 2:
                store_text = str(df.iloc[2, 2])
                if pd.notna(store_text) and store_text != 'nan':
                    try:
                        store_number = int(float(store_text))
                    except:
                        logger.warning(f"Could not parse store number: {store_text}")

            if not week_ending or not store_number:
                logger.error(f"Missing metadata: week_ending={week_ending}, store_number={store_number}")
                return []

            logger.info(f"Week Ending: {week_ending} | Store: {store_number}")

            store_info = self.store_mapping.get(store_number)
            if not store_info:
                logger.warning(f"Store {store_number} not in mapping")
                legal_entity = "Unknown"
                class_code = f"{store_number} - Unknown"
                store_name = f"Store {store_number}"
            else:
                legal_entity = store_info['legal_entity']
                class_code = store_info['class_code']
                store_name = store_info['store_name']

            header_row = None
            for idx in range(min(10, len(df))):
                row_vals = df.iloc[idx].astype(str).tolist()
                if 'Sales Item' in row_vals and 'Summary' in row_vals:
                    header_row = idx
                    break

            if header_row is None:
                logger.error("Could not find header row")
                return []

            records = []
            for idx in range(header_row + 3, len(df)):
                row = df.iloc[idx]
                sales_item = row.iloc[0]
                summary = row.iloc[1]

                if pd.isna(sales_item) or str(sales_item).strip() == '' or str(sales_item) == 'nan':
                    continue

                sales_item_str = str(sales_item).strip()
                if sales_item_str in ['Total of Above', '- OVER-RINGS', '= Adjusted Sales']:
                    continue

                try:
                    amount = float(summary) if pd.notna(summary) else 0.0
                except:
                    amount = 0.0

                records.append({
                    'store_number': store_number,
                    'store_name': store_name,
                    'legal_entity': legal_entity,
                    'class_code': class_code,
                    'week_ending': week_ending,
                    'sales_item': sales_item_str,
                    'amount': amount,
                    'description': f"{week_ending} WSR Entry",
                    'created_at': datetime.now().isoformat()
                })

            logger.info(f"Extracted {len(records)} account records")
            return records

        except Exception as e:
            logger.error(f"Failed to parse file (attempt {attempt}): {e}")
            import traceback
            traceback.print_exc()
            return []

    def parse_wsr_file_with_retry(self, filepath: str) -> List[Dict]:
        """Parse with up to MAX_PARSE_RETRIES attempts"""
        for attempt in range(1, MAX_PARSE_RETRIES + 1):
            records = self.parse_wsr_file(filepath, attempt)
            if records:
                return records
            if attempt < MAX_PARSE_RETRIES:
                logger.warning(f"Parse attempt {attempt} returned 0 records, retrying...")
                import time
                time.sleep(2)
        logger.error(f"All {MAX_PARSE_RETRIES} parse attempts failed for {os.path.basename(filepath)}")
        return []

    def upload_to_supabase(self, records: List[Dict]):
        """Upload WSR records to Supabase services_wsr table via upsert"""
        if not self.supabase:
            logger.warning("Supabase not configured, skipping upload")
            return

        logger.info(f"\n{'='*80}")
        logger.info(f"Uploading {len(records)} WSR records to Supabase (upsert)")

        try:
            total_uploaded = 0
            for i in range(0, len(records), self.batch_size):
                batch = records[i:i + self.batch_size]
                self.supabase.table('services_wsr').upsert(
                    batch,
                    on_conflict='store_number,week_ending,sales_item'
                ).execute()
                total_uploaded += len(batch)
                logger.info(f"Upserted batch: {total_uploaded}/{len(records)} records")

            logger.info(f"✓ Successfully upserted {total_uploaded} records")

        except Exception as e:
            logger.error(f"Failed to upload to Supabase: {e}")
            import traceback
            traceback.print_exc()

    # ========================================================================
    # GOOGLE SHEETS TAB CREATION  (unchanged from original)
    # ========================================================================

    def create_google_sheets_tabs(self, records: List[Dict], week_ending: str = None):
        if not self.sheets_service:
            logger.warning("Google Sheets not configured, skipping tab creation")
            return

        logger.info(f"\n{'='*80}")
        logger.info("Creating Google Sheets tabs")

        try:
            by_entity_week = {}
            for record in records:
                key = f"{record['legal_entity']}|{record['week_ending']}"
                if key not in by_entity_week:
                    by_entity_week[key] = []
                by_entity_week[key].append(record)

            for key, entity_records in by_entity_week.items():
                entity, week = key.split('|')
                self.create_sheet_tab(f"{entity} {week}", entity_records)

            logger.info(f"✓ Created {len(by_entity_week)} tabs")

        except Exception as e:
            logger.error(f"Failed to create Google Sheets tabs: {e}")
            import traceback
            traceback.print_exc()

    def create_sheet_tab(self, tab_name: str, records: List[Dict]):
        try:
            spreadsheet = self.sheets_service.spreadsheets().get(
                spreadsheetId=self.spreadsheet_id
            ).execute()

            sheet_id = None
            for sheet in spreadsheet.get('sheets', []):
                if sheet['properties']['title'] == tab_name:
                    sheet_id = sheet['properties']['sheetId']
                    break

            if sheet_id is None:
                response = self.sheets_service.spreadsheets().batchUpdate(
                    spreadsheetId=self.spreadsheet_id,
                    body={'requests': [{'addSheet': {'properties': {'title': tab_name, 'index': 0}}}]}
                ).execute()
                sheet_id = response['replies'][0]['addSheet']['properties']['sheetId']
            else:
                self.sheets_service.spreadsheets().batchUpdate(
                    spreadsheetId=self.spreadsheet_id,
                    body={'requests': [{'updateCells': {'range': {'sheetId': sheet_id}, 'fields': 'userEnteredValue'}}]}
                ).execute()

            header_row = [['Account', 'Amount', 'Journal Date', 'Description', 'Name', 'Class']]
            data_rows = []
            skipped_count = 0

            for record in records:
                sales_item = record['sales_item']
                amount = record['amount']
                mapping_info = self.account_mapping.get(sales_item)

                if not mapping_info:
                    stripped = sales_item.lstrip('- ').lstrip('+ ').lstrip('= ')
                    mapping_info = self.account_mapping.get(stripped)

                if not mapping_info:
                    skipped_count += 1
                    continue

                qbo_account = mapping_info['qbo_account']
                debit_credit = mapping_info['debit_credit']
                name = mapping_info.get('name', '')

                adjusted_amount = amount
                if debit_credit.lower() == 'reverse':
                    adjusted_amount = -amount
                elif debit_credit.lower() == 'debit' and amount > 0:
                    adjusted_amount = -amount
                elif debit_credit.lower() == 'credit' and amount < 0:
                    adjusted_amount = -amount

                data_rows.append([
                    qbo_account, adjusted_amount, record['week_ending'],
                    record['description'], name, record['class_code']
                ])

            if skipped_count > 0:
                logger.info(f"  Skipped {skipped_count} unmapped accounts")

            self.sheets_service.spreadsheets().values().update(
                spreadsheetId=self.spreadsheet_id,
                range=f"{tab_name}!A1",
                valueInputOption='RAW',
                body={'values': header_row + data_rows}
            ).execute()

            self.sheets_service.spreadsheets().batchUpdate(
                spreadsheetId=self.spreadsheet_id,
                body={'requests': [{'repeatCell': {
                    'range': {'sheetId': sheet_id, 'startRowIndex': 0, 'endRowIndex': 1},
                    'cell': {'userEnteredFormat': {
                        'backgroundColor': {'red': 0.2, 'green': 0.6, 'blue': 0.2},
                        'textFormat': {'foregroundColor': {'red': 1, 'green': 1, 'blue': 1}, 'bold': True}
                    }},
                    'fields': 'userEnteredFormat(backgroundColor,textFormat)'
                }}]}
            ).execute()

            logger.info(f"✓ Wrote {len(data_rows)} rows to tab '{tab_name}'")

        except Exception as e:
            logger.error(f"Failed to create/update tab '{tab_name}': {e}")
            import traceback
            traceback.print_exc()

    # ========================================================================
    # PART 2: WEEKLY SALES EXPECTED DEPOSITS
    # ========================================================================

    def parse_weekly_sales_xlsx(self, path: str) -> List[Dict[str, Any]]:
        logger.info(f"Parsing Weekly Sales (xlsx): {os.path.basename(path)}")
        wb = openpyxl.load_workbook(path, data_only=True)
        if "Weekly Sales" not in wb.sheetnames:
            logger.warning(f'"Weekly Sales" not found in {path}')
            return []

        ws = wb["Weekly Sales"]
        store_number = str(ws.cell(4, 3).value).strip()
        week_ending = _to_iso_date(ws.cell(2, 3).value)
        EXPECTED_ROW = 60

        out = []
        for c in range(4, 18, 2):
            date = _to_iso_date(ws.cell(9, c).value)
            if not date:
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
            logger.warning(f"Expected 7 days, got {len(out)}")
        return out

    def parse_weekly_sales_xls(self, path: str) -> List[Dict[str, Any]]:
        logger.info(f"Parsing Weekly Sales (xls): {os.path.basename(path)}")
        df = pd.read_excel(path, sheet_name="Weekly Sales", header=None, engine="xlrd")

        store_number = str(df.iat[3, 2]).strip()
        week_ending = _to_iso_date(df.iat[1, 2])
        EXPECTED_ROW = 59

        out = []
        for col_excel_1based in range(4, 18, 2):
            c0 = col_excel_1based - 1
            date = _to_iso_date(df.iat[8, c0])
            if not date:
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
            logger.warning(f"Expected 7 days, got {len(out)}")
        return out

    def parse_weekly_sales_file(self, path: str) -> List[Dict[str, Any]]:
        lower = path.lower()
        try:
            if lower.endswith(".xlsx"):
                return self.parse_weekly_sales_xlsx(path)
            elif lower.endswith(".xls"):
                return self.parse_weekly_sales_xls(path)
            else:
                logger.warning(f"Unsupported file type: {path}")
                return []
        except Exception as e:
            logger.error(f"Failed to parse Weekly Sales from {path}: {e}")
            import traceback
            traceback.print_exc()
            return []

    def upload_expected_deposits_to_supabase(self, records: List[Dict[str, Any]]):
        if not self.supabase or not records:
            return

        logger.info(f"\n{'='*80}")
        logger.info(f"Uploading {len(records)} expected deposits records (upsert)")

        try:
            for i in range(0, len(records), self.batch_size):
                batch = records[i:i + self.batch_size]
                self.supabase.table(self.expected_deposits_table).upsert(
                    batch,
                    on_conflict='store_number,date'
                ).execute()
                logger.info(f"  Upserted batch {i // self.batch_size + 1}")

            logger.info(f"✓ All {len(records)} expected deposits records uploaded")

        except Exception as e:
            logger.error(f"Failed to upload expected deposits: {e}")
            import traceback
            traceback.print_exc()

    def process_weekly_sales_files(self, directory: str) -> List[Dict[str, Any]]:
        if not os.path.exists(directory):
            logger.error(f"Directory not found: {directory}")
            return []

        all_files = [f for f in os.listdir(directory)
                     if (f.endswith('.xls') or f.endswith('.xlsx'))
                     and not f.startswith('~$')]

        all_records = []
        for filename in all_files:
            records = self.parse_weekly_sales_file(os.path.join(directory, filename))
            if records:
                all_records.extend(records)

        return all_records


# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    auto_confirm = '--auto-confirm' in sys.argv
    parser = WSRParser()
    download_dir = os.getenv('PROCESSED_DIR', './processed')

    if not os.path.exists(download_dir):
        logger.error(f"Directory not found: {download_dir}")
        sys.exit(1)

    overall_success = True

    # ========================================================================
    # PART 1: WSR LINE-ITEM DATA
    # ========================================================================
    logger.info(f"\n{'='*80}")
    logger.info("PART 1: WSR PROCESSING")
    logger.info(f"{'='*80}")

    parser.extract_zip_files(download_dir)

    wsr_files = [f for f in os.listdir(download_dir)
                 if (f.endswith('.xls') or f.endswith('.xlsx'))
                 and not f.startswith('~$')]

    if wsr_files:
        logger.info(f"Found {len(wsr_files)} Excel file(s)")

        proceed = auto_confirm
        if not proceed:
            print(f"\nProcess {len(wsr_files)} file(s) for WSR data? (y/n): ", end='')
            proceed = input().lower() == 'y'

        if proceed:
            # ── Parse all files ───────────────────────────────────────────
            all_wsr_records = []
            failed_files = []

            for wsr_file in wsr_files:
                filepath = os.path.join(download_dir, wsr_file)
                records = parser.parse_wsr_file_with_retry(filepath)
                if records:
                    all_wsr_records.extend(records)
                else:
                    failed_files.append(wsr_file)

            if failed_files:
                logger.warning(f"\n⚠️  {len(failed_files)} file(s) produced zero records after retries:")
                for f in failed_files:
                    logger.warning(f"    {f}")

            # ── Audit parsed records ──────────────────────────────────────
            if all_wsr_records:
                unique_weeks = sorted(set(r['week_ending'] for r in all_wsr_records))
                logger.info(f"\nTotal WSR records: {len(all_wsr_records)}")
                logger.info(f"Weeks found: {', '.join(unique_weeks)}")

                # Audit per week
                for week in unique_weeks:
                    week_records = [r for r in all_wsr_records if r['week_ending'] == week]
                    missing = parser.audit_parsed_records(week_records, label=f"WSR week {week}")
                    if missing:
                        overall_success = False

                # ── Upload ────────────────────────────────────────────────
                parser.upload_to_supabase(all_wsr_records)

                # ── Google Sheets ─────────────────────────────────────────
                parser.create_google_sheets_tabs(all_wsr_records)

                # ── Supabase verification ─────────────────────────────────
                wsr_verified = parser.verify_supabase_upload(unique_weeks)
                if not wsr_verified:
                    overall_success = False

            else:
                logger.warning("No WSR records extracted")
                overall_success = False

    # ========================================================================
    # PART 2: EXPECTED DEPOSITS
    # ========================================================================
    logger.info(f"\n{'='*80}")
    logger.info("PART 2: WEEKLY SALES EXPECTED DEPOSITS")
    logger.info(f"{'='*80}")

    expected_deposits = parser.process_weekly_sales_files(download_dir)

    if expected_deposits:
        # Audit
        exp_issues = parser.audit_expected_deposits(expected_deposits)
        if exp_issues["missing_stores"] or exp_issues["incomplete_days"]:
            overall_success = False

        proceed = auto_confirm
        if not proceed:
            print(f"\nUpload {len(expected_deposits)} expected deposits? (y/n): ", end='')
            proceed = input().lower() == 'y'

        if proceed:
            parser.upload_expected_deposits_to_supabase(expected_deposits)

        # CSV export
        csv_path = os.path.join(download_dir, 'weekly_expected_deposits.csv')
        pd.DataFrame(expected_deposits).sort_values(['store_number', 'date']).to_csv(csv_path, index=False)
        logger.info(f"✓ Exported CSV: {csv_path}")

    else:
        logger.warning("No expected deposits data extracted")
        overall_success = False

    # ========================================================================
    # FINAL SUMMARY
    # ========================================================================
    logger.info(f"\n{'='*80}")
    if overall_success:
        logger.info("✓ ALL PROCESSING COMPLETE — no issues detected")
    else:
        logger.error("❌ PROCESSING COMPLETE WITH ISSUES — review warnings above")
    logger.info(f"{'='*80}")

    # Non-zero exit code fails the GitHub Action and triggers notification
    if not overall_success:
        sys.exit(1)


if __name__ == "__main__":
    main()

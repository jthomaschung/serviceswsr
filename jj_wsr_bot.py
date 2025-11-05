"""
Jimmy John's WSR Export Bot - Complete Fixed Version
Automates downloading WSR (Weekly Sales Report) exports from Jimmy John's portal
"""

import os
import time
import logging
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Optional, Tuple
import json
from playwright.sync_api import sync_playwright, Page, Download
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging with UTF-8 encoding to handle special characters
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('jj_wsr_bot.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class JimmyJohnsWSRBot:
    """Bot for downloading WSR reports from Jimmy John's Macromatix portal"""
    
    def __init__(self):
        # URLs - start with dashboard URL since login redirects there
        self.start_url = "https://prod-services.jimmyjohns.com/pages/aspx/dashboard/"
        
        # Credentials
        self.email = os.getenv('JJ_EMAIL')
        self.password = os.getenv('JJ_PASSWORD')
        
        # Directories
        self.download_dir = Path(os.getenv('DOWNLOAD_DIR', './downloads'))
        self.processed_dir = Path(os.getenv('PROCESSED_DIR', './processed'))
        self.download_dir.mkdir(exist_ok=True)
        self.processed_dir.mkdir(exist_ok=True)
        
        # Database URL for data ingestion
        self.database_url = os.getenv('DATABASE_URL')
        
        # Track downloads
        self.downloaded_files = []
    
    def login(self, page: Page) -> bool:
        """Handle login if needed"""
        try:
            logger.info("Navigating to Jimmy John's portal...")
            
            # Go to the dashboard URL - it will redirect to login if needed
            page.goto(self.start_url, wait_until='domcontentloaded', timeout=30000)
            
            # Check if we need to login (look for login elements)
            page.wait_for_timeout(2000)
            
            # Check if already logged in
            if "dashboard" in page.url.lower() and page.locator('text="MY DASHBOARD"').count() > 0:
                logger.info("Already logged in!")
                return True
            
            # Look for email/username field
            if page.locator('input[type="email"], input[type="text"]').count() > 0:
                logger.info("Login required, entering credentials...")
                
                # Enter email
                email_input = page.locator('input[type="email"], input[type="text"]').first
                email_input.fill(self.email)
                logger.info("Entered email")
                
                # Look for NEXT button (two-step login)
                if page.locator('button:has-text("NEXT")').count() > 0:
                    page.locator('button:has-text("NEXT")').click()
                    logger.info("Clicked NEXT")
                    page.wait_for_timeout(2000)
                
                # Enter password
                password_input = page.locator('input[type="password"]').first
                password_input.fill(self.password)
                logger.info("Entered password")
                
                # Click sign in
                signin_buttons = [
                    'button:has-text("SIGN IN")',
                    'button:has-text("Sign In")',
                    'button:has-text("Login")',
                    'button[type="submit"]'
                ]
                
                for selector in signin_buttons:
                    if page.locator(selector).count() > 0:
                        page.locator(selector).first.click()
                        logger.info(f"Clicked sign in button")
                        break
                
                # Wait for dashboard to load
                logger.info("Waiting for dashboard to load...")
                page.wait_for_timeout(5000)
            
            # Verify we're on the dashboard
            if "dashboard" in page.url.lower() or page.locator('text="MY DASHBOARD"').count() > 0:
                logger.info("Successfully on dashboard!")
                return True
            else:
                logger.error(f"Login failed. Current URL: {page.url}")
                page.screenshot(path='login_failed.png')
                return False
                
        except Exception as e:
            logger.error(f"Login process failed: {e}")
            page.screenshot(path='login_error.png')
            return False
    
    def navigate_to_wsr_export(self, page: Page) -> bool:
        """Navigate to WSR Export page"""
        try:
            logger.info("Looking for Sales Reports link...")
            
            # Click on Sales Reports link under RESOURCES
            sales_reports = page.locator('text="Sales Reports"')
            if sales_reports.count() > 0:
                sales_reports.click()
                logger.info("Clicked Sales Reports")
                
                # Wait for page to load
                page.wait_for_load_state('networkidle', timeout=15000)
                page.wait_for_timeout(2000)
                
                # Now click on WSR EXPORT in the menu
                logger.info("Looking for WSR EXPORT...")
                
                # Try different selectors for WSR EXPORT
                wsr_selectors = [
                    'text="WSR EXPORT"',
                    'text="WSR Export"',
                    'a:has-text("WSR")',
                    '*:has-text("WSR EXPORT")'
                ]
                
                for selector in wsr_selectors:
                    if page.locator(selector).count() > 0:
                        page.locator(selector).first.click()
                        logger.info("Clicked WSR EXPORT")
                        
                        # Wait for WSR Export page to load
                        page.wait_for_load_state('networkidle', timeout=10000)
                        page.wait_for_timeout(2000)
                        
                        # Verify we're on the WSR Export page
                        if page.locator('text="Select Reporting Week Ending Date"').count() > 0:
                            logger.info("Successfully on WSR Export page")
                            return True
                        break
                
                # If we can't find WSR EXPORT, take a screenshot
                if page.locator('text="Select Reporting Week Ending Date"').count() == 0:
                    logger.error("Could not find WSR Export page elements")
                    page.screenshot(path='wsr_navigation_failed.png')
                    return False
                    
            else:
                logger.error("Could not find Sales Reports link")
                page.screenshot(path='sales_reports_not_found.png')
                return False
                
            return True
            
        except Exception as e:
            logger.error(f"Failed to navigate to WSR Export: {e}")
            page.screenshot(path='navigation_error.png')
            return False
    
    def select_reporting_week(self, page: Page, week_offset: int = 0) -> str:
        """Select reporting week (0 = most recent, 1 = previous week, etc.)"""
        try:
            logger.info(f"Selecting reporting week (offset: {week_offset})...")
            
            # Click on the week dropdown
            week_dropdown = page.locator('text="Select Reporting Week Ending Date"').locator('xpath=following-sibling::*').first
            week_dropdown.click()
            
            # Wait for dropdown options to appear
            page.wait_for_timeout(1000)
            
            # Get all week options
            week_options = page.locator('[role="option"]').all()
            
            if not week_options:
                # Try alternative selector
                week_options = page.locator('.dropdown-item').all()
            
            if week_offset < len(week_options):
                selected_week = week_options[week_offset].text_content()
                week_options[week_offset].click()
                logger.info(f"Selected week: {selected_week}")
                return selected_week
            else:
                logger.error(f"Week offset {week_offset} out of range")
                return None
                
        except Exception as e:
            logger.error(f"Failed to select week: {e}")
            return None
    
    def get_all_stores(self, page: Page) -> int:
        """Get count of all available stores by opening the dropdown"""
        try:
            logger.info("Detecting total number of stores...")
            
            # Open the stores dropdown (we know it's element index 2)
            dropdown_elements = page.locator('input.form-control:visible, [class*="select"]:visible, [class*="dropdown"]:visible').all()
            
            if len(dropdown_elements) >= 3:
                logger.info(f"Found {len(dropdown_elements)} dropdown elements")
                # Click element index 2 (the Stores dropdown)
                dropdown_elements[2].click()
                logger.info("Clicked Stores dropdown (element 2), waiting for checkboxes to appear...")
                
                # Wait longer for dropdown to fully load
                page.wait_for_timeout(3000)
                
                # Try to wait for checkboxes to appear
                try:
                    page.wait_for_selector('input[type="checkbox"]:visible', timeout=5000)
                except:
                    logger.warning("Checkboxes didn't appear, trying to click dropdown again...")
                    # Try clicking again if checkboxes don't appear
                    dropdown_elements[2].click()
                    page.wait_for_timeout(2000)
                
                # Count checkboxes (excluding "Select All" which is index 0)
                num_checkboxes = page.locator('input[type="checkbox"]:visible').count()
                logger.info(f"Found {num_checkboxes} total checkboxes")
                
                # Close dropdown
                page.keyboard.press('Escape')
                page.wait_for_timeout(500)
                
                # Subtract 1 for "Select All" checkbox
                num_stores = num_checkboxes - 1 if num_checkboxes > 0 else 0
                
                if num_stores == 0:
                    logger.warning("No stores detected, defaulting to 79 stores based on previous runs")
                    return 79  # Default based on your successful runs
                
                logger.info(f"Found {num_stores} stores in dropdown")
                return num_stores
            else:
                logger.warning("Could not find enough dropdown elements, defaulting to 79 stores")
                return 79  # Based on your logs showing 80 checkboxes (79 stores + Select All)
            
        except Exception as e:
            logger.error(f"Failed to get store count: {e}")
            return 79  # Default based on your logs
    
    def select_store_batch(self, page: Page, batch_start: int, batch_size: int = 15, total_stores: int = 80) -> int:
        """Select a batch of stores by checkbox index"""
        try:
            batch_end = min(batch_start + batch_size, total_stores)
            logger.info(f"Selecting stores {batch_start + 1} to {batch_end} of {total_stores}...")
            
            # Open the Stores dropdown (element index 2)
            dropdown_elements = page.locator('input.form-control:visible, [class*="select"]:visible, [class*="dropdown"]:visible').all()
            
            if len(dropdown_elements) >= 3:
                logger.info("Opening Stores dropdown...")
                dropdown_elements[2].click()
                page.wait_for_timeout(2000)
                
                # Check if dropdown opened
                num_checkboxes = page.locator('input[type="checkbox"]:visible').count()
                if num_checkboxes > 0:
                    logger.info(f"Found {num_checkboxes} checkboxes")
                    
                    # First, uncheck "Select All" if it's checked
                    select_all = page.locator('input[type="checkbox"]:visible').first
                    if select_all.is_checked():
                        select_all.click()
                        page.wait_for_timeout(500)
                        logger.info("Unchecked 'Select All'")
                    
                    # Get all checkboxes
                    all_checkboxes = page.locator('input[type="checkbox"]:visible').all()
                    
                    selected_count = 0
                    # Select stores in this batch (skip index 0 which is "Select All")
                    for i in range(batch_start + 1, min(batch_end + 1, len(all_checkboxes))):
                        try:
                            checkbox = all_checkboxes[i]
                            if not checkbox.is_checked():
                                checkbox.click()
                                selected_count += 1
                                logger.info(f"Selected store at index {i}")
                                page.wait_for_timeout(100)  # Small delay between selections
                        except Exception as e:
                            logger.warning(f"Failed to select store at index {i}: {e}")
                    
                    # Close dropdown
                    page.keyboard.press('Escape')
                    page.wait_for_timeout(500)
                    
                    logger.info(f"Selected {selected_count} stores in this batch")
                    return selected_count
                else:
                    logger.error("No checkboxes found after opening dropdown")
                    return 0
            else:
                logger.error("Could not find Stores dropdown")
                return 0
                
        except Exception as e:
            logger.error(f"Failed to select store batch: {e}")
            page.screenshot(path='store_selection_error.png')
            return 0
    
    def download_wsr_export(self, page: Page, week: str, batch_num: int) -> Optional[str]:
        """Download the WSR export file"""
        try:
            logger.info(f"Starting download for batch {batch_num}...")
            
            # Click EXPORT button to start the process
            export_button = page.locator('button:has-text("EXPORT")')
            if export_button.count() > 0:
                # Set up download handler BEFORE clicking export
                # Increased timeout to 120 seconds for larger batches
                with page.expect_download(timeout=120000) as download_info:  # 120 second timeout
                    export_button.click()
                    logger.info("Clicked EXPORT button, waiting for server to process and download ZIP...")
                    logger.info("This may take up to 2 minutes for larger batches...")
                    
                    # The server will process and then auto-download the ZIP
                    # The expect_download will catch it when it starts
                
                # Get the download (this will be the ZIP file)
                download = download_info.value
                
                # Get the actual filename from the server
                suggested_filename = download.suggested_filename
                logger.info(f"Downloaded file: {suggested_filename}")
                
                # Generate our filename while preserving the extension
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                week_str = week.replace('/', '-') if week else "unknown"
                
                # Keep the original extension (.zip)
                extension = Path(suggested_filename).suffix if suggested_filename else '.zip'
                filename = f"WSR_Export_{week_str}_Batch{batch_num}_{timestamp}{extension}"
                
                # Save the file
                save_path = self.download_dir / filename
                download.save_as(save_path)
                
                logger.info(f"Saved as: {filename}")
                self.downloaded_files.append(save_path)
                
                # Move to processed immediately
                processed_path = self.processed_dir / filename
                save_path.rename(processed_path)
                logger.info(f"Moved to processed: {processed_path}")
                
                # Verify file size
                file_size = processed_path.stat().st_size
                logger.info(f"File size: {file_size:,} bytes")
                
                if file_size < 1000:  # Less than 1KB is likely corrupt
                    logger.warning("File seems too small, might be corrupt")
                
                return str(processed_path)
                
            else:
                logger.error("Could not find EXPORT button")
                return None
            
        except Exception as e:
            logger.error(f"Failed to download export: {e}")
            return None
    
    def run(self, weeks_to_download: int = 1):
        """Main execution flow - downloads all stores in batches of 15"""
        try:
            logger.info(f"\n{'='*60}")
            logger.info(f"Starting Jimmy John's WSR Export Bot")
            logger.info(f"Download Directory: {self.download_dir.absolute()}")
            logger.info(f"Processed Directory: {self.processed_dir.absolute()}")
            logger.info(f"{'='*60}\n")
            
            with sync_playwright() as p:
                # Launch browser
                browser = p.chromium.launch(
                    headless=True,  # Required for GitHub Actions
                    args=['--disable-blink-features=AutomationControlled']
                )
                
                context = browser.new_context(
                    accept_downloads=True,
                    viewport={'width': 1920, 'height': 1080}
                )
                
                page = context.new_page()
                
                # Enable console logging for debugging
                page.on("console", lambda msg: logger.debug(f"Browser console: {msg.text}"))
                
                # Login
                if not self.login(page):
                    raise Exception("Login failed")
                
                # Navigate to WSR Export
                if not self.navigate_to_wsr_export(page):
                    raise Exception("Failed to navigate to WSR Export")
                
                # Process each week
                for week_offset in range(weeks_to_download):
                    logger.info(f"\n{'='*50}")
                    logger.info(f"Processing Week {week_offset + 1} of {weeks_to_download}")
                    logger.info(f"{'='*50}")
                    
                    # Select week
                    selected_week = self.select_reporting_week(page, week_offset)
                    if not selected_week:
                        logger.error(f"Failed to select week {week_offset}")
                        continue
                    
                    # Get total number of stores
                    total_stores = self.get_all_stores(page)
                    
                    if total_stores == 0:
                        logger.error("No stores found")
                        continue
                    
                    # Calculate batches
                    batch_size = 15
                    num_batches = (total_stores + batch_size - 1) // batch_size
                    
                    logger.info(f"Total stores: {total_stores}")
                    logger.info(f"Number of batches needed: {num_batches} (15 stores per batch)")
                    
                    # Process each batch
                    for batch_num in range(num_batches):
                        batch_start = batch_num * batch_size
                        
                        logger.info(f"\n--- Batch {batch_num + 1} of {num_batches} ---")
                        
                        # Clear any previous selections by reloading page if not first batch
                        if batch_num > 0:
                            logger.info("Reloading page for next batch...")
                            page.reload()
                            page.wait_for_timeout(3000)
                            
                            # Re-select week after reload
                            self.select_reporting_week(page, week_offset)
                            page.wait_for_timeout(1000)
                        
                        # Select stores for this batch
                        num_selected = self.select_store_batch(page, batch_start, batch_size, total_stores)
                        
                        if num_selected > 0:
                            # Download
                            filepath = self.download_wsr_export(page, selected_week, batch_num + 1)
                            
                            if filepath:
                                logger.info(f"Successfully downloaded batch {batch_num + 1}")
                            else:
                                logger.warning(f"Failed to download batch {batch_num + 1}")
                            
                            # Wait between batches to avoid overloading server
                            if batch_num < num_batches - 1:
                                logger.info("Waiting 10 seconds before next batch...")
                                time.sleep(10)
                        else:
                            logger.warning(f"No stores selected for batch {batch_num + 1}, skipping download")
                
                # Keep browser open for a moment for debugging
                logger.info("Keeping browser open for 10 seconds for debugging...")
                time.sleep(10)
                
                browser.close()
                
            # Summary
            logger.info(f"\n{'='*60}")
            logger.info(f"Bot Execution Complete!")
            logger.info(f"Total files downloaded: {len(self.downloaded_files)}")
            logger.info(f"Files saved to: {self.processed_dir.absolute()}")
            logger.info(f"{'='*60}")
            
        except Exception as e:
            logger.error(f"Bot execution failed: {e}")
            raise

def main():
    """Main entry point"""
    bot = JimmyJohnsWSRBot()
    
    # Configuration
    weeks_to_download = int(os.getenv('WEEKS_TO_DOWNLOAD', 1))
    
    # Run the bot
    bot.run(weeks_to_download)

if __name__ == "__main__":
    main()

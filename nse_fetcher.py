#!/usr/bin/env python3
"""
NSE Options Fetcher with Real Cookies
====================================

This script uses real NSE cookies provided by the user to fetch both Call (CE) and Put (PE) options data
for the same strike price - the one closest to the current stock price.
"""

import subprocess
import json
import pandas as pd
from datetime import datetime
import time
import argparse
import sys
import os
import logging
from typing import Dict, List, Optional, Tuple

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class NSECookieFetcher: 
    """
    NSE Options Fetcher using real cookies
    """
    
    def __init__(self):
        file_path = 'cookies.txt'  # Replace with the actual path to your text file

        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                file_content_string = file.read()
        except FileNotFoundError:
            print(f"Error: The file '{file_path}' was not found.")
        except Exception as e:
            print(f"An error occurred: {e}")
                
        # Real NSE cookies provided by user
        self.cookies = file_content_string
        os.chdir(os.path.dirname(os.path.abspath(__file__)))
        print("Cookies: ", self.cookies)
        # Common headers that match a real browser request
        self.headers = [
            '-H', 'accept: application/json, text/plain, */*',
            '-H', 'accept-language: en-US,en;q=0.9',
            '-H', 'cache-control: max-age=0',
            '-H', 'referer: https://www.nseindia.com/get-quotes/equity?symbol=ADANIENT',
            '-H', 'sec-fetch-dest: empty',
            '-H', 'sec-fetch-mode: cors',
            '-H', 'sec-fetch-site: same-origin',
            '-H', 'user-agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            '-H', 'x-requested-with: XMLHttpRequest',
            '-H', f'cookie: {self.cookies}',
            '--compressed'
        ]
    
    def fetch_with_cookies(self, url: str, output_file: str, max_retries: int = 3) -> Optional[str]:
        """
        Fetch data using curl with real cookies
        """
        for attempt in range(max_retries):
            try:
                logger.info(f"Fetching attempt {attempt + 1}/{max_retries}")
                logger.info(f"URL: {url}")
                
                # Build curl command
                cmd = ['curl', '-s', url] + self.headers + ['-o', output_file]
                
                logger.debug("Executing curl with cookies...")
                result = subprocess.run(
                    cmd,
                    timeout=30,
                    capture_output=True,
                    text=True
                )
                
                logger.info(f"Curl exit code: {result.returncode}")
                
                if result.returncode == 0:
                    if os.path.exists(output_file) and os.path.getsize(output_file) > 0:
                        with open(output_file, 'r', encoding='utf-8') as f:
                            content = f.read()
                        
                        logger.info(f"Received {len(content)} characters")
                        logger.debug(f"Content preview: {content[:100]}...")
                        
                        # Check for common error patterns
                        if any(error in content.lower() for error in ['access denied', 'resource not found', 'unauthorized', 'forbidden']):
                            logger.warning("Received error page instead of data")
                            logger.debug(f"Error content: {content[:200]}")
                        else:
                            return content
                    else:
                        logger.warning("No content received")
                else:
                    logger.warning(f"Curl failed with exit code: {result.returncode}")
                    if result.stderr:
                        logger.debug(f"Stderr: {result.stderr}")
                
            except subprocess.TimeoutExpired:
                logger.warning(f"Request timeout on attempt {attempt + 1}")
            except Exception as e:
                logger.warning(f"Error on attempt {attempt + 1}: {e}")
            
            if attempt < max_retries - 1:
                logger.info("Retrying in 3 seconds...")
                time.sleep(0.2)
        
        logger.error("All attempts failed")
        return None
    
    def fetch_equity_options(self, symbol: str, expiry_date: str) -> Optional[Dict]:
        """
        Fetch equity options data
        """
        url = f'https://www.nseindia.com/api/option-chain-v3?type=Equity&symbol={symbol}&expiry={expiry_date}'
        output_file = f'equity_options_{symbol}.json'
        
        logger.info(f"Fetching equity options for {symbol}")
        
        content = self.fetch_with_cookies(url, output_file)
        
        if content:
            try:
                data = json.loads(content)
                
                # Validate structure
                if 'records' in data and 'underlyingValue' in data['records']:
                    logger.info(f"✅ Successfully fetched equity options for {symbol}")
                    logger.info(f"Underlying value: {data['records']['underlyingValue']}")
                    logger.info(f"Options data entries: {len(data['records'].get('data', []))}")
                    return data
                else:
                    logger.warning(f"Unexpected data structure: {list(data.keys())}")
                    return None
                    
            except json.JSONDecodeError as e:
                logger.error(f"JSON decode error: {e}")
                logger.debug(f"Raw content: {content[:500]}")
                return None
        
        return None
    
    def fetch_index_options(self, symbol: str) -> Optional[Dict]:
        """
        Fetch index options data
        """
        url = f'https://www.nseindia.com/api/option-chain-indices?symbol={symbol}'
        output_file = f'index_options_{symbol}.json'
        
        logger.info(f"Fetching index options for {symbol}")
        
        content = self.fetch_with_cookies(url, output_file)
        
        if content:
            try:
                data = json.loads(content)
                
                if 'records' in data:
                    logger.info(f"✅ Successfully fetched index options for {symbol}")
                    return data
                else:
                    logger.warning(f"Unexpected data structure: {list(data.keys())}")
                    return None
                    
            except json.JSONDecodeError as e:
                logger.error(f"JSON decode error: {e}")
                return None
        
        return None
    
    def fetch_nearest_expiry_date(self, symbol: str = 'ABB') -> Optional[str]:
        """
        Fetch the nearest expiry date from NSE option-chain-contract-info API
        Uses ABB as default symbol to get expiry dates
        Returns the first (nearest) expiry date or None if fetch fails
        """
        url = f'https://www.nseindia.com/api/option-chain-contract-info?symbol={symbol}'
        output_file = 'contract_info.json'
        
        logger.info(f"Fetching nearest expiry date from NSE API using symbol {symbol}...")
        
        content = self.fetch_with_cookies(url, output_file)
        
        if content:
            try:
                data = json.loads(content)
                
                # Extract expiry dates from response
                if 'expiryDates' in data and data['expiryDates']:
                    expiry_dates = data['expiryDates']
                    nearest_expiry = expiry_dates[0]  # First date is the nearest
                    logger.info(f"✅ Found expiry dates: {expiry_dates}")
                    logger.info(f"🎯 Using nearest expiry: {nearest_expiry}")
                    return nearest_expiry
                else:
                    logger.warning(f"No expiry dates found in response")
                    return None
                    
            except json.JSONDecodeError as e:
                logger.error(f"JSON decode error: {e}")
                logger.debug(f"Raw content: {content[:500]}")
                return None
        
        logger.warning("Failed to fetch expiry date from API")
        return None
    
    def determine_symbol_type(self, symbol: str) -> str:
        """
        Determine if symbol is index or equity
        """
        index_symbols = ['NIFTY', 'BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY', 'NIFTYNXT50']
        return 'index' if symbol.upper() in index_symbols else 'equity'
    
    def fetch_options_data(self, symbol: str, expiry_date: str) -> Optional[Dict]:
        """
        Fetch options data based on symbol type
        """
        symbol_type = self.determine_symbol_type(symbol)
        logger.info(f"Processing {symbol} as {symbol_type} symbol")
        
        if symbol_type == 'index':
            # Try index API first for indices
            data = self.fetch_index_options(symbol)
            if data:
                return data
            
            # Fallback to equity API
            logger.info(f"Index API failed, trying equity API for {symbol}")
            return self.fetch_equity_options(symbol, expiry_date)
        else:
            # Try equity API first for stocks
            data = self.fetch_equity_options(symbol, expiry_date)
            if data:
                return data
            
            # Fallback to index API
            logger.info(f"Equity API failed, trying index API for {symbol}")
            return self.fetch_index_options(symbol)
    
    def find_options_around_spot(self, data: Dict, symbol: str = '') -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float], Optional[float], Optional[float], Optional[float], Optional[int]]:
        """
        Find the strike price closest to underlying value and get both CE and PE for that same strike
        Also find the closest PE strike to 90% of underlying value
        Returns: (underlying_value, closest_strike, ce_last_price, closest_strike, pe_last_price, 
                  closest_pe_strike_90, pe_last_price_90, lot_size)
        """
        if not data or 'records' not in data:
            return None, None, None, None, None, None, None, None
        
        records = data['records']
        print(records)
        underlying_value = records.get('underlyingValue', 0)
        options_data = records.get('data', [])
        
        # Extract lot size from API only - no defaults
        # Lot size is the minimum buyQuantity1 value from all CE and PE records
        lot_size = None
        buy_quantities = []
        
        if options_data:
            for option in options_data:
                # Check CE buyQuantity1
                if 'CE' in option and option['CE']:
                    ce_buy_qty = option['CE'].get('buyQuantity1', 0)
                    if ce_buy_qty > 0:
                        buy_quantities.append(ce_buy_qty)
                
                # Check PE buyQuantity1
                if 'PE' in option and option['PE']:
                    pe_buy_qty = option['PE'].get('buyQuantity1', 0)
                    if pe_buy_qty > 0:
                        buy_quantities.append(pe_buy_qty)
            
            # Lot size is the minimum of all buy quantities
            if buy_quantities:
                lot_size = min(buy_quantities)
        
        logger.info(f"📊 Analysis:")
        logger.info(f"  Underlying value: ₹{underlying_value}")
        logger.info(f"  Total options entries: {len(options_data)}")
        logger.info(f"  Lot size: {lot_size}")
        
        if not options_data:
            return underlying_value, None, None, None, None, None, None, lot_size
        
        # Find the strike price closest to underlying value
        closest_strike = None
        min_distance = float('inf')
        
        # Calculate 90% of underlying value
        ninety_percent_value = underlying_value * 0.9
        closest_pe_strike_90 = None
        min_distance_90 = float('inf')
        
        for option in options_data:
            strike_price = option.get('strikePrice', 0)
            if strike_price > 0:  # Ensure valid strike price
                # Find closest to spot price
                distance = abs(strike_price - underlying_value)
                if distance < min_distance:
                    min_distance = distance
                    closest_strike = strike_price
                
                # Find closest to 90% of spot price
                distance_90 = abs(strike_price - ninety_percent_value)
                if distance_90 < min_distance_90:
                    min_distance_90 = distance_90
                    closest_pe_strike_90 = strike_price
        
        if closest_strike is None:
            logger.warning("❌ No valid strike prices found")
            return underlying_value, None, None, None, None, None, None, lot_size
        
        logger.info(f"🎯 Closest strike to spot price: ₹{closest_strike} (distance: ₹{min_distance:.2f})")
        logger.info(f"🎯 90% of spot price: ₹{ninety_percent_value:.2f}")
        logger.info(f"🎯 Closest PE strike to 90% spot: ₹{closest_pe_strike_90} (distance: ₹{min_distance_90:.2f})")
        
        # Find CE and PE data for the closest strike
        ce_last_price = None
        pe_last_price = None
        pe_last_price_90 = None
        
        for option in options_data:
            strike_price = option.get('strikePrice', 0)
            
            # Get CE and PE data for closest strike to spot
            if strike_price == closest_strike:
                # Get CE data
                ce_data = option.get('CE', {})
                if ce_data and ce_data.get('lastPrice', 0) > 0:
                    ce_last_price = ce_data.get('lastPrice', 0)
                    logger.info(f"✅ Found CE at closest strike:")
                    logger.info(f"   Strike: ₹{closest_strike}")
                    logger.info(f"   CE Last Price: ₹{ce_last_price}")
                
                # Get PE data  
                pe_data = option.get('PE', {})
                if pe_data and pe_data.get('lastPrice', 0) > 0:
                    pe_last_price = pe_data.get('lastPrice', 0)
                    logger.info(f"✅ Found PE at closest strike:")
                    logger.info(f"   Strike: ₹{closest_strike}")
                    logger.info(f"   PE Last Price: ₹{pe_last_price}")
            
            # Get PE data for closest strike to 90% spot
            if strike_price == closest_pe_strike_90:
                pe_data_90 = option.get('PE', {})
                if pe_data_90 and pe_data_90.get('lastPrice', 0) > 0:
                    pe_last_price_90 = pe_data_90.get('lastPrice', 0)
                    logger.info(f"✅ Found PE at 90% strike:")
                    logger.info(f"   Strike: ₹{closest_pe_strike_90}")
                    logger.info(f"   PE Last Price: ₹{pe_last_price_90}")
        
        if ce_last_price is None:
            logger.warning(f"❌ No valid CE data found at strike ₹{closest_strike}")
        if pe_last_price is None:
            logger.warning(f"❌ No valid PE data found at strike ₹{closest_strike}")
        if pe_last_price_90 is None:
            logger.warning(f"❌ No valid PE data found at 90% strike ₹{closest_pe_strike_90}")
            
        return underlying_value, closest_strike, ce_last_price, closest_strike, pe_last_price, closest_pe_strike_90, pe_last_price_90, lot_size
    
    def analyze_symbol(self, symbol: str, expiry_date: str) -> Optional[Dict]:
        """
        Analyze options for a single symbol
        """
        logger.info(f"\n{'='*60}")
        logger.info(f"🔍 ANALYZING: {symbol.upper()}")
        logger.info(f"📅 Expiry: {expiry_date}")
        logger.info(f"{'='*60}")
        
        # Fetch options data
        data = self.fetch_options_data(symbol, expiry_date)
        
        if not data:
            logger.error(f"❌ Failed to fetch data for {symbol}")
            return None
        
        # Analyze the data
        underlying_value, ce_strike, ce_last_price, pe_strike, pe_last_price, pe_strike_90, pe_last_price_90, lot_size = self.find_options_around_spot(data, symbol)
        
        if underlying_value is None:
            logger.error(f"❌ Could not extract underlying value for {symbol}")
            return None
        
        # Since we're using the same strike for both CE and PE, use ce_strike for both
        strike_price = ce_strike  # This is the closest strike to spot price
        
        # Calculate 90% of stock price
        stock_price_90 = underlying_value * 0.9 if underlying_value else None
        
        # Calculate formula values for both CE and PE
        ce_formula_value = 'N/A'
        pe_formula_value = 'N/A'
        
        if strike_price is not None and ce_last_price is not None and underlying_value > 0:
            ce_formula_value = ((ce_last_price + strike_price - underlying_value) * 100) / underlying_value
            ce_formula_value = round(ce_formula_value, 2)  # Round to 2 decimal places
            
        if strike_price is not None and pe_last_price is not None and underlying_value > 0:
            pe_formula_value = ((pe_last_price + underlying_value - strike_price) * 100) / underlying_value
            pe_formula_value = round(pe_formula_value, 2)  # Round to 2 decimal places
        
        # Calculate total cost for PE at 90% strike (PE Premium × Lot Size)
        total_cost_90 = 'N/A'
        if pe_last_price_90 is not None and lot_size is not None:
            total_cost_90 = round(pe_last_price_90 * lot_size, 2)
        
        # Calculate total strike value at 90% (Strike Price × Lot Size)
        total_strike_value_90 = 'N/A'
        if pe_strike_90 is not None and lot_size is not None:
            total_strike_value_90 = round(pe_strike_90 * lot_size, 2)
        
        result = {
            'Stock Name': symbol.upper(),
            'Stock Price (Underlying Value)': underlying_value,
            'Strike Price': strike_price if strike_price is not None else 'N/A',
            'CE Last Price': ce_last_price if ce_last_price is not None else 'N/A',
            'CE Formula Value (%)': ce_formula_value,
            'CE % on Current Price': round(ce_last_price * 100 / underlying_value, 2) if ce_last_price is not None else 'N/A',
            'PE Last Price': pe_last_price if pe_last_price is not None else 'N/A',
            'PE Formula Value (%)': pe_formula_value,
            'PE % on Current Price': round(pe_last_price * 100 / underlying_value, 2) if pe_last_price is not None else 'N/A',
            'Stock Price * 0.9': round(stock_price_90, 2) if stock_price_90 is not None else 'N/A',
            'Closest PE Strike to 90% Price': pe_strike_90 if pe_strike_90 is not None else 'N/A',
            'PE Premium at 90% Strike': pe_last_price_90 if pe_last_price_90 is not None else 'N/A',
            'Lot Size': lot_size if lot_size is not None else 'N/A',
            'Total Premium at 90% Strike (Premium Lot)': total_cost_90,
            'Total Cost at 90% (Strike Lot)': total_strike_value_90
        }
        
        logger.info(f"📋 Result: {result}")
        return result
    
    def fetch_master_quote(self) -> Optional[List[str]]:
        """
        Fetch the master quote to get all available symbols
        """
        url = 'https://www.nseindia.com/api/master-quote'
        output_file = 'master_quote.json'
        
        logger.info("Fetching master quote for symbols list...")
        
        content = self.fetch_with_cookies(url, output_file)
        
        if content:
            try:
                data = json.loads(content)
                
                # The API returns a simple list of symbol strings
                if isinstance(data, list):
                    # Check if it's a list of strings (direct symbol list)
                    if data and isinstance(data[0], str):
                        symbols = data
                        logger.info(f"✅ Successfully fetched {len(symbols)} symbols from master quote")
                        return symbols
                    # Or if it's a list of dicts with 'symbol' key
                    else:
                        symbols = []
                        for item in data:
                            if isinstance(item, dict) and 'symbol' in item:
                                symbols.append(item['symbol'])
                        if symbols:
                            logger.info(f"✅ Successfully fetched {len(symbols)} symbols from master quote")
                            return symbols
                elif isinstance(data, dict):
                    # If data is a dict, try to find symbols in common keys
                    for key in ['data', 'symbols', 'results']:
                        if key in data:
                            items = data[key]
                            if isinstance(items, list):
                                # Check if it's a list of strings
                                if items and isinstance(items[0], str):
                                    logger.info(f"✅ Successfully fetched {len(items)} symbols from master quote")
                                    return items
                                # Or list of dicts
                                else:
                                    symbols = []
                                    for item in items:
                                        if isinstance(item, dict) and 'symbol' in item:
                                            symbols.append(item['symbol'])
                                    if symbols:
                                        logger.info(f"✅ Successfully fetched {len(symbols)} symbols from master quote")
                                        return symbols
                            break
                
                logger.warning("No symbols found in master quote response")
                logger.debug(f"Response structure: {list(data.keys()) if isinstance(data, dict) else type(data)}")
                logger.debug(f"Response preview: {str(data)[:200]}")
                return None
                    
            except json.JSONDecodeError as e:
                logger.error(f"JSON decode error: {e}")
                logger.debug(f"Raw content: {content[:500]}")
                return None
        
        return None
    
    def cleanup_files(self):
        """
        Clean up temporary JSON files
        """
        json_files = [f for f in os.listdir('.') if f.startswith(('equity_options_', 'index_options_', 'master_quote', 'contract_info'))]
        for file in json_files:
            try:
                os.remove(file)
                logger.debug(f"Cleaned up {file}")
            except:
                pass

def main():
    """
    Main function
    """
    # Fallback predefined symbols list (used only if API fetch fails)
    fallback_symbols_list = [
    "360ONE",
    "ABB",
    "ABCAPITAL",
    "ADANIENSOL",
    "ADANIENT",
    "ADANIGREEN",
    "ADANIPORTS",
    "ALKEM",
    "AMBER",
    "AMBUJACEM",
    "ANGELONE",
    "APLAPOLLO",
    "APOLLOHOSP",
    "ASHOKLEY",
    "ASIANPAINT",
    "ASTRAL",
    "AUBANK",
    "AUROPHARMA",
    "AXISBANK",
    "BAJAJ-AUTO",
    "BAJAJFINSV",
    "BAJFINANCE",
    "BANDHANBNK",
    "BANKBARODA",
    "BANKINDIA",
    "BDL",
    "BEL",
    "BHARATFORG",
    "BHARTIARTL",
    "BHEL",
    "BIOCON",
    "BLUESTARCO",
    "BOSCHLTD",
    "BPCL",
    "BRITANNIA",
    "BSE",
    "CAMS",
    "CANBK",
    "CDSL",
    "CGPOWER",
    "CHOLAFIN",
    "CIPLA",
    "COALINDIA",
    "COFORGE",
    "COLPAL",
    "CONCOR",
    "CROMPTON",
    "CUMMINSIND",
    "CYIENT",
    "DABUR",
    "DALBHARAT",
    "DELHIVERY",
    "DIVISLAB",
    "DIXON",
    "DLF",
    "DMART",
    "DRREDDY",
    "EICHERMOT",
    "ETERNAL",
    "EXIDEIND",
    "FEDERALBNK",
    "FORTIS",
    "GAIL",
    "GLENMARK",
    "GMRAIRPORT",
    "GODREJCP",
    "GODREJPROP",
    "GRASIM",
    "HAL",
    "HAVELLS",
    "HCLTECH",
    "HDFCAMC",
    "HDFCBANK",
    "HDFCLIFE",
    "HEROMOTOCO",
    "HFCL",
    "HINDALCO",
    "HINDPETRO",
    "HINDUNILVR",
    "HINDZINC",
    "HUDCO",
    "ICICIBANK",
    "ICICIGI",
    "ICICIPRULI",
    "IDEA",
    "IDFCFIRSTB",
    "IEX",
    "IGL",
    "IIFL",
    "INDHOTEL",
    "INDIANB",
    "INDIGO",
    "INDUSINDBK",
    "INDUSTOWER",
    "INFY",
    "INOXWIND",
    "IOC",
    "IRCTC",
    "IREDA",
    "IRFC",
    "ITC",
    "JINDALSTEL",
    "JIOFIN",
    "JSWENERGY",
    "JSWSTEEL",
    "JUBLFOOD",
    "KALYANKJIL",
    "KAYNES",
    "KEI",
    "KFINTECH",
    "KOTAKBANK",
    "KPITTECH",
    "LAURUSLABS",
    "LICHSGFIN",
    "LICI",
    "LODHA",
    "LT",
    "LTF",
    "LTIM",
    "LUPIN",
    "M&M",
    "MANAPPURAM",
    "MANKIND",
    "MARICO",
    "MARUTI",
    "MAXHEALTH",
    "MAZDOCK",
    "MCX",
    "MFSL",
    "MOTHERSON",
    "MPHASIS",
    "MUTHOOTFIN",
    "NATIONALUM",
    "NAUKRI",
    "NBCC",
    "NCC",
    "NESTLEIND",
    "NHPC",
    "NMDC",
    "NTPC",
    "NUVAMA",
    "NYKAA",
    "OBEROIRLTY",
    "OFSS",
    "OIL",
    "ONGC",
    "PAGEIND",
    "PATANJALI",
    "PAYTM",
    "PERSISTENT",
    "PETRONET",
    "PFC",
    "PGEL",
    "PHOENIXLTD",
    "PIDILITIND",
    "PIIND",
    "PNB",
    "PNBHOUSING",
    "POLICYBZR",
    "POLYCAB",
    "POWERGRID",
    "POWERINDIA",
    "PPLPHARMA",
    "PRESTIGE",
    "RBLBANK",
    "RECLTD",
    "RELIANCE",
    "RVNL",
    "SAIL",
    "SAMMAANCAP",
    "SBICARD",
    "SBILIFE",
    "SBIN",
    "SHREECEM",
    "SHRIRAMFIN",
    "SIEMENS",
    "SOLARINDS",
    "SONACOMS",
    "SRF",
    "SUNPHARMA",
    "SUPREMEIND",
    "SUZLON",
    "SYNGENE",
    "TATACONSUM",
    "TATAELXSI",
    "TATAPOWER",
    "TATASTEEL",
    "TATATECH",
    "TCS",
    "TECHM",
    "TIINDIA",
    "TITAGARH",
    "TITAN",
    "TMPV",
    "TORNTPHARM",
    "TORNTPOWER",
    "TRENT",
    "TVSMOTOR",
    "ULTRACEMCO",
    "UNIONBANK",
    "UNITDSPR",
    "UNOMINDA",
    "UPL",
    "VBL",
    "VEDL",
    "VOLTAS",
    "WIPRO",
    "YESBANK",
    "ZYDUSLIFE"
]

    parser = argparse.ArgumentParser(
        description='NSE Options Fetcher using Real Cookies',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  %(prog)s --symbols ADANIENT
  %(prog)s --symbols BANKNIFTY NIFTY  
  %(prog)s --symbols ADANIENT RELIANCE --expiry 28-Nov-2025
  %(prog)s --fetch-symbols  # Fetch symbols list from NSE API
        '''
    )
    
    parser.add_argument('--symbols', nargs='+', 
                       help='Stock/Index symbols to analyze')
    parser.add_argument('--expiry', default=None, 
                       help='Expiry date (DD-MMM-YYYY format). If not specified, will fetch nearest expiry from NSE API')
    parser.add_argument('--output', help='Output CSV filename')
    parser.add_argument('--keep-files', action='store_true',
                       help='Keep temporary JSON files for inspection')
    parser.add_argument('--fetch-symbols', action='store_true',
                       help='Fetch symbols list from NSE master-quote API')
    
    args = parser.parse_args()
    
    fetcher = NSECookieFetcher()
    
    # Determine expiry date to use
    if args.expiry:
        expiry_date = args.expiry
        logger.info(f"Using user-specified expiry date: {expiry_date}")
    else:
        # Fetch nearest expiry date from NSE API
        logger.info("No expiry date specified, fetching nearest expiry from NSE API...")
        expiry_date = fetcher.fetch_nearest_expiry_date('ABB')
        if not expiry_date:
            logger.error("❌ Failed to fetch expiry date from API and no expiry specified")
            logger.error("Please specify expiry date using --expiry argument")
            sys.exit(1)
        logger.info(f"✅ Using dynamically fetched expiry date: {expiry_date}")
    
    # Determine which symbols to process
    if args.symbols:
        # User provided specific symbols
        symbols_to_process = args.symbols
        logger.info(f"Using user-provided symbols: {symbols_to_process}")
    elif args.fetch_symbols:
        # Fetch symbols from API
        logger.info("Fetching symbols list from NSE master-quote API...")
        api_symbols = fetcher.fetch_master_quote()
        if api_symbols:
            symbols_to_process = api_symbols
            logger.info(f"✅ Using {len(symbols_to_process)} symbols from API")
        else:
            logger.warning("⚠️  Failed to fetch symbols from API, using fallback list")
            symbols_to_process = fallback_symbols_list
    else:
        # Default: Try to fetch from API, fallback to predefined list
        logger.info("Attempting to fetch symbols from NSE master-quote API...")
        api_symbols = fetcher.fetch_master_quote()
        if api_symbols:
            symbols_to_process = api_symbols
            logger.info(f"✅ Using {len(symbols_to_process)} symbols from API")
        else:
            logger.warning("⚠️  Failed to fetch symbols from API, using fallback list")
            symbols_to_process = fallback_symbols_list
    
    print(f"Arguments: {args}")
    print(f"Total symbols to process: {len(symbols_to_process)}")
    print(f"Expiry date to use: {expiry_date}")
    
    results = []
    
    try:
        logger.info(f"🚀 Starting analysis for {len(symbols_to_process)} symbols")
        logger.info(f"📅 Using expiry date: {expiry_date}")
        
        for i, symbol in enumerate(symbols_to_process, 1):
            logger.info(f"\n[{i}/{len(symbols_to_process)}] Processing {symbol}")
            
            result = fetcher.analyze_symbol(symbol, expiry_date)
            if result:
                results.append(result)
            
            # Delay between symbols to be respectful
            if i < len(symbols_to_process):
                logger.info("⏳ Waiting 3 seconds before next symbol...")
                time.sleep(0.2)
        
        # Process results
        if results:
            df = pd.DataFrame(results)
            
            # Generate output filename
            if args.output is None:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                args.output = f"nse_cookies_analysis_{timestamp}.csv"
            
            # Save to CSV
            df.to_csv(args.output, index=False)
            
            # Display results
            print(f"\n{'='*80}")
            print("🎯 NSE OPTIONS ANALYSIS RESULTS (Using Real Cookies)")
            print(f"{'='*80}")
            print(df.to_string(index=False))
            
            print(f"\n{'='*80}")
            print("📊 SUMMARY")
            print(f"{'='*80}")
            for _, row in df.iterrows():
                stock = row['Stock Name']
                price = row['Stock Price (Underlying Value)']
                strike = row['Strike Price']
                ce_premium = row['CE Last Price']
                pe_premium = row['PE Last Price']
                
                print(f"📈 {stock}: Current ₹{price} | Strike ₹{strike}")
                
                if ce_premium != 'N/A':
                    print(f"   ✅ CE: ₹{ce_premium}")
                else:
                    print(f"   ⚠️  CE: No data available")
                    
                if pe_premium != 'N/A':
                    print(f"   ✅ PE: ₹{pe_premium}")
                else:
                    print(f"   ⚠️  PE: No data available")
                
                print()  # Empty line for better readability
            
            print(f"\n💾 Data saved to: {args.output}")
            print(f"📈 Successfully analyzed {len(results)} symbols")
            print(f"📅 Operation completed with expiry date: {expiry_date}")
            
        else:
            print("❌ No data could be retrieved for any symbols")
            print("💡 This might indicate:")
            print("   - Cookies have expired")
            print("   - NSE has updated their blocking mechanisms")
            print("   - Symbols might not have options available")
    
    except KeyboardInterrupt:
        print("\n⏹️  Operation cancelled by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"💥 Unexpected error: {e}")
        sys.exit(1)
    finally:
        # Cleanup
        if not args.keep_files:
            fetcher.cleanup_files()

if __name__ == "__main__":
    main()
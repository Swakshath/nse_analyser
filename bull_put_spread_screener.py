#!/usr/bin/env python3
"""
Bull Put Spread Screener Engine
================================

Scans NSE FnO stocks, calculates Bull Put Spread metrics for each,
and ranks them by risk-adjusted return (ROI%, EV, POP).

Data source: NSE India APIs (option chain, contract info, master quote)
Authentication: Real NSE session cookies from cookies.txt

This file is completely independent of nse_fetcher.py.
"""

import subprocess
import json
import math
import os
import time
import logging
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from scipy.stats import norm

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Fallback FnO symbol list (copied from nse_fetcher.py, kept independent)
# ---------------------------------------------------------------------------
FALLBACK_FNO_SYMBOLS: List[str] = [
    "360ONE", "ABB", "ABCAPITAL", "ADANIENSOL", "ADANIENT", "ADANIGREEN",
    "ADANIPORTS", "ALKEM", "AMBER", "AMBUJACEM", "ANGELONE", "APLAPOLLO",
    "APOLLOHOSP", "ASHOKLEY", "ASIANPAINT", "ASTRAL", "AUBANK", "AUROPHARMA",
    "AXISBANK", "BAJAJ-AUTO", "BAJAJFINSV", "BAJFINANCE", "BANDHANBNK",
    "BANKBARODA", "BANKINDIA", "BDL", "BEL", "BHARATFORG", "BHARTIARTL",
    "BHEL", "BIOCON", "BLUESTARCO", "BOSCHLTD", "BPCL", "BRITANNIA", "BSE",
    "CAMS", "CANBK", "CDSL", "CGPOWER", "CHOLAFIN", "CIPLA", "COALINDIA",
    "COFORGE", "COLPAL", "CONCOR", "CROMPTON", "CUMMINSIND", "CYIENT",
    "DABUR", "DALBHARAT", "DELHIVERY", "DIVISLAB", "DIXON", "DLF", "DMART",
    "DRREDDY", "EICHERMOT", "ETERNAL", "EXIDEIND", "FEDERALBNK", "FORTIS",
    "GAIL", "GLENMARK", "GMRAIRPORT", "GODREJCP", "GODREJPROP", "GRASIM",
    "HAL", "HAVELLS", "HCLTECH", "HDFCAMC", "HDFCBANK", "HDFCLIFE",
    "HEROMOTOCO", "HFCL", "HINDALCO", "HINDPETRO", "HINDUNILVR", "HINDZINC",
    "HUDCO", "ICICIBANK", "ICICIGI", "ICICIPRULI", "IDEA", "IDFCFIRSTB",
    "IEX", "IGL", "IIFL", "INDHOTEL", "INDIANB", "INDIGO", "INDUSINDBK",
    "INDUSTOWER", "INFY", "INOXWIND", "IOC", "IRCTC", "IREDA", "IRFC",
    "ITC", "JINDALSTEL", "JIOFIN", "JSWENERGY", "JSWSTEEL", "JUBLFOOD",
    "KALYANKJIL", "KAYNES", "KEI", "KFINTECH", "KOTAKBANK", "KPITTECH",
    "LAURUSLABS", "LICHSGFIN", "LICI", "LODHA", "LT", "LTF", "LTIM",
    "LUPIN", "M&M", "MANAPPURAM", "MANKIND", "MARICO", "MARUTI",
    "MAXHEALTH", "MAZDOCK", "MCX", "MFSL", "MOTHERSON", "MPHASIS",
    "MUTHOOTFIN", "NATIONALUM", "NAUKRI", "NBCC", "NCC", "NESTLEIND",
    "NHPC", "NMDC", "NTPC", "NUVAMA", "NYKAA", "OBEROIRLTY", "OFSS",
    "OIL", "ONGC", "PAGEIND", "PATANJALI", "PAYTM", "PERSISTENT",
    "PETRONET", "PFC", "PGEL", "PHOENIXLTD", "PIDILITIND", "PIIND", "PNB",
    "PNBHOUSING", "POLICYBZR", "POLYCAB", "POWERGRID", "POWERINDIA",
    "PPLPHARMA", "PRESTIGE", "RBLBANK", "RECLTD", "RELIANCE", "RVNL",
    "SAIL", "SAMMAANCAP", "SBICARD", "SBILIFE", "SBIN", "SHREECEM",
    "SHRIRAMFIN", "SIEMENS", "SOLARINDS", "SONACOMS", "SRF", "SUNPHARMA",
    "SUPREMEIND", "SUZLON", "SYNGENE", "TATACONSUM", "TATAELXSI",
    "TATAPOWER", "TATASTEEL", "TATATECH", "TCS", "TECHM", "TIINDIA",
    "TITAGARH", "TITAN", "TMPV", "TORNTPHARM", "TORNTPOWER", "TRENT",
    "TVSMOTOR", "ULTRACEMCO", "UNIONBANK", "UNITDSPR", "UNOMINDA", "UPL",
    "VBL", "VEDL", "VOLTAS", "WIPRO", "YESBANK", "ZYDUSLIFE",
]

INDEX_SYMBOLS = {"NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "NIFTYNXT50"}

# Sector mapping for concentration warnings (top stocks)
SECTOR_MAP: Dict[str, str] = {
    # Banking / Financial
    "HDFCBANK": "Banking", "ICICIBANK": "Banking", "SBIN": "Banking",
    "KOTAKBANK": "Banking", "AXISBANK": "Banking", "BANKBARODA": "Banking",
    "PNB": "Banking", "INDUSINDBK": "Banking", "FEDERALBNK": "Banking",
    "IDFCFIRSTB": "Banking", "CANBK": "Banking", "BANDHANBNK": "Banking",
    "AUBANK": "Banking", "RBLBANK": "Banking", "INDIANB": "Banking",
    "BANKINDIA": "Banking", "UNIONBANK": "Banking", "YESBANK": "Banking",
    "BAJFINANCE": "NBFC", "BAJAJFINSV": "NBFC", "CHOLAFIN": "NBFC",
    "SHRIRAMFIN": "NBFC", "MUTHOOTFIN": "NBFC", "MANAPPURAM": "NBFC",
    "LTF": "NBFC", "LICHSGFIN": "NBFC", "PNBHOUSING": "NBFC",
    "HDFCAMC": "Financial Services", "HDFCLIFE": "Insurance",
    "SBILIFE": "Insurance", "ICICIPRULI": "Insurance", "ICICIGI": "Insurance",
    "SBICARD": "Financial Services", "LICI": "Insurance",
    # IT
    "TCS": "IT", "INFY": "IT", "HCLTECH": "IT", "WIPRO": "IT",
    "TECHM": "IT", "LTIM": "IT", "MPHASIS": "IT", "COFORGE": "IT",
    "PERSISTENT": "IT", "KPITTECH": "IT", "TATAELXSI": "IT", "CYIENT": "IT",
    "OFSS": "IT",
    # Pharma
    "SUNPHARMA": "Pharma", "DRREDDY": "Pharma", "CIPLA": "Pharma",
    "DIVISLAB": "Pharma", "LUPIN": "Pharma", "AUROPHARMA": "Pharma",
    "BIOCON": "Pharma", "TORNTPHARM": "Pharma", "ALKEM": "Pharma",
    "GLENMARK": "Pharma", "LAURUSLABS": "Pharma", "MANKIND": "Pharma",
    "SYNGENE": "Pharma", "PPLPHARMA": "Pharma",
    # Auto
    "MARUTI": "Auto", "M&M": "Auto", "TATAMOTORS": "Auto",
    "BAJAJ-AUTO": "Auto", "HEROMOTOCO": "Auto", "EICHERMOT": "Auto",
    "TVSMOTOR": "Auto", "ASHOKLEY": "Auto", "BHARATFORG": "Auto",
    "MOTHERSON": "Auto", "SONACOMS": "Auto", "UNOMINDA": "Auto",
    "TMPV": "Auto",
    # Energy / Oil & Gas
    "RELIANCE": "Energy", "ONGC": "Energy", "IOC": "Energy",
    "BPCL": "Energy", "HINDPETRO": "Energy", "GAIL": "Energy",
    "OIL": "Energy", "PETRONET": "Energy", "IGL": "Energy",
    "ADANIGREEN": "Energy", "ADANIENSOL": "Energy", "TATAPOWER": "Energy",
    "NTPC": "Power", "POWERGRID": "Power", "NHPC": "Power",
    "TORNTPOWER": "Power", "JSWENERGY": "Power", "PFC": "Power",
    "RECLTD": "Power", "IREDA": "Power",
    # Metals & Mining
    "TATASTEEL": "Metals", "JSWSTEEL": "Metals", "HINDALCO": "Metals",
    "VEDL": "Metals", "COALINDIA": "Metals", "NMDC": "Metals",
    "SAIL": "Metals", "NATIONALUM": "Metals", "JINDALSTEL": "Metals",
    "HINDZINC": "Metals",
    # FMCG
    "HINDUNILVR": "FMCG", "ITC": "FMCG", "NESTLEIND": "FMCG",
    "BRITANNIA": "FMCG", "DABUR": "FMCG", "MARICO": "FMCG",
    "COLPAL": "FMCG", "GODREJCP": "FMCG", "TATACONSUM": "FMCG",
    "VBL": "FMCG", "UNITDSPR": "FMCG", "DMART": "FMCG", "PATANJALI": "FMCG",
    # Infra / Construction / Real Estate
    "LT": "Infra", "ADANIPORTS": "Infra", "DLF": "Real Estate",
    "GODREJPROP": "Real Estate", "OBEROIRLTY": "Real Estate",
    "PRESTIGE": "Real Estate", "LODHA": "Real Estate",
    "PHOENIXLTD": "Real Estate", "CONCOR": "Infra",
    # Telecom
    "BHARTIARTL": "Telecom", "IDEA": "Telecom", "INDUSTOWER": "Telecom",
    # Defence
    "HAL": "Defence", "BEL": "Defence", "BDL": "Defence",
    "MAZDOCK": "Defence",
    # Others
    "ADANIENT": "Conglomerate", "TITAN": "Consumer", "JUBLFOOD": "Consumer",
    "TRENT": "Retail", "NYKAA": "Retail", "ETERNAL": "Consumer",
    "DELHIVERY": "Logistics", "INDIGO": "Aviation", "IRCTC": "Travel",
    "IRFC": "Financial Services", "BSE": "Exchange", "MCX": "Exchange",
    "CDSL": "Exchange", "CAMS": "Financial Services",
    "KALYANKJIL": "Consumer", "POLYCAB": "Capital Goods",
    "HAVELLS": "Capital Goods", "CROMPTON": "Capital Goods",
    "SIEMENS": "Capital Goods", "ABB": "Capital Goods",
    "CGPOWER": "Capital Goods", "BHEL": "Capital Goods",
    "CUMMINSIND": "Capital Goods", "BOSCHLTD": "Capital Goods",
    "DIXON": "Capital Goods", "KAYNES": "Capital Goods",
    "KEI": "Capital Goods", "BLUESTARCO": "Capital Goods",
    "TIINDIA": "Capital Goods", "POWERINDIA": "Capital Goods",
    "PIDILITIND": "Chemicals", "SRF": "Chemicals", "PIIND": "Chemicals",
    "UPL": "Chemicals", "ASTRAL": "Building Materials",
    "AMBUJACEM": "Cement", "SHREECEM": "Cement", "DALBHARAT": "Cement",
    "ULTRACEMCO": "Cement", "GRASIM": "Cement",
    "APOLLOHOSP": "Healthcare", "MAXHEALTH": "Healthcare",
    "FORTIS": "Healthcare",
    "ASIANPAINT": "Paints", "PAGEIND": "Textile",
    "APLAPOLLO": "Steel Pipes", "SUPREMEIND": "Plastics",
    "NAUKRI": "Internet", "POLICYBZR": "Internet", "PAYTM": "Internet",
    "JIOFIN": "Financial Services", "MFSL": "Financial Services",
    "NUVAMA": "Financial Services", "ANGELONE": "Financial Services",
    "IIFL": "Financial Services", "KFINTECH": "Financial Services",
    "SAMMAANCAP": "Financial Services", "360ONE": "Financial Services",
    "ABCAPITAL": "Financial Services",
    "SUZLON": "Wind Energy", "INOXWIND": "Wind Energy",
    "HFCL": "Telecom Infra", "HUDCO": "Housing Finance",
    "RVNL": "Infra", "NBCC": "Infra", "NCC": "Infra",
    "GMRAIRPORT": "Aviation Infra", "PGEL": "Energy",
    "VOLTAS": "Consumer Durables", "EXIDEIND": "Auto Ancillary",
    "TITAGARH": "Railways", "SOLARINDS": "Industrial",
    "TATATECH": "IT Services", "MAZDOCK": "Defence",
    "PNBHOUSING": "Housing Finance",
}


# ============================================================================
# NSE Data Fetcher (self-contained, copied logic from nse_fetcher.py)
# ============================================================================

class NSEDataFetcher:
    """
    Fetches option-chain data from NSE India using session cookies.
    Completely independent copy — does not import nse_fetcher.py.
    """

    def __init__(self, cookies_path: str = "cookies.txt"):
        self.base_dir = os.path.dirname(os.path.abspath(__file__))
        os.chdir(self.base_dir)

        # Cookies are optional — NSE responds without them for some endpoints
        self.cookies = ""
        try:
            with open(cookies_path, "r", encoding="utf-8") as f:
                self.cookies = f.read().strip()
                logger.info(f"Loaded cookies from {cookies_path}")
        except FileNotFoundError:
            logger.info("No cookies.txt found — proceeding without cookies")

        # Auto-detect corporate proxy from environment
        self.proxy = os.environ.get("HTTPS_PROXY") or os.environ.get("HTTP_PROXY") or os.environ.get("https_proxy") or os.environ.get("http_proxy") or ""
        if self.proxy:
            logger.info(f"Using proxy: {self.proxy}")

        # Headers matching a real Chrome 146 browser session (NSE has Akamai bot detection)
        self.headers = [
            "-H", "accept: */*",
            "-H", "accept-language: en-GB,en-US;q=0.9,en;q=0.8",
            "-H", "priority: u=1, i",
            "-H", "referer: https://www.nseindia.com/option-chain",
            "-H", 'sec-ch-ua: "Chromium";v="146", "Not-A.Brand";v="24", "Google Chrome";v="146"',
            "-H", "sec-ch-ua-mobile: ?0",
            "-H", 'sec-ch-ua-platform: "macOS"',
            "-H", "sec-fetch-dest: empty",
            "-H", "sec-fetch-mode: cors",
            "-H", "sec-fetch-site: same-origin",
            "-H", "user-agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
        ]

    # ------------------------------------------------------------------
    def _curl_fetch(self, url: str, output_file: str, max_retries: int = 3) -> Optional[str]:
        """Execute curl and return response body, or None on failure."""
        for attempt in range(max_retries):
            try:
                cmd = ["curl", "-s", url] + self.headers
                # Use -b flag for cookies (not -H 'cookie:') — required by NSE/Akamai
                if self.cookies:
                    cmd += ["-b", self.cookies]
                cmd += ["-o", output_file]
                # Add proxy if detected from environment
                if self.proxy:
                    cmd += ["--proxy", self.proxy]
                result = subprocess.run(cmd, timeout=30, capture_output=True, text=True)

                if result.returncode == 0 and os.path.exists(output_file) and os.path.getsize(output_file) > 0:
                    with open(output_file, "r", encoding="utf-8") as f:
                        content = f.read()
                    if any(err in content.lower() for err in ["access denied", "resource not found", "unauthorized", "forbidden"]):
                        logger.warning(f"NSE returned error page on attempt {attempt + 1}")
                    else:
                        return content
            except subprocess.TimeoutExpired:
                logger.warning(f"Timeout on attempt {attempt + 1}")
            except Exception as e:
                logger.warning(f"Error on attempt {attempt + 1}: {e}")

            if attempt < max_retries - 1:
                time.sleep(0.3)

        return None

    # ------------------------------------------------------------------
    def fetch_nearest_expiry(self, symbol: str = "ABB") -> Optional[str]:
        url = f"https://www.nseindia.com/api/option-chain-contract-info?symbol={symbol}"
        content = self._curl_fetch(url, "contract_info.json")
        if content:
            try:
                data = json.loads(content)
                dates = data.get("expiryDates", [])
                if dates:
                    logger.info(f"Nearest expiry: {dates[0]}")
                    return dates[0]
            except json.JSONDecodeError:
                pass
        return None

    # ------------------------------------------------------------------
    def fetch_all_expiries(self, symbol: str = "ABB") -> List[str]:
        """Return all available expiry dates for a symbol."""
        url = f"https://www.nseindia.com/api/option-chain-contract-info?symbol={symbol}"
        content = self._curl_fetch(url, "contract_info.json")
        if content:
            try:
                data = json.loads(content)
                return data.get("expiryDates", [])
            except json.JSONDecodeError:
                pass
        return []

    # ------------------------------------------------------------------
    def fetch_option_chain(self, symbol: str, expiry_date: str) -> Optional[Dict]:
        """Fetch full option chain for a symbol. Tries equity API first, falls back to index."""
        is_index = symbol.upper() in INDEX_SYMBOLS

        if is_index:
            data = self._fetch_index(symbol)
            if data:
                return data
            return self._fetch_equity(symbol, expiry_date)
        else:
            data = self._fetch_equity(symbol, expiry_date)
            if data:
                return data
            return self._fetch_index(symbol)

    def _fetch_equity(self, symbol: str, expiry_date: str) -> Optional[Dict]:
        url = f"https://www.nseindia.com/api/option-chain-v3?type=Equity&symbol={symbol}&expiry={expiry_date}"
        content = self._curl_fetch(url, f"equity_options_{symbol}.json")
        if content:
            try:
                data = json.loads(content)
                if "records" in data and "underlyingValue" in data.get("records", {}):
                    return data
            except json.JSONDecodeError:
                pass
        return None

    def _fetch_index(self, symbol: str) -> Optional[Dict]:
        url = f"https://www.nseindia.com/api/option-chain-indices?symbol={symbol}"
        content = self._curl_fetch(url, f"index_options_{symbol}.json")
        if content:
            try:
                data = json.loads(content)
                if "records" in data:
                    return data
            except json.JSONDecodeError:
                pass
        return None

    # ------------------------------------------------------------------
    def fetch_master_quote(self) -> Optional[List[str]]:
        url = "https://www.nseindia.com/api/master-quote"
        content = self._curl_fetch(url, "master_quote.json")
        if content:
            try:
                data = json.loads(content)
                if isinstance(data, list):
                    if data and isinstance(data[0], str):
                        return data
                    return [item["symbol"] for item in data if isinstance(item, dict) and "symbol" in item] or None
                if isinstance(data, dict):
                    for key in ("data", "symbols", "results"):
                        if key in data and isinstance(data[key], list):
                            items = data[key]
                            if items and isinstance(items[0], str):
                                return items
                            return [item["symbol"] for item in items if isinstance(item, dict) and "symbol" in item] or None
            except json.JSONDecodeError:
                pass
        return None

    # ------------------------------------------------------------------
    def fetch_zerodha_margin(
        self,
        symbol: str,
        expiry_date: str,
        short_strike: float,
        long_strike: float,
        lot_size: int,
    ) -> Optional[float]:
        """
        Fetch actual margin required from Zerodha SPAN margin calculator.

        Parameters:
            symbol: NSE symbol (e.g. "INOXWIND")
            expiry_date: expiry in NSE format "28-Apr-2026"
            short_strike: strike price of the put being sold
            long_strike: strike price of the put being bought
            lot_size: number of shares per lot

        Returns: total margin in ₹, or None if fetch fails
        """
        try:
            # Derive Zerodha scrip format: SYMBOL + YY + MMM (e.g. INOXWIND26APR)
            expiry_dt = datetime.strptime(expiry_date, "%d-%b-%Y")
            yy = expiry_dt.strftime("%y")            # "26"
            mmm = expiry_dt.strftime("%b").upper()    # "APR"
            scrip = f"{symbol}{yy}{mmm}"

            # URL-encode the form data for two legs
            # Leg 1: sell put at short_strike
            # Leg 2: buy put at long_strike
            form_data = (
                f"action=calculate"
                f"&exchange%5B%5D=NFO&product%5B%5D=OPT&scrip%5B%5D={scrip}"
                f"&option_type%5B%5D=PE&strike_price%5B%5D={short_strike:g}"
                f"&qty%5B%5D={lot_size}&trade%5B%5D=sell"
                f"&exchange%5B%5D=NFO&product%5B%5D=OPT&scrip%5B%5D={scrip}"
                f"&option_type%5B%5D=PE&strike_price%5B%5D={long_strike:g}"
                f"&qty%5B%5D={lot_size}&trade%5B%5D=buy"
            )

            cmd = [
                "curl", "-s", "https://zerodha.com/margin-calculator/SPAN",
                "-H", "Accept: application/json, text/javascript, */*; q=0.01",
                "-H", "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8",
                "-H", "Content-Type: application/x-www-form-urlencoded; charset=UTF-8",
                "-H", "Origin: https://zerodha.com",
                "-H", "Referer: https://zerodha.com/margin-calculator/SPAN/",
                "-H", "Sec-Fetch-Dest: empty",
                "-H", "Sec-Fetch-Mode: cors",
                "-H", "Sec-Fetch-Site: same-origin",
                "-H", "User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36",
                "-H", "X-Requested-With: XMLHttpRequest",
                "-H", 'sec-ch-ua: "Google Chrome";v="147", "Not.A/Brand";v="8", "Chromium";v="147"',
                "-H", "sec-ch-ua-mobile: ?0",
                "-H", 'sec-ch-ua-platform: "macOS"',
                "--data-raw", form_data,
            ]
            # Add proxy if detected
            if self.proxy:
                cmd += ["--proxy", self.proxy]

            result = subprocess.run(cmd, timeout=15, capture_output=True, text=True)

            if result.returncode == 0 and result.stdout:
                data = json.loads(result.stdout)
                total_margin = data.get("total", {}).get("total", None)
                if total_margin is not None and total_margin > 0:
                    return round(total_margin, 2)
        except (subprocess.TimeoutExpired, json.JSONDecodeError, ValueError, KeyError) as e:
            logger.debug(f"Zerodha margin fetch failed for {symbol}: {e}")
        except Exception as e:
            logger.debug(f"Zerodha margin fetch unexpected error for {symbol}: {e}")

        return None

    # ------------------------------------------------------------------
    def cleanup(self):
        for f in os.listdir("."):
            if f.startswith(("equity_options_", "index_options_", "master_quote", "contract_info")):
                try:
                    os.remove(f)
                except OSError:
                    pass


# ============================================================================
# Bull Put Spread Calculation Engine
# ============================================================================

class BullPutSpreadEngine:
    """
    Given an option chain, finds the best Bull Put Spread for a symbol and
    computes all relevant metrics: net credit, max loss, ROI%, POP, EV.
    """

    FAT_TAIL_BASE_HAIRCUT = 0.02   # 2% base
    FAT_TAIL_PER_SIGMA    = 0.01   # +1% per sigma

    @staticmethod
    def get_atm_iv(options_data: List[Dict], underlying: float) -> Optional[float]:
        """
        Compute ATM IV as the average of the CE and PE implied volatility
        at the strike closest to the underlying price.
        """
        if not options_data or underlying <= 0:
            return None

        closest = min(options_data, key=lambda o: abs(o.get("strikePrice", 0) - underlying))

        ivs = []
        for side in ("CE", "PE"):
            side_data = closest.get(side, {})
            if side_data:
                iv = side_data.get("impliedVolatility", 0)
                if iv and iv > 0:
                    ivs.append(iv / 100.0)  # NSE gives IV in % form

        return sum(ivs) / len(ivs) if ivs else None

    @staticmethod
    def get_lot_size(options_data: List[Dict]) -> Optional[int]:
        """Extract lot size from minimum buyQuantity1 across all options."""
        quantities = []
        for opt in options_data:
            for side in ("CE", "PE"):
                side_data = opt.get(side, {})
                if side_data:
                    qty = side_data.get("buyQuantity1", 0)
                    if qty > 0:
                        quantities.append(qty)
        return min(quantities) if quantities else None

    @staticmethod
    def estimate_pop(spot: float, strike: float, iv: float, dte: int) -> Tuple[float, float, float]:
        """
        Quick POP estimate using z-score method (no Black-Scholes needed).
        Returns (adjusted_pop, z_score, expected_move).
        """
        if iv <= 0 or dte <= 0 or spot <= 0:
            return 0.0, 0.0, 0.0

        expected_move = spot * iv * math.sqrt(dte / 365.0)
        if expected_move == 0:
            return 0.0, 0.0, 0.0

        z_score = (spot - strike) / expected_move
        theoretical_pop = norm.cdf(z_score)

        # Fat-tail haircut: 2% base + 1% per sigma
        haircut = BullPutSpreadEngine.FAT_TAIL_BASE_HAIRCUT + (z_score * BullPutSpreadEngine.FAT_TAIL_PER_SIGMA)
        adjusted_pop = max(0.0, min(1.0, theoretical_pop - haircut))

        return adjusted_pop, z_score, expected_move

    def _build_pe_strikes(self, options_data: List[Dict]) -> List[Dict]:
        """Build sorted list of strikes that have PE data.

        Requires: lastPrice > 0 AND buyPrice1 > 0 AND sellPrice1 > 0.
        - LTP > 0 = strike has actually traded
        - buyPrice1 > 0 = there's a buyer (needed to sell the put)
        - sellPrice1 > 0 = there's a seller (needed to buy the put)
        Premium = mid-price (bid+ask)/2 which is more current than LTP.
        """
        pe_strikes = []
        for opt in options_data:
            sp = opt.get("strikePrice", 0)
            pe = opt.get("PE", {})
            if not pe or sp <= 0:
                continue

            ltp = pe.get("lastPrice", 0) or 0
            if ltp <= 0:
                continue

            bid = pe.get("buyPrice1", 0) or 0   # NSE actual field name
            ask = pe.get("sellPrice1", 0) or 0   # NSE actual field name

            # Must have both buyer and seller — otherwise can't execute
            if bid <= 0 or ask <= 0:
                continue

            premium = (bid + ask) / 2

            pe_strikes.append({
                "strike": sp,
                "premium": premium,
                "oi": pe.get("openInterest", 0),
                "volume": pe.get("totalTradedVolume", 0),
                "iv": pe.get("impliedVolatility", 0),
                "bid": bid,
                "ask": ask,
                "ltp": ltp,
            })
        pe_strikes.sort(key=lambda x: x["strike"])
        return pe_strikes

    def _evaluate_spread(
        self,
        short_leg: Dict,
        long_leg: Dict,
        underlying: float,
        atm_iv: Optional[float],
        dte: int,
        lot_size: int = 1,
    ) -> Optional[Dict]:
        """Evaluate a single short/long combination and return metrics with absolute EV/Lot score."""
        net_credit = short_leg["premium"] - long_leg["premium"]
        if net_credit <= 0:
            return None

        spread_width = short_leg["strike"] - long_leg["strike"]
        if spread_width <= 0:
            return None

        max_loss_per_unit = spread_width - net_credit
        if max_loss_per_unit <= 0:
            max_loss_per_unit = 0.01

        roi_pct = (net_credit / max_loss_per_unit) * 100.0
        breakeven = short_leg["strike"] - net_credit
        safety_margin_pct = ((underlying - short_leg["strike"]) / underlying) * 100.0

        # Compute absolute EV per lot for scoring
        # This favors wider spreads that generate more real profit,
        # instead of narrow spreads that look good on ROI% but earn pennies.
        ev_per_lot_score = 0.0
        if atm_iv and atm_iv > 0 and dte > 0:
            pop, _, _ = self.estimate_pop(underlying, short_leg["strike"], atm_iv, dte)
            ev_per_unit = (pop * net_credit) - ((1 - pop) * max_loss_per_unit)
            ev_per_lot_score = ev_per_unit * lot_size

        return {
            "short_strike": short_leg["strike"],
            "short_premium": short_leg["premium"],
            "short_oi": short_leg["oi"],
            "short_volume": short_leg["volume"],
            "short_iv": short_leg["iv"],
            "short_bid": short_leg["bid"],
            "short_ask": short_leg["ask"],
            "short_ltp": short_leg.get("ltp", 0),
            "long_strike": long_leg["strike"],
            "long_premium": long_leg["premium"],
            "long_oi": long_leg["oi"],
            "long_volume": long_leg["volume"],
            "long_bid": long_leg.get("bid", 0),
            "long_ask": long_leg.get("ask", 0),
            "long_ltp": long_leg.get("ltp", 0),
            "net_credit": round(net_credit, 2),
            "spread_width": round(spread_width, 2),
            "max_loss_per_unit": round(max_loss_per_unit, 2),
            "roi_pct": round(roi_pct, 2),
            "breakeven": round(breakeven, 2),
            "safety_margin_pct": round(safety_margin_pct, 2),
            "_ev_lot_score": ev_per_lot_score,  # internal scoring: absolute EV per lot
        }

    def find_spread(
        self,
        options_data: List[Dict],
        underlying: float,
        short_strike_pct_low: float = 0.85,
        short_strike_pct_high: float = 0.95,
        atm_iv: Optional[float] = None,
        dte: int = 30,
        lot_size: int = 1,
    ) -> Optional[Dict]:
        """
        Find the BEST Bull Put Spread by scanning all strike combinations in a range.

        Parameters:
            options_data: list of option chain entries
            underlying: current stock price
            short_strike_pct_low: lower bound of scan range (e.g. 0.85 = 85% of spot)
            short_strike_pct_high: upper bound of scan range (e.g. 0.95 = 95% of spot)
            atm_iv: ATM implied volatility (for EV scoring)
            dte: days to expiry (for EV scoring)
            lot_size: shares per lot (used to score by absolute EV per lot)

        Scans all short strike candidates in the range, tries ALL long leg
        candidates for each, and returns the spread with the highest absolute
        EV per lot — favoring wider spreads that make more real money.
        """
        if not options_data or underlying <= 0:
            return None

        pe_strikes = self._build_pe_strikes(options_data)
        if len(pe_strikes) < 2:
            return None

        # Define the scan range for short strikes
        range_low = underlying * short_strike_pct_low
        range_high = underlying * short_strike_pct_high

        # Filter short leg candidates within range
        short_candidates = [s for s in pe_strikes if range_low <= s["strike"] <= range_high and s["premium"] > 0]

        if not short_candidates:
            return None

        # Guardrails for spread width (as fraction of short strike)
        MIN_SPREAD_WIDTH_PCT = 0.005   # at least 0.5% of short strike
        MAX_SPREAD_WIDTH_PCT = 0.15    # at most 15% of short strike

        best_spread = None
        best_score = -999999

        for short_leg in short_candidates:
            # All strikes below this short leg
            long_candidates = [s for s in pe_strikes if s["strike"] < short_leg["strike"]]
            if not long_candidates:
                continue

            min_width = short_leg["strike"] * MIN_SPREAD_WIDTH_PCT
            max_width = short_leg["strike"] * MAX_SPREAD_WIDTH_PCT

            for long_leg in long_candidates:
                width = short_leg["strike"] - long_leg["strike"]
                # Skip if spread width is outside guardrails
                if width < min_width or width > max_width:
                    continue

                spread = self._evaluate_spread(short_leg, long_leg, underlying, atm_iv, dte, lot_size)
                if spread and spread["_ev_lot_score"] > best_score:
                    best_score = spread["_ev_lot_score"]
                    best_spread = spread

        # Remove internal scoring field before returning
        if best_spread:
            best_spread.pop("_ev_lot_score", None)

        return best_spread

    @staticmethod
    def find_support_levels(
        options_data: List[Dict],
        short_strike: float,
        underlying: float,
        top_n: int = 3,
    ) -> List[Dict]:
        """
        Find OI-based support levels below the short strike.

        Scans all PE strikes below the short strike, ranks by Put OI,
        and returns the top N as support levels with strength rating.

        Each level includes:
          - strike, put_oi, put_volume, pcr (PE OI / CE OI at that strike)
          - distance_pct: how far below the short strike (%)
          - strength: "Strong" / "Moderate" / "Weak" based on OI + volume + PCR
        """
        candidates = []
        for opt in options_data:
            sp = opt.get("strikePrice", 0)
            if sp <= 0 or sp >= short_strike:
                continue

            pe = opt.get("PE", {}) or {}
            ce = opt.get("CE", {}) or {}

            pe_oi = pe.get("openInterest", 0) or 0
            if pe_oi <= 0:
                continue

            ce_oi = ce.get("openInterest", 0) or 0
            pe_vol = pe.get("totalTradedVolume", 0) or 0
            pcr = (pe_oi / ce_oi) if ce_oi > 0 else 99.0  # high PCR = bullish

            distance_pct = ((short_strike - sp) / short_strike) * 100.0

            candidates.append({
                "strike": sp,
                "put_oi": pe_oi,
                "put_volume": pe_vol,
                "pcr": round(pcr, 2),
                "distance_pct": round(distance_pct, 2),
            })

        # Sort by Put OI descending — highest OI = strongest support
        candidates.sort(key=lambda x: x["put_oi"], reverse=True)
        top = candidates[:top_n]

        # Assign strength rating
        if top:
            max_oi = top[0]["put_oi"]
            for level in top:
                oi_ratio = level["put_oi"] / max_oi if max_oi > 0 else 0
                has_volume = level["put_volume"] > 0
                high_pcr = level["pcr"] > 1.2

                if oi_ratio >= 0.7 and has_volume and high_pcr:
                    level["strength"] = "Strong"
                elif oi_ratio >= 0.4 or (has_volume and high_pcr):
                    level["strength"] = "Moderate"
                else:
                    level["strength"] = "Weak"

        # Re-sort by strike descending (nearest support first)
        top.sort(key=lambda x: x["strike"], reverse=True)
        return top

    def analyze_symbol(
        self,
        symbol: str,
        option_chain_data: Dict,
        dte: int,
        short_strike_pct_low: float = 0.85,
        short_strike_pct_high: float = 0.95,
    ) -> Optional[Dict]:
        """
        Full analysis for one symbol.
        Returns a flat dict ready for DataFrame insertion.
        """
        records = option_chain_data.get("records", {})
        underlying = records.get("underlyingValue", 0)
        options_data = records.get("data", [])

        if not underlying or not options_data:
            return None

        lot_size = self.get_lot_size(options_data)
        atm_iv = self.get_atm_iv(options_data, underlying)

        spread = self.find_spread(
            options_data, underlying,
            short_strike_pct_low, short_strike_pct_high,
            atm_iv, dte,
            lot_size=lot_size or 1,
        )
        if not spread:
            return None

        # POP & EV calculation
        pop_adjusted, z_score, expected_move = (0.0, 0.0, 0.0)
        ev_per_unit = 0.0
        ev_per_lot = 0.0

        iv_for_pop = atm_iv  # use ATM IV for expected move (more reliable)
        if iv_for_pop and iv_for_pop > 0 and dte > 0:
            pop_adjusted, z_score, expected_move = self.estimate_pop(
                underlying, spread["short_strike"], iv_for_pop, dte
            )
            # EV = (POP × MaxProfit) - ((1 - POP) × MaxLoss)
            ev_per_unit = (pop_adjusted * spread["net_credit"]) - ((1 - pop_adjusted) * spread["max_loss_per_unit"])
            if lot_size:
                ev_per_lot = ev_per_unit * lot_size

        # Liquidity score (simple: sum of OI on both legs)
        total_oi = (spread.get("short_oi", 0) or 0) + (spread.get("long_oi", 0) or 0)
        total_volume = (spread.get("short_volume", 0) or 0) + (spread.get("long_volume", 0) or 0)

        # Bid-ask quality for short leg
        bid_ask_spread = 0
        if spread["short_bid"] and spread["short_ask"] and spread["short_ask"] > 0:
            bid_ask_spread = ((spread["short_ask"] - spread["short_bid"]) / spread["short_ask"]) * 100

        # OI-based support levels below the short strike
        support_levels = self.find_support_levels(
            options_data, spread["short_strike"], underlying, top_n=3
        )

        sector = SECTOR_MAP.get(symbol.upper(), "Other")

        # Flatten support levels into columns
        support_data = {}
        for i, lvl in enumerate(support_levels, start=1):
            support_data[f"Support {i}"] = lvl["strike"]
            support_data[f"Support {i} OI"] = lvl["put_oi"]
            support_data[f"Support {i} Vol"] = lvl["put_volume"]
            support_data[f"Support {i} PCR"] = lvl["pcr"]
            support_data[f"Support {i} Dist (%)"] = lvl["distance_pct"]
            support_data[f"Support {i} Strength"] = lvl["strength"]

        # Fill missing levels (if fewer than 3 found)
        for i in range(len(support_levels) + 1, 4):
            support_data[f"Support {i}"] = None
            support_data[f"Support {i} OI"] = None
            support_data[f"Support {i} Vol"] = None
            support_data[f"Support {i} PCR"] = None
            support_data[f"Support {i} Dist (%)"] = None
            support_data[f"Support {i} Strength"] = None

        return {
            "Symbol": symbol.upper(),
            "Sector": sector,
            "Spot Price": round(underlying, 2),
            "ATM IV (%)": round(atm_iv * 100, 2) if atm_iv else None,
            "Expected Move": round(expected_move, 2) if expected_move else None,
            "Short Strike": spread["short_strike"],
            "Long Strike": spread["long_strike"],
            "Spread Width": spread["spread_width"],
            "Short Premium": spread["short_premium"],
            "Short LTP": spread.get("short_ltp", 0),
            "Short Bid": spread.get("short_bid", 0),
            "Short Ask": spread.get("short_ask", 0),
            "Long Premium": spread["long_premium"],
            "Long LTP": spread.get("long_ltp", 0),
            "Long Bid": spread.get("long_bid", 0),
            "Long Ask": spread.get("long_ask", 0),
            "Net Credit": spread["net_credit"],
            "Max Loss / Unit": spread["max_loss_per_unit"],
            "ROI (%)": spread["roi_pct"],
            "Breakeven": spread["breakeven"],
            "Safety Margin (%)": spread["safety_margin_pct"],
            "POP (%)": round(pop_adjusted * 100, 2),
            "z-Score": round(z_score, 2),
            "EV / Unit": round(ev_per_unit, 2),
            "EV / Lot": round(ev_per_lot, 2) if lot_size else None,
            "Lot Size": lot_size,
            "Net Credit / Lot": round(spread["net_credit"] * lot_size, 2) if lot_size else None,
            "Max Loss / Lot": round(spread["max_loss_per_unit"] * lot_size, 2) if lot_size else None,
            # Capital Required — fallback = Max Loss/Lot; overwritten by real Zerodha SPAN margin in run_full_scan
            "Capital Required": round(spread["max_loss_per_unit"] * lot_size, 2) if lot_size else None,
            "Short OI": spread["short_oi"],
            "Long OI": spread["long_oi"],
            "Total OI": total_oi,
            "Total Volume": total_volume,
            "Bid-Ask Spread (%)": round(bid_ask_spread, 2),
            "EV Positive": ev_per_unit > 0,
            # EV as % of capital at risk — the TRUE efficiency metric
            "EV/Capital (%)": round((ev_per_unit / spread["max_loss_per_unit"]) * 100, 2) if spread["max_loss_per_unit"] > 0 else 0,
            # Annualized: assume you can repeat this trade every expiry cycle
            "Annualized EV (%)": round((ev_per_unit / spread["max_loss_per_unit"]) * (365 / max(dte, 1)) * 100, 2) if spread["max_loss_per_unit"] > 0 else 0,
            # Support levels (OI-based)
            **support_data,
        }


# ============================================================================
# Main orchestrator — run a full scan
# ============================================================================

def _process_single_symbol(
    fetcher: NSEDataFetcher,
    engine: BullPutSpreadEngine,
    symbol: str,
    expiry_date: str,
    dte: int,
    short_strike_pct_low: float,
    short_strike_pct_high: float,
) -> Optional[Dict]:
    """
    Process a single symbol: fetch option chain, analyze spread, fetch Zerodha margin.
    Thread-safe — each symbol writes to its own output file.
    """
    try:
        data = fetcher.fetch_option_chain(symbol, expiry_date)
        if not data:
            logger.warning(f"No data for {symbol}")
            return None

        result = engine.analyze_symbol(
            symbol, data, dte, short_strike_pct_low, short_strike_pct_high
        )
        if result:
            # Fetch actual Zerodha margin for this spread
            lot_size = result.get("Lot Size")
            if lot_size:
                margin = fetcher.fetch_zerodha_margin(
                    symbol, expiry_date,
                    result["Short Strike"], result["Long Strike"], lot_size,
                )
                if margin is not None:
                    result["Capital Required"] = margin
                    logger.info(f"  💰 {symbol} Zerodha margin: ₹{margin:,.0f}")

            logger.info(f"  ✅ {symbol}: ROI={result['ROI (%)']:.1f}% | EV/Unit={result['EV / Unit']:.2f} | POP={result['POP (%)']:.1f}%")
            return result
        else:
            logger.info(f"  ⚠️  {symbol}: No valid spread found")
            return None

    except Exception as e:
        logger.error(f"  ❌ {symbol}: {e}")
        return None


def run_full_scan(
    symbols: Optional[List[str]] = None,
    expiry_date: Optional[str] = None,
    short_strike_pct_low: float = 0.85,
    short_strike_pct_high: float = 0.95,
    delay_between_symbols: float = 0.3,
    max_parallel: int = 5,
    progress_callback=None,
) -> Tuple[List[Dict], str, int]:
    """
    Scan all symbols and return list of result dicts.

    Processes symbols in parallel batches for speed (default: 5 at a time).

    Parameters:
        symbols: list of NSE symbols (None = use fallback list)
        expiry_date: e.g. '24-Apr-2025' (None = auto-fetch nearest)
        short_strike_pct_low: lower bound of strike scan range (0.85 = 85% of spot)
        short_strike_pct_high: upper bound of strike scan range (0.95 = 95% of spot)
        delay_between_symbols: seconds delay between batches (not individual requests)
        max_parallel: number of stocks to fetch in parallel (default: 5)
        progress_callback: callable(current_index, total, symbol, status_msg)

    Returns: (results_list, expiry_date_used, dte)
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    fetcher = NSEDataFetcher()
    engine = BullPutSpreadEngine()

    # Resolve symbols
    if not symbols:
        logger.info("Fetching symbol list from NSE API...")
        api_symbols = fetcher.fetch_master_quote()
        symbols = api_symbols if api_symbols else FALLBACK_FNO_SYMBOLS
        logger.info(f"Using {len(symbols)} symbols")

    # Resolve expiry
    if not expiry_date:
        logger.info("Fetching nearest expiry date...")
        expiry_date = fetcher.fetch_nearest_expiry()
        if not expiry_date:
            raise RuntimeError("Could not determine expiry date. Please provide --expiry.")

    # Compute DTE
    try:
        # NSE returns dates like "24-Apr-2025"
        expiry_dt = datetime.strptime(expiry_date, "%d-%b-%Y")
        dte = max(1, (expiry_dt - datetime.now()).days)
    except ValueError:
        logger.warning(f"Could not parse expiry date '{expiry_date}', defaulting DTE=30")
        dte = 30

    logger.info(f"Expiry: {expiry_date} | DTE: {dte} days")
    logger.info(f"Short strike range: {short_strike_pct_low * 100:.0f}%-{short_strike_pct_high * 100:.0f}% of spot | Parallel: {max_parallel} at a time")

    results = []
    total = len(symbols)

    # Process in batches of max_parallel
    for batch_start in range(0, total, max_parallel):
        batch_end = min(batch_start + max_parallel, total)
        batch = symbols[batch_start:batch_end]

        if progress_callback:
            progress_callback(batch_start, total, ", ".join(batch), f"Batch {batch_start // max_parallel + 1}: {', '.join(batch)}")

        logger.info(f"[{batch_start + 1}-{batch_end}/{total}] Processing batch: {', '.join(batch)}")

        with ThreadPoolExecutor(max_workers=max_parallel) as executor:
            futures = {
                executor.submit(
                    _process_single_symbol,
                    fetcher, engine, sym, expiry_date, dte,
                    short_strike_pct_low, short_strike_pct_high,
                ): sym
                for sym in batch
            }

            for future in as_completed(futures):
                result = future.result()
                if result:
                    results.append(result)

        # Update progress after batch completes
        if progress_callback:
            progress_callback(batch_end - 1, total, batch[-1], f"Completed batch ({batch_end}/{total})")

        # Delay between batches to be nice to NSE servers
        if batch_end < total:
            time.sleep(delay_between_symbols)

    # Cleanup temp files
    fetcher.cleanup()

    # Sort by EV descending
    results.sort(key=lambda r: r.get("EV / Unit", 0), reverse=True)

    return results, expiry_date, dte


# ============================================================================
# CLI entry point
# ============================================================================

if __name__ == "__main__":
    import argparse
    import pandas as pd

    parser = argparse.ArgumentParser(description="Bull Put Spread Screener")
    parser.add_argument("--symbols", nargs="+", help="Specific symbols to scan")
    parser.add_argument("--expiry", default=None, help="Expiry date (DD-MMM-YYYY)")
    parser.add_argument("--strike-low", type=float, default=0.85,
                        help="Lower bound of short strike range as fraction of spot (default: 0.85)")
    parser.add_argument("--strike-high", type=float, default=0.95,
                        help="Upper bound of short strike range as fraction of spot (default: 0.95)")
    parser.add_argument("--output", default=None, help="Output CSV path")
    parser.add_argument("--delay", type=float, default=0.3,
                        help="Delay between batches in seconds")
    parser.add_argument("--parallel", type=int, default=5,
                        help="Number of stocks to fetch in parallel (default: 5)")

    args = parser.parse_args()

    results, expiry_used, dte = run_full_scan(
        symbols=args.symbols,
        expiry_date=args.expiry,
        short_strike_pct_low=args.strike_low,
        short_strike_pct_high=args.strike_high,
        delay_between_symbols=args.delay,
        max_parallel=args.parallel,
    )

    if results:
        df = pd.DataFrame(results)

        if args.output is None:
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            args.output = f"bull_put_spread_scan_{ts}.csv"

        df.to_csv(args.output, index=False)

        print(f"\n{'=' * 80}")
        print(f"🐂 BULL PUT SPREAD SCREENER — {expiry_used} (DTE: {dte})")
        print(f"{'=' * 80}")
        print(f"Scanned {len(results)} stocks with valid spreads\n")

        # Show top 10 by EV
        top = df[df["EV Positive"] == True].head(10)
        if not top.empty:
            display_cols = [
                "Symbol", "Sector", "Spot Price", "Short Strike", "Long Strike",
                "Net Credit", "Max Loss / Unit", "ROI (%)", "POP (%)", "EV / Unit",
                "Safety Margin (%)", "Lot Size", "Net Credit / Lot", "Max Loss / Lot",
            ]
            print(top[display_cols].to_string(index=False))
        else:
            print("⚠️  No positive-EV spreads found.")

        print(f"\n💾 Full results saved to: {args.output}")
    else:
        print("❌ No valid spreads found for any symbol.")

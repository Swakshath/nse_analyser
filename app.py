#!/usr/bin/env python3
"""
Bull Put Spread Screener — Streamlit UI
========================================

Interactive dashboard for scanning NSE FnO stocks and ranking
Bull Put Spread opportunities by ROI%, EV, POP, and more.

Run with:
    streamlit run app.py
"""

import streamlit as st
import pandas as pd
import math
from datetime import datetime
from scipy.stats import norm

from bull_put_spread_screener import (
    NSEDataFetcher,
    BullPutSpreadEngine,
    FALLBACK_FNO_SYMBOLS,
    SECTOR_MAP,
    run_full_scan,
)

# ============================================================================
# Page config
# ============================================================================
st.set_page_config(
    page_title="🐂 Bull Put Spread Screener",
    page_icon="🐂",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ============================================================================
# Custom CSS for color-coded table and overall styling
# ============================================================================
st.markdown("""
<style>
    /* Main header styling */
    .main-header {
        font-size: 2.2rem;
        font-weight: 700;
        color: #1f77b4;
        margin-bottom: 0.2rem;
    }
    .sub-header {
        font-size: 1.0rem;
        color: #666;
        margin-bottom: 1.5rem;
    }

    /* Top picks cards */
    .top-pick-card {
        background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
        border-radius: 12px;
        padding: 1.2rem;
        margin-bottom: 0.8rem;
        border-left: 4px solid #00d4aa;
        color: #e0e0e0;
    }
    .top-pick-card h3 {
        color: #00d4aa;
        margin: 0 0 0.5rem 0;
        font-size: 1.3rem;
    }
    .top-pick-card .metric {
        display: inline-block;
        margin-right: 1.5rem;
        margin-bottom: 0.3rem;
    }
    .top-pick-card .metric-label {
        font-size: 0.75rem;
        color: #888;
        text-transform: uppercase;
    }
    .top-pick-card .metric-value {
        font-size: 1.1rem;
        font-weight: 600;
        color: #fff;
    }
    .top-pick-card .metric-value.positive { color: #00d4aa; }
    .top-pick-card .metric-value.negative { color: #ff6b6b; }

    /* Warning banner */
    .sector-warning {
        background: #fff3cd;
        border: 1px solid #ffc107;
        border-radius: 8px;
        padding: 0.8rem 1rem;
        margin: 1rem 0;
        color: #856404;
    }

    /* Stats row */
    .stat-box {
        background: #f8f9fa;
        border-radius: 8px;
        padding: 1rem;
        text-align: center;
    }
    .stat-box .stat-value {
        font-size: 1.8rem;
        font-weight: 700;
        color: #1f77b4;
    }
    .stat-box .stat-label {
        font-size: 0.85rem;
        color: #666;
    }

    /* Hide Streamlit default */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}

    /* Dataframe styling */
    .stDataFrame {
        font-size: 0.9rem;
    }
</style>
""", unsafe_allow_html=True)


# ============================================================================
# Sidebar controls
# ============================================================================

with st.sidebar:
    st.markdown("## ⚙️ Screener Settings")
    st.markdown("---")

    st.markdown("### 📍 Strike Selection")
    strike_range = st.slider(
        "Short strike scan range (% of spot)",
        min_value=80, max_value=99, value=(85, 95), step=1,
        help="Scan ALL strikes in this range and pick the one with the best EV/Capital for each stock. "
             "Example: 85-95 means scan from 5% OTM to 15% OTM."
    )
    short_strike_pct_low = strike_range[0] / 100.0
    short_strike_pct_high = strike_range[1] / 100.0

    st.markdown("### 🎯 Filters")
    min_pop = st.slider(
        "Min POP (%)",
        min_value=50, max_value=95, value=75, step=5,
        help="Minimum Probability of Profit to display."
    )

    min_roi = st.slider(
        "Min ROI (%)",
        min_value=0, max_value=100, value=5, step=5,
        help="Minimum Return on Investment to display."
    )

    ev_positive_only = st.checkbox(
        "Only show EV-positive spreads",
        value=True,
        help="Hide spreads with negative Expected Value."
    )

    min_credit_capital_pct = st.slider(
        "Min Credit/Capital (%)",
        min_value=0.0, max_value=20.0, value=2.5, step=0.5,
        help="Minimum Net Credit per Lot ÷ Capital Required × 100. "
             "E.g. 2% means you get at least ₹2 credit for every ₹100 margin blocked."
    )

    min_oi = st.number_input(
        "Min total OI (both legs)",
        min_value=0, value=0, step=500,
        help="Minimum Open Interest across short + long strike."
    )

    st.markdown("### 📅 Expiry")
    expiry_mode = st.radio(
        "Expiry date",
        ["Auto-detect nearest", "Manual entry"],
        index=0,
    )
    manual_expiry = None
    if expiry_mode == "Manual entry":
        manual_expiry = st.text_input(
            "Expiry (DD-MMM-YYYY)", placeholder="24-Apr-2025"
        )

    st.markdown("### 📊 Symbols")
    symbol_mode = st.radio(
        "Stock universe",
        ["All FnO stocks (~230)", "Custom list"],
        index=0,
    )
    custom_symbols = None
    if symbol_mode == "Custom list":
        custom_symbols_input = st.text_area(
            "Enter symbols (comma-separated)",
            placeholder="RELIANCE, HDFCBANK, TCS, INFY, SBIN",
            height=80,
        )
        if custom_symbols_input:
            custom_symbols = [s.strip().upper() for s in custom_symbols_input.split(",") if s.strip()]

    st.markdown("### ⏱️ Speed & Throttle")
    max_parallel = st.slider(
        "Parallel requests",
        min_value=1, max_value=250, value=50, step=1,
        help="Number of stocks to fetch simultaneously. "
             "Higher = faster scan but more aggressive on NSE servers."
    )
    api_delay = st.slider(
        "Delay between batches (sec)",
        min_value=0.0, max_value=2.0, value=0.3, step=0.1,
        help="Pause between each batch of parallel requests. "
             "Higher = slower but safer from rate-limiting."
    )

    st.markdown("---")
    scan_button = st.button("🚀 Run Scan", type="primary", use_container_width=True)


# ============================================================================
# Helper: Color-code a dataframe
# ============================================================================

def style_results_table(df: pd.DataFrame):
    """Apply conditional formatting to the results DataFrame."""

    def color_ev(val):
        if pd.isna(val):
            return ""
        return "background-color: #d4edda; color: #155724" if val > 0 else "background-color: #f8d7da; color: #721c24"

    def color_pop(val):
        if pd.isna(val):
            return ""
        if val >= 85:
            return "background-color: #d4edda; color: #155724"
        if val >= 75:
            return "background-color: #fff3cd; color: #856404"
        return "background-color: #f8d7da; color: #721c24"

    def color_roi(val):
        if pd.isna(val):
            return ""
        if val >= 20:
            return "background-color: #d4edda; color: #155724"
        if val >= 10:
            return "background-color: #fff3cd; color: #856404"
        return ""

    def color_safety(val):
        if pd.isna(val):
            return ""
        if val >= 12:
            return "background-color: #d4edda; color: #155724"
        if val >= 8:
            return "background-color: #fff3cd; color: #856404"
        return "background-color: #f8d7da; color: #721c24"

    def color_bid_ask(val):
        if pd.isna(val):
            return ""
        if val <= 5:
            return "background-color: #d4edda; color: #155724"
        if val <= 15:
            return "background-color: #fff3cd; color: #856404"
        return "background-color: #f8d7da; color: #721c24"

    def color_support_strength(val):
        if pd.isna(val) or val == "":
            return ""
        if val == "Strong":
            return "background-color: #d4edda; color: #155724"
        if val == "Moderate":
            return "background-color: #fff3cd; color: #856404"
        return "background-color: #f8d7da; color: #721c24"

    styler = df.style
    for scol in ["Support 1 Strength", "Support 2 Strength", "Support 3 Strength"]:
        if scol in df.columns:
            styler = styler.map(color_support_strength, subset=[scol])
    if "EV / Unit" in df.columns:
        styler = styler.map(color_ev, subset=["EV / Unit"])
    if "EV / Lot" in df.columns:
        styler = styler.map(color_ev, subset=["EV / Lot"])
    if "POP (%)" in df.columns:
        styler = styler.map(color_pop, subset=["POP (%)"])
    if "ROI (%)" in df.columns:
        styler = styler.map(color_roi, subset=["ROI (%)"])
    if "Safety Margin (%)" in df.columns:
        styler = styler.map(color_safety, subset=["Safety Margin (%)"])
    if "Bid-Ask Spread (%)" in df.columns:
        styler = styler.map(color_bid_ask, subset=["Bid-Ask Spread (%)"])
    if "EV/Capital (%)" in df.columns:
        styler = styler.map(color_ev, subset=["EV/Capital (%)"])
    if "Annualized EV (%)" in df.columns:
        styler = styler.map(color_roi, subset=["Annualized EV (%)"])

    styler = styler.format({
        "Spot Price": "₹{:,.2f}",
        "Short Strike": "₹{:,.2f}",
        "Long Strike": "₹{:,.2f}",
        "Spread Width": "₹{:,.2f}",
        "Short Premium": "₹{:,.2f}",
        "Short LTP": "₹{:,.2f}",
        "Short Bid": "₹{:,.2f}",
        "Short Ask": "₹{:,.2f}",
        "Long Premium": "₹{:,.2f}",
        "Long LTP": "₹{:,.2f}",
        "Long Bid": "₹{:,.2f}",
        "Long Ask": "₹{:,.2f}",
        "Net Credit": "₹{:,.2f}",
        "Max Loss / Unit": "₹{:,.2f}",
        "Breakeven": "₹{:,.2f}",
        "Net Credit / Lot": "₹{:,.0f}",
        "Max Loss / Lot": "₹{:,.0f}",
        "Capital Required": "₹{:,.0f}",
        "EV / Unit": "₹{:,.2f}",
        "EV / Lot": "₹{:,.0f}",
        "ROI (%)": "{:,.1f}%",
        "POP (%)": "{:,.1f}%",
        "Safety Margin (%)": "{:,.1f}%",
        "ATM IV (%)": "{:,.1f}%",
        "Bid-Ask Spread (%)": "{:,.1f}%",
        "Expected Move": "₹{:,.2f}",
        "z-Score": "{:,.2f}",
        "Total OI": "{:,.0f}",
        "Total Volume": "{:,.0f}",
        "Short OI": "{:,.0f}",
        "Long OI": "{:,.0f}",
        "EV/Capital (%)": "{:,.1f}%",
        "Annualized EV (%)": "{:,.1f}%",
        "Support 1": "₹{:,.2f}",
        "Support 2": "₹{:,.2f}",
        "Support 3": "₹{:,.2f}",
        "Support 1 OI": "{:,.0f}",
        "Support 2 OI": "{:,.0f}",
        "Support 3 OI": "{:,.0f}",
        "Support 1 Dist (%)": "{:,.1f}%",
        "Support 2 Dist (%)": "{:,.1f}%",
        "Support 3 Dist (%)": "{:,.1f}%",
    }, na_rep="—")

    return styler


# ============================================================================
# Main content area
# ============================================================================

st.markdown('<div class="main-header">🐂 Bull Put Spread Screener</div>', unsafe_allow_html=True)
st.markdown(
    '<div class="sub-header">Scan NSE FnO stocks → Rank by Expected Value, ROI, and Probability of Profit</div>',
    unsafe_allow_html=True,
)

# ============================================================================
# Scan logic
# ============================================================================

if scan_button:
    symbols_to_scan = custom_symbols if custom_symbols else None
    expiry_to_use = manual_expiry if manual_expiry else None

    progress_bar = st.progress(0)
    status_text = st.empty()

    def update_progress(current, total, symbol, msg):
        pct = (current + 1) / total
        progress_bar.progress(pct)
        status_text.text(f"[{current + 1}/{total}] {msg}")

    try:
        with st.spinner("Initializing scan..."):
            results, expiry_used, dte = run_full_scan(
                symbols=symbols_to_scan,
                expiry_date=expiry_to_use,
                short_strike_pct_low=short_strike_pct_low,
                short_strike_pct_high=short_strike_pct_high,
                delay_between_symbols=api_delay,
                max_parallel=max_parallel,
                progress_callback=update_progress,
            )

        progress_bar.empty()
        status_text.empty()

        if results:
            st.session_state["results"] = results
            st.session_state["expiry_used"] = expiry_used
            st.session_state["dte"] = dte
            st.session_state["strike_range"] = f"{strike_range[0]}%-{strike_range[1]}%"
            st.success(f"✅ Scan complete — {len(results)} valid spreads found for expiry {expiry_used} (DTE: {dte} days) | Strike range: {strike_range[0]}%-{strike_range[1]}% of spot (scanning ALL short+long combos)")
        else:
            st.error("❌ No valid spreads found. Check your cookies or try different settings.")

    except FileNotFoundError as e:
        progress_bar.empty()
        status_text.empty()
        st.error(f"🔑 {e}")
    except RuntimeError as e:
        progress_bar.empty()
        status_text.empty()
        st.error(f"⚠️ {e}")
    except Exception as e:
        progress_bar.empty()
        status_text.empty()
        st.error(f"💥 Unexpected error: {e}")


# ============================================================================
# Display results (persisted in session_state)
# ============================================================================

if "results" in st.session_state and st.session_state["results"]:
    results = st.session_state["results"]
    expiry_used = st.session_state["expiry_used"]
    dte = st.session_state["dte"]

    df = pd.DataFrame(results)

    # ------------------------------------------------------------------
    # Apply filters
    # ------------------------------------------------------------------
    filtered = df.copy()
    if ev_positive_only:
        filtered = filtered[filtered["EV Positive"] == True]
    filtered = filtered[filtered["POP (%)"] >= min_pop]
    filtered = filtered[filtered["ROI (%)"] >= min_roi]
    if min_oi > 0:
        filtered = filtered[filtered["Total OI"] >= min_oi]
    if min_credit_capital_pct > 0:
        # Credit/Capital % = (Net Credit / Lot) / Capital Required × 100
        filtered = filtered[
            (filtered["Capital Required"].notna()) &
            (filtered["Capital Required"] > 0) &
            (filtered["Net Credit / Lot"].notna()) &
            ((filtered["Net Credit / Lot"] / filtered["Capital Required"]) * 100 >= min_credit_capital_pct)
        ]

    # ------------------------------------------------------------------
    # Summary stats row
    # ------------------------------------------------------------------
    st.markdown(f"### 📅 Expiry: {expiry_used} | DTE: {dte} days")

    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        st.metric("Total Scanned", len(df))
    with col2:
        st.metric("After Filters", len(filtered))
    with col3:
        ev_pos = len(df[df["EV Positive"] == True])
        st.metric("EV+ Spreads", ev_pos)
    with col4:
        avg_roi = filtered["ROI (%)"].mean() if len(filtered) > 0 else 0
        st.metric("Avg ROI (%)", f"{avg_roi:.1f}%")
    with col5:
        avg_pop = filtered["POP (%)"].mean() if len(filtered) > 0 else 0
        st.metric("Avg POP (%)", f"{avg_pop:.1f}%")

    # ------------------------------------------------------------------
    # Top 3 picks
    # ------------------------------------------------------------------
    st.markdown("---")
    st.markdown("### 🏆 Top 3 Setups (by Expected Value)")

    top3 = filtered.head(3)
    if len(top3) > 0:
        cols = st.columns(min(3, len(top3)))
        for idx, (_, row) in enumerate(top3.iterrows()):
            with cols[idx]:
                ev_class = "positive" if row["EV / Unit"] > 0 else "negative"
                lot_info = ""
                if pd.notna(row.get("Net Credit / Lot")) and pd.notna(row.get("Max Loss / Lot")):
                    lot_info = f"""
                    <div class="metric">
                        <div class="metric-label">Credit/Lot</div>
                        <div class="metric-value positive">₹{row['Net Credit / Lot']:,.0f}</div>
                    </div>
                    <div class="metric">
                        <div class="metric-label">Max Loss/Lot</div>
                        <div class="metric-value negative">₹{row['Max Loss / Lot']:,.0f}</div>
                    </div>
                    """

                ev_lot_display = ""
                if pd.notna(row.get("EV / Lot")):
                    ev_lot_class = "positive" if row["EV / Lot"] > 0 else "negative"
                    ev_lot_display = f"""
                    <div class="metric">
                        <div class="metric-label">EV/Lot</div>
                        <div class="metric-value {ev_lot_class}">₹{row['EV / Lot']:,.0f}</div>
                    </div>
                    """

                st.markdown(f"""
                <div class="top-pick-card">
                    <h3>#{idx + 1} {row['Symbol']}</h3>
                    <div style="font-size: 0.8rem; color: #aaa; margin-bottom: 0.5rem;">
                        {row['Sector']} | Spot: ₹{row['Spot Price']:,.2f}
                    </div>
                    <div class="metric">
                        <div class="metric-label">Spread</div>
                        <div class="metric-value">₹{row['Short Strike']:,.2f} / ₹{row['Long Strike']:,.2f}</div>
                    </div>
                    <div class="metric">
                        <div class="metric-label">Net Credit</div>
                        <div class="metric-value positive">₹{row['Net Credit']:,.2f}</div>
                    </div>
                    <div class="metric">
                        <div class="metric-label">ROI</div>
                        <div class="metric-value positive">{row['ROI (%)']:.1f}%</div>
                    </div>
                    <div class="metric">
                        <div class="metric-label">POP</div>
                        <div class="metric-value">{row['POP (%)']:.1f}%</div>
                    </div>
                    <div class="metric">
                        <div class="metric-label">EV/Unit</div>
                        <div class="metric-value {ev_class}">₹{row['EV / Unit']:,.2f}</div>
                    </div>
                    {ev_lot_display}
                    <div class="metric">
                        <div class="metric-label">Safety</div>
                        <div class="metric-value">{row['Safety Margin (%)']:.1f}%</div>
                    </div>
                    {lot_info}
                    <div class="metric">
                        <div class="metric-label">Lot Size</div>
                        <div class="metric-value">{int(row['Lot Size']) if pd.notna(row.get('Lot Size')) else '—'}</div>
                    </div>
                    <div class="metric">
                        <div class="metric-label">EV/Capital</div>
                        <div class="metric-value positive">{row['EV/Capital (%)']:.1f}%</div>
                    </div>
                    <div class="metric">
                        <div class="metric-label">Annualized EV</div>
                        <div class="metric-value positive">{row['Annualized EV (%)']:.1f}%</div>
                    </div>
                </div>
                """, unsafe_allow_html=True)
    else:
        st.info("No spreads match current filters. Try relaxing the filters in the sidebar.")

    # ------------------------------------------------------------------
    # Sector concentration warning
    # ------------------------------------------------------------------
    if len(filtered) >= 3:
        sector_counts = filtered["Sector"].value_counts()
        dominant = sector_counts.head(1)
        if len(dominant) > 0:
            top_sector = dominant.index[0]
            top_count = dominant.values[0]
            top_pct = (top_count / len(filtered)) * 100
            if top_pct >= 40 and top_count >= 3:
                st.markdown(
                    f'<div class="sector-warning">'
                    f'⚠️ <strong>Concentration Warning:</strong> {top_count} of {len(filtered)} '
                    f'filtered results ({top_pct:.0f}%) are in <strong>{top_sector}</strong>. '
                    f'Avoid putting too many spreads in one sector — correlation risk during crashes.'
                    f'</div>',
                    unsafe_allow_html=True,
                )

    # ------------------------------------------------------------------
    # Sort controls
    # ------------------------------------------------------------------
    st.markdown("---")
    st.markdown("### 📊 Full Results Table")

    sort_col_left, sort_col_right = st.columns([1, 1])
    with sort_col_left:
        sort_by = st.selectbox(
            "Sort by",
            options=[
                "EV/Capital (%)", "Annualized EV (%)", "EV / Unit", "EV / Lot",
                "ROI (%)", "POP (%)",
                "Safety Margin (%)", "Net Credit", "Net Credit / Lot",
                "Capital Required", "Max Loss / Lot", "Total OI", "ATM IV (%)", "Bid-Ask Spread (%)",
                "Spot Price", "Symbol",
            ],
            index=0,
        )
    with sort_col_right:
        sort_ascending = st.selectbox(
            "Order",
            options=["Descending", "Ascending"],
            index=0,
        ) == "Ascending"

    sorted_df = filtered.sort_values(
        by=sort_by, ascending=sort_ascending, na_position="last"
    )

    # ------------------------------------------------------------------
    # Column selection
    # ------------------------------------------------------------------
    all_display_cols = [
        "Symbol", "Sector", "Spot Price", "ATM IV (%)", "Expected Move",
        "Short Strike", "Long Strike", "Spread Width",
        "Short Premium", "Short LTP", "Short Bid", "Short Ask",
        "Long Premium", "Long LTP", "Long Bid", "Long Ask",
        "Net Credit", "Max Loss / Unit",
        "ROI (%)", "Breakeven", "Safety Margin (%)",
        "POP (%)", "z-Score", "EV / Unit", "EV / Lot",
        "EV/Capital (%)", "Annualized EV (%)",
        "Lot Size", "Net Credit / Lot", "Max Loss / Lot", "Capital Required",
        "Short OI", "Long OI", "Total OI", "Total Volume",
        "Bid-Ask Spread (%)",
        "Support 1", "Support 1 OI", "Support 1 Dist (%)", "Support 1 Strength",
        "Support 2", "Support 2 OI", "Support 2 Dist (%)", "Support 2 Strength",
        "Support 3", "Support 3 OI", "Support 3 Dist (%)", "Support 3 Strength",
    ]
    # Only show columns that exist in the data
    available_cols = [c for c in all_display_cols if c in sorted_df.columns]

    default_cols = [
        "Symbol", "Sector", "Spot Price", "Short Strike", "Short Premium",
        "Short Bid", "Short Ask",
        "Long Strike", "Long Premium",
        "Long Bid", "Long Ask",
        "Net Credit", "Max Loss / Unit", "ROI (%)", "POP (%)",
        "EV / Unit", "EV/Capital (%)", "Annualized EV (%)",
        "Safety Margin (%)", "Lot Size",
        "Capital Required", "Net Credit / Lot", "Max Loss / Lot", "EV / Lot", "Total OI",
        "Support 1", "Support 1 OI", "Support 1 Dist (%)", "Support 1 Strength",
        "Support 2", "Support 2 OI", "Support 2 Dist (%)", "Support 2 Strength",
    ]
    default_cols = [c for c in default_cols if c in available_cols]

    selected_cols = st.multiselect(
        "Columns to display",
        options=available_cols,
        default=default_cols,
    )

    if not selected_cols:
        selected_cols = default_cols

    # ------------------------------------------------------------------
    # Render styled table
    # ------------------------------------------------------------------
    display_df = sorted_df[selected_cols].reset_index(drop=True)
    styled = style_results_table(display_df)
    st.dataframe(
        styled,
        use_container_width=True,
        height=min(800, 40 + len(display_df) * 35),
    )

    # ------------------------------------------------------------------
    # Download button
    # ------------------------------------------------------------------
    csv_data = sorted_df[available_cols].to_csv(index=False)
    st.download_button(
        label="📥 Download CSV",
        data=csv_data,
        file_name=f"bull_put_spread_{expiry_used}_{datetime.now().strftime('%H%M%S')}.csv",
        mime="text/csv",
    )

    # ------------------------------------------------------------------
    # What-If Calculator — edit premiums and recalculate
    # ------------------------------------------------------------------
    st.markdown("---")
    st.markdown("### 🧮 What-If Calculator")
    st.markdown("*Pick a spread from the scan, tweak the premiums you'd actually get, and see recalculated metrics instantly.*")

    whatif_symbols = sorted_df["Symbol"].tolist()
    if whatif_symbols:
        selected_symbol = st.selectbox(
            "Select a spread to analyze",
            options=whatif_symbols,
            index=0,
            key="whatif_symbol",
        )

        sel_row = sorted_df[sorted_df["Symbol"] == selected_symbol].iloc[0]

        wi_col1, wi_col2, wi_col3, wi_col4 = st.columns(4)
        with wi_col1:
            st.markdown(f"**Spot:** ₹{sel_row['Spot Price']:,.2f}")
            st.markdown(f"**Short Strike:** ₹{sel_row['Short Strike']:,.2f}")
        with wi_col2:
            st.markdown(f"**Long Strike:** ₹{sel_row['Long Strike']:,.2f}")
            st.markdown(f"**Spread Width:** ₹{sel_row['Spread Width']:,.2f}")
        with wi_col3:
            new_short_premium = st.number_input(
                "Short Premium (Sell @)",
                min_value=0.0,
                value=float(sel_row["Short Premium"]),
                step=0.05,
                format="%.2f",
                key="whatif_short_prem",
                help="The premium you'd actually receive for selling the short put",
            )
        with wi_col4:
            new_long_premium = st.number_input(
                "Long Premium (Buy @)",
                min_value=0.0,
                value=float(sel_row["Long Premium"]),
                step=0.05,
                format="%.2f",
                key="whatif_long_prem",
                help="The premium you'd actually pay for buying the long put",
            )

        # --- Recalculate all metrics with new premiums ---
        wi_net_credit = new_short_premium - new_long_premium
        wi_spread_width = sel_row["Short Strike"] - sel_row["Long Strike"]
        wi_max_loss_unit = wi_spread_width - wi_net_credit if wi_net_credit < wi_spread_width else 0.01
        if wi_max_loss_unit <= 0:
            wi_max_loss_unit = 0.01
        wi_roi = (wi_net_credit / wi_max_loss_unit) * 100.0 if wi_net_credit > 0 else 0.0
        wi_breakeven = sel_row["Short Strike"] - wi_net_credit

        # POP stays the same (depends on short strike & spot, not premiums)
        wi_pop = sel_row["POP (%)"] / 100.0  # convert back to fraction
        wi_ev_unit = (wi_pop * wi_net_credit) - ((1 - wi_pop) * wi_max_loss_unit) if wi_net_credit > 0 else 0.0
        wi_lot_size = int(sel_row["Lot Size"]) if pd.notna(sel_row.get("Lot Size")) else None
        wi_ev_lot = wi_ev_unit * wi_lot_size if wi_lot_size else None
        wi_credit_lot = wi_net_credit * wi_lot_size if wi_lot_size else None
        wi_max_loss_lot = wi_max_loss_unit * wi_lot_size if wi_lot_size else None
        wi_ev_capital = (wi_ev_unit / wi_max_loss_unit) * 100.0 if wi_max_loss_unit > 0 and wi_net_credit > 0 else 0.0
        wi_ann_ev = (wi_ev_unit / wi_max_loss_unit) * (365 / max(dte, 1)) * 100.0 if wi_max_loss_unit > 0 and wi_net_credit > 0 else 0.0

        # --- Display results ---
        if wi_net_credit <= 0:
            st.error("⚠️ Net credit is zero or negative — this is not a valid Bull Put Spread at these premiums.")
        else:
            # Original vs What-If comparison
            st.markdown("#### 📊 Original vs What-If")

            cmp_col1, cmp_col2, cmp_col3, cmp_col4, cmp_col5, cmp_col6 = st.columns(6)
            with cmp_col1:
                orig_credit = sel_row["Net Credit"]
                delta_credit = wi_net_credit - orig_credit
                st.metric("Net Credit", f"₹{wi_net_credit:,.2f}", delta=f"₹{delta_credit:,.2f}")
            with cmp_col2:
                orig_max_loss = sel_row["Max Loss / Unit"]
                delta_ml = wi_max_loss_unit - orig_max_loss
                st.metric("Max Loss/Unit", f"₹{wi_max_loss_unit:,.2f}", delta=f"₹{delta_ml:,.2f}", delta_color="inverse")
            with cmp_col3:
                orig_roi = sel_row["ROI (%)"]
                delta_roi = wi_roi - orig_roi
                st.metric("ROI %", f"{wi_roi:,.1f}%", delta=f"{delta_roi:,.1f}%")
            with cmp_col4:
                orig_ev = sel_row["EV / Unit"]
                delta_ev = wi_ev_unit - orig_ev
                st.metric("EV/Unit", f"₹{wi_ev_unit:,.2f}", delta=f"₹{delta_ev:,.2f}")
            with cmp_col5:
                orig_ev_cap = sel_row["EV/Capital (%)"]
                delta_ev_cap = wi_ev_capital - orig_ev_cap
                st.metric("EV/Capital %", f"{wi_ev_capital:,.1f}%", delta=f"{delta_ev_cap:,.1f}%")
            with cmp_col6:
                orig_ann = sel_row["Annualized EV (%)"]
                delta_ann = wi_ann_ev - orig_ann
                st.metric("Ann. EV %", f"{wi_ann_ev:,.1f}%", delta=f"{delta_ann:,.1f}%")

            # Lot-level metrics
            if wi_lot_size:
                lot_col1, lot_col2, lot_col3, lot_col4 = st.columns(4)
                with lot_col1:
                    st.metric("Lot Size", f"{wi_lot_size:,}")
                with lot_col2:
                    st.metric("Credit/Lot", f"₹{wi_credit_lot:,.0f}")
                with lot_col3:
                    st.metric("Max Loss/Lot", f"₹{wi_max_loss_lot:,.0f}")
                with lot_col4:
                    st.metric("EV/Lot", f"₹{wi_ev_lot:,.0f}" if wi_ev_lot else "—")

            st.markdown(f"**Breakeven:** ₹{wi_breakeven:,.2f} | **POP:** {sel_row['POP (%)']:.1f}% *(unchanged — depends on strike, not premium)*")

    # ------------------------------------------------------------------
    # EV distribution chart
    # ------------------------------------------------------------------
    st.markdown("---")
    st.markdown("### 📈 EV Distribution")

    chart_col1, chart_col2 = st.columns(2)

    with chart_col1:
        if "EV / Unit" in filtered.columns and len(filtered) > 0:
            chart_data = filtered[["Symbol", "EV / Unit"]].sort_values("EV / Unit", ascending=True).tail(20)
            st.bar_chart(chart_data.set_index("Symbol")["EV / Unit"], use_container_width=True)

    with chart_col2:
        if "ROI (%)" in filtered.columns and "POP (%)" in filtered.columns and len(filtered) > 0:
            st.scatter_chart(
                filtered[["ROI (%)", "POP (%)", "Symbol"]].set_index("Symbol"),
                x="ROI (%)",
                y="POP (%)",
                use_container_width=True,
            )

    # ------------------------------------------------------------------
    # Sector breakdown
    # ------------------------------------------------------------------
    st.markdown("---")
    st.markdown("### 🏢 Sector Breakdown")
    if len(filtered) > 0:
        sector_stats = (
            filtered.groupby("Sector")
            .agg(
                Count=("Symbol", "count"),
                Avg_ROI=("ROI (%)", "mean"),
                Avg_POP=("POP (%)", "mean"),
                Avg_EV=("EV / Unit", "mean"),
            )
            .round(2)
            .sort_values("Count", ascending=False)
        )
        sector_stats.columns = ["Count", "Avg ROI (%)", "Avg POP (%)", "Avg EV/Unit"]
        st.dataframe(sector_stats, use_container_width=True)

else:
    # ------------------------------------------------------------------
    # Welcome state — no scan run yet
    # ------------------------------------------------------------------
    st.markdown("---")
    st.markdown("""
    ### 👋 How to Use

    1. **Place your NSE cookies** in `cookies.txt` in the project directory
    2. **Adjust settings** in the sidebar (strike %, spread width, filters)
    3. **Click "🚀 Run Scan"** to fetch live data from NSE and rank all FnO stocks

    ### 📐 What Gets Calculated

    | Metric | What It Means |
    |--------|---------------|
    | **Net Credit** | Premium you receive (Short Put premium − Long Put premium) |
    | **Max Loss** | Maximum you can lose per unit (Spread Width − Net Credit) |
    | **ROI (%)** | Return on risk = Net Credit ÷ Max Loss × 100 |
    | **POP (%)** | Probability of Profit — estimated from IV using z-score method |
    | **EV / Unit** | Expected Value = (POP × Profit) − ((1−POP) × Loss). **Must be positive.** |
    | **Safety Margin** | How far OTM your short strike is (% below spot) |
    | **Bid-Ask Spread** | Liquidity indicator — lower is better |

    ### 🧠 Key Rules
    - **EV must be positive** — if Expected Value is negative, the trade loses money long-term even with high win rate
    - **POP is fat-tail adjusted** — we apply a conservative haircut to account for real-world crash risk
    - **ATM IV** is used for expected move (not individual strike IV) — more reliable
    - **Sector concentration** — tool warns you if too many picks are in the same sector
    """)

    st.markdown("---")
    st.markdown(
        "<div style='text-align: center; color: #888; font-size: 0.85rem;'>"
        "Built for Bull Put Spread screening on NSE India | Uses real-time option chain data"
        "</div>",
        unsafe_allow_html=True,
    )

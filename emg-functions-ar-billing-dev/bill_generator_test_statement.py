import os
import logging
from io import BytesIO, StringIO
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from fpdf import FPDF
from office365.runtime.auth.client_credential import ClientCredential
from office365.sharepoint.client_context import ClientContext
from office365.runtime.client_request_exception import ClientRequestException
import re
import csv
from decimal import Decimal, ROUND_HALF_UP
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple
import matplotlib as mpl
from matplotlib.font_manager import FontProperties
from fpdf.enums import XPos, YPos
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
import msal
#from dotenv import load_dotenv

# =========================
# Config
# =========================
#load_dotenv()
FOLDER    = os.getenv("FOLDER")
FOLDER_LOGO = os.getenv("FOLDER_LOGO")

# SQL connection
server = os.getenv("DB_SERVER")
database = os.getenv("DB_DATABASE")
username = os.getenv("DB_USERNAME")
password = os.getenv("DB_PASSWORD")
# History table
HISTORY_TABLE = "dbo.billing_history"
# Skip invoices already present in history (by invoice_number)
SKIP_EXISTING_INVOICES = False

GENERATE_DATE = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")

# If monthly view totals are EX-GST
GST_RATE = 0.00


# =========================
# SharePoint connection
# =========================
def get_sharepoint_context() -> ClientContext:
    """
    Authenticate to SharePoint using Azure AD App (Client Credentials Flow).
    Make sure the app registration has permissions for SharePoint Online (Sites.Selected or Sites.FullControl.All).
    """
    client_id = os.getenv("AZURE_CLIENT_ID")
    client_secret = os.getenv("AZURE_CLIENT_SECRET")
    site_url = os.getenv("SITE_URL")

    creds = ClientCredential(client_id, client_secret)
    ctx = ClientContext(site_url).with_credentials(creds)
    return ctx

def upload_bytes_to_sharepoint(file_bytes: bytes, remote_filename: str) -> None:
    ctx = get_sharepoint_context()
    folder = ctx.web.get_folder_by_server_relative_url(FOLDER)
    try:
        folder.upload_file(remote_filename, file_bytes).execute_query()
        print(f"✅ Uploaded {remote_filename} to SharePoint")
    except ClientRequestException as ex:
        print(f"SharePoint error: {ex}")
    except Exception as e:
        print(f"Unexpected error uploading {remote_filename}: {e}")

# =========================
# SharePoint log
# =========================
class SharePointLogHandler(logging.Handler):
    """Custom logging handler that appends logs to a persistent file on SharePoint."""
    def __init__(self, remote_filename: str):
        super().__init__()
        self.remote_filename = remote_filename
        self.buffer = StringIO()

        # Try to download existing log
        existing = download_file_from_sharepoint(self.remote_filename)
        if existing:
            try:
                self.buffer.write(existing.decode("utf-8"))
            except Exception:
                # If file not UTF-8, start fresh
                pass

    def emit(self, record):
        try:
            msg = self.format(record)
            self.buffer.write(msg + "\n")
            # Upload the entire buffer back to SharePoint
            upload_bytes_to_sharepoint(
                self.buffer.getvalue().encode("utf-8"),
                self.remote_filename
            )
        except Exception as e:
            print(f"Logging error: {e}")

# =========================
# SharePoint helper
# =========================
def upload_bytes_to_sharepoint(file_bytes: bytes, filename: str) -> None:
    try:
        ctx = get_sharepoint_context()
        folder = ctx.web.get_folder_by_server_relative_url(FOLDER)
        folder.upload_file(filename, file_bytes).execute_query()
        logging.info(f"✅ Uploaded {filename} to SharePoint")
    except ClientRequestException as e:
        logging.error(f"SharePoint error uploading {filename}: {e}")
        raise
    except Exception as e:
        logging.error(f"Unexpected error uploading {filename}: {e}")
        raise

# =========================
# Style / Colors
# =========================
custom_colors = {
    "Firm Gas Sales": "#0089D0",
    "Spot Gas Sales": "#9DC0D7",
    "Transport Fee": "#425B7E",
    "Distribution Charges": "#F4A261",
    "Adjustment Charges": "#E76F51",
    "Other Charges": "#2A9D8F"
}
mpl.rcParams["font.family"] = "Arial"


# =========================
# SQL helpers
# =========================
def get_engine():
    return create_engine(
        f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+18+for+SQL+Server",
        fast_executemany=True
    )

def ensure_billing_history_table(engine) -> None:
    q = """
    SELECT 1
    FROM sys.objects
    WHERE object_id = OBJECT_ID(N'dbo.billing_history')
      AND type = N'U'
    """
    with engine.begin() as con:
        row = con.exec_driver_sql(q).fetchone()
        if row is None:
            raise RuntimeError("Table dbo.billing_history does not exist. Please create it first.")

def load_views() -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Returns:
        monthly_df: dbo.vw_test_charges_monthly (1 row per invoice)
        breakdown_df: dbo.vw_test_charges_breakdown (charge lines)
        daily_df: dbo.vw_billing_charges_daily (daily consumption)
    """
    eng = get_engine()
    with eng.begin() as conn:
        logging.info("Connected via SQLAlchemy engine.")
        monthly_df = pd.read_sql("SELECT * FROM dbo.vw_test_charges_monthly;", conn)
        breakdown_df = pd.read_sql("SELECT * FROM dbo.vw_test_charges_breakdown;", conn)
        try:
            daily_df = pd.read_sql("SELECT * FROM dbo.vw_billing_charges_daily;", conn)
        except Exception:
            logging.warning("vw_billing_charges_daily not found; consumption chart will be skipped.")
            daily_df = pd.DataFrame()

    logging.info(f"monthly rows={len(monthly_df)}, breakdown rows={len(breakdown_df)}, daily rows={len(daily_df)}")
    return monthly_df, breakdown_df, daily_df


# =========================
# Header & detail shaping
# =========================
def build_invoice_headers_from_monthly(monthly: pd.DataFrame) -> pd.DataFrame:
    """
    Forward the monthly view as the canonical header/totals dataset.
    """
    if monthly.empty:
        return pd.DataFrame()

    df = monthly.copy()

    money_cols = [
        "firm_gas_amount","spot_gas_amount",
        "transport_firm_amount","transport_overrun_amount",
        "atco_usage_amount","atco_demand_amount","atco_standing_amount",
        "gas_adjustment_charges","distribution_adjustment_charges","regulatory_adjustment_charges",
        "admin_fee","late_payment_fee","total_amount","gst_amount","total_in_gst_amount"
    ]
    for c in money_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0).round(2)

    df = df.dropna(subset=["invoice_number"])
    return df.reset_index(drop=True)

def build_charges_df_from_breakdown(invoice_lines: pd.DataFrame) -> pd.DataFrame:
    """
    Convert raw breakdown rows to the table schema expected by the PDF section.
    """
    if invoice_lines.empty:
        return pd.DataFrame(columns=[
            "Charge Category","Charge Type","Rate","Rate UOM","Unit","Unit UOM","Amount ex GST","Statement Amount ex GST"
        ])
    df = invoice_lines.copy()
    out = pd.DataFrame({
        "Charge Category": df["charge_category"] if "charge_category" in df.columns else pd.Series(["Other"] * len(df)),
        "Charge Type": df["charge_type"] if "charge_type" in df.columns else pd.Series([""] * len(df)),
        "Rate": df["rate"] if "rate" in df.columns else pd.Series([""] * len(df)),
        "Rate UOM": df["rate_uom"] if "rate_uom" in df.columns else pd.Series([""] * len(df)),
        "Unit": df["unit"] if "unit" in df.columns else pd.Series([""] * len(df)),
        "Unit UOM": df["unit_uom"] if "unit_uom" in df.columns else pd.Series([""] * len(df)),
        "Amount ex GST": df["amount"] if "amount" in df.columns else pd.Series([""] * len(df)),
        "Statement Amount ex GST": df["statement_total_amount"] if "statement_total_amount" in df.columns else pd.Series([""] * len(df)),
    })
    order = ["Firm Gas Sales", "Spot Gas Sales", "Transport Fee", "Distribution Charges", "Adjustment Charges", "Other Charges"]
    out["Charge Category"] = pd.Categorical(out["Charge Category"], order, ordered=True)
    out = out.sort_values(["Charge Category","Charge Type"]).reset_index(drop=True)
    return out


# Mapping breakdown -> billing_history columns
CHARGE_TYPE_TO_COL = {
    "Firm Gas Sales":                       "firm_gas_amount",
    "Spot Gas Sales":                       "spot_gas_amount",
    "Transport Fee on Firm Capacity":       "transport_firm_amount",
    "Deliveries above Firm Capacity":       "transport_overrun_amount",
    "ATCO Usage Charges":                   "atco_usage_amount",
    "ATCO Demand Charges":                  "atco_demand_amount",
    "ATCO Standing Charges":                "atco_standing_amount",
    "Gas Adjustment Charges":               "gas_adjustment_charges",
    "Distribution Adjustment Charges":      "distribution_adjustment_charges",
    "Regulatory Adjustment Charges":        "regulatory_adjustment_charges",
    "Admin Fee":                            "admin_fee",
    "Late Payment Fee":                     "late_payment_fee",
}

def build_history_row_from_monthly(m: pd.Series) -> Dict[str, Any]:
    def get(*names, **kw):
        default = kw.get("default", None)
        for n in names:
            if n in m and pd.notna(m[n]):
                return m[n]
        return default

    bs = pd.to_datetime(get("bill_start_date", "start_date"), errors="coerce")
    be = pd.to_datetime(get("bill_end_date", "end_date"), errors="coerce")

    billing_days = get("billing_days")
    if pd.isna(billing_days) and bs is not None and be is not None:
        billing_days = abs((be.normalize() - bs.normalize()).days) + 1

    gj_consumption = get("gj_consumption", "consumption_gj", "total_gj", default=None)

    total_ex = get("total_amount", "total_ex_gst", default=0)
    gst_amt = get("gst_amount", "gst", default=0)
    total_inc = get("total_in_gst_amount", "total_inc_gst")
    if total_inc is None:
        total_inc = float(total_ex or 0) + float(gst_amt or 0)

    statement_total_ex = get("statement_total_amount", "statement_total_ex_gst", default=0)
    statement_gst_amt = get("statement_gst_amount", "statement_gst", default=0)
    statement_total_inc = get("statement_total_in_gst_amount", "statement_total_inc_gst")
    if statement_total_inc is None:
        statement_total_inc = float(statement_total_ex or 0) + float(statement_gst_amt or 0)

    h: Dict[str, Any] = {
        "inv_agg_code": get("inv_agg_code"),
        "itemlised": get("itemlised", default=""),
        "statement_number": get("statement_number"),
        "invoice_number": get("invoice_number"),
        "purchase_order_number": get("purchase_order_number", "po_number"),
        "company_name": get("company_name"),
        "company_code": get("company_code"),
        "account_number": get("account_number"),
        "mirn": get("mirn"),
        "distributor": get("distributor", "network", "distributor_name"),
        "bill_start_date": bs,
        "bill_end_date": be,
        "billing_days": billing_days,
        "bill_issue_date": pd.to_datetime(get("bill_issue_date", "issue_date"), errors="coerce"),
        "gj_consumption": float(get("gj_consumption", default=0) or 0),
        "firm_gas_amount": float(get("firm_gas_amount", default=0) or 0),
        "spot_gas_amount": float(get("spot_gas_amount", default=0) or 0),
        "atco_usage_amount": float(get("atco_usage_amount", default=0) or 0),
        "atco_demand_amount": float(get("atco_demand_amount", default=0) or 0),
        "atco_standing_amount": float(get("atco_standing_amount", default=0) or 0),
        "transport_firm_amount": float(get("transport_firm_amount", default=0) or 0),
        "transport_overrun_amount": float(get("transport_overrun_amount", default=0) or 0),
        "gas_adjustment_charges": float(get("gas_adjustment_charges", default=0) or 0),
        "distribution_adjustment_charges": float(get("distribution_adjustment_charges", default=0) or 0),
        "regulatory_adjustment_charges": float(get("regulatory_adjustment_charges", default=0) or 0),
        "admin_fee": float(get("admin_fee", default=0) or 0),
        "late_payment_fee": float(get("late_payment_fee", default=0) or 0),
        "total_amount": float(total_ex or 0),
        "gst_amount": float(gst_amt or 0),
        "total_in_gst_amount": float(total_inc or 0),
        "statement_total_amount": float(total_ex or 0),
        "statement_gst_amount": float(gst_amt or 0),
        "statement_total_in_gst_amount": float(total_inc or 0),
        "generated_at_utc": datetime.utcnow(),
    }
    return h

# =========================
# Charts
# =========================
def generate_pie_chart(breakdown_series: pd.Series, custom_colors_map: Dict[str, str]) -> BytesIO:
    """
    Generates a styled pie chart with:
      - Percentage labels only
      - Colored dots around the pie
      - Two-column legend with matching dots below the chart
    Returns:
        BytesIO: PNG image data ready for embedding in PDF
    """
    if breakdown_series is None or breakdown_series.empty:
        return BytesIO()

    labels = breakdown_series.index.to_list()

    colors = [custom_colors_map.get(cat, "#2A3240") for cat in labels]

    fig, ax = plt.subplots(figsize=(3.8, 3.8))

    wedges, _, autotexts = ax.pie(
        breakdown_series.values,
        autopct=lambda p: f"{int(round(p))}%",  # integer percent inline
        startangle=90,
        pctdistance=1.12,
        colors=colors,
        textprops={"fontsize": 8, "color": "black"}
    )

    ax.axis("equal")

    # Custom label layout (colored dot + percentage)
    for i, autotext in enumerate(autotexts):
        color = wedges[i].get_facecolor()
        percent = autotext.get_text()
        autotext.set_text("")
        x, y = autotext.get_position()
        ax.text(x - 0.04, y, "●", color=color, fontsize=9, ha="center", va="center")
        ax.text(x + 0.04, y, percent, color="black", fontsize=8, ha="left", va="center")

    # Build custom legend below
    handles = [
        plt.Line2D([0], [0], marker="o", color="w", label=cat,
                   markerfacecolor=col, markersize=8)
        for cat, col in zip(labels, colors)
    ]

    fig.legend(
        handles=handles,
        labels=labels,
        loc="lower center",
        bbox_to_anchor=(0.5, 0.15),
        ncol=2,
        frameon=False,
        fontsize=8
    )

    plt.subplots_adjust(bottom=0.25)

    # Save to BytesIO instead of file
    buf = BytesIO()
    fig.savefig(buf, format="png", dpi=300, bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    return buf

def generate_consumption_chart(
    mirn: str,
    billing_period: str,
    cust_consumption_df: pd.DataFrame,
    contract_mdq: Optional[float],
    fig_height_mm: float = 80.0
) -> BytesIO:
    if cust_consumption_df.empty:
        return BytesIO()

    df = cust_consumption_df.copy()
    df["gas_date"] = pd.to_datetime(df["gas_date"], errors="coerce")
    df["gj_consumption"] = pd.to_numeric(df["gj_consumption"], errors="coerce")
    df = df.dropna(subset=["gas_date", "gj_consumption"]).sort_values("gas_date")
    if df.empty:
        return BytesIO()

    start_date = df["gas_date"].min()
    end_date = df["gas_date"].max()

    fig_width_mm = 180.0
    fig = plt.figure(figsize=(fig_width_mm / 25.4, fig_height_mm / 25.4))
    ax = fig.add_subplot(111)

    light_blue = "#f0f8ff"
    fig.patch.set_facecolor(light_blue)
    ax.set_facecolor(light_blue)

    fig.subplots_adjust(left=0.12, right=0.98, top=0.94, bottom=0.32)

    ax.set_title(f"MIRN: {mirn}    Period: {billing_period}", fontsize=7, fontweight="normal", pad=4)

    ax.bar(df["gas_date"], df["gj_consumption"], label="Daily Consumption", alpha=0.95, color="#0089D0")

    if contract_mdq is not None and np.isfinite(float(contract_mdq)) and float(contract_mdq) > 0:
        ax.axhline(
            y=float(contract_mdq),
            linewidth=1.5,
            color="#000000",
            label="Contract MDQ",
            zorder=3
        )

    ax.set_xlim(start_date, end_date)
    ax.xaxis.set_major_locator(mdates.DayLocator(interval=1))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%d %b"))
    ax.margins(x=0.005)

    ax.tick_params(axis="x", labelrotation=45, labelsize=6, pad=3, length=0)
    for lbl in ax.get_xticklabels():
        lbl.set_fontweight("bold")

    max_y = float(df["gj_consumption"].max() or 0)
    mdq_y = float(contract_mdq or 0)
    ymax = max(max_y, mdq_y)
    ax.set_ylim(0, ymax * 1.08 if ymax > 0 else 10)
    ax.set_axisbelow(True)
    ax.grid(which="both", axis="both", linestyle="--", linewidth=0.5)
    ax.tick_params(axis="y", labelsize=6)
    for s in ("top", "right", "left"):
        ax.spines[s].set_visible(False)
    ax.spines["bottom"].set_linewidth(1)

    handles, labels = ax.get_legend_handles_labels()
    if handles:
        fig.legend(
            handles, labels,
            loc="lower center",
            bbox_to_anchor=(0.5, -0.1),
            ncol=max(1, len(labels)),
            frameon=False,
            prop=FontProperties(size=6, weight="bold")
        )

    buf_c = BytesIO()
    fig.savefig(buf_c, format="png", dpi=300, bbox_inches="tight", facecolor=fig.get_facecolor())
    buf_c.seek(0)
    plt.close(fig)
    return buf_c

def generate_accounts_mirn_chart(billing_period: str, df: pd.DataFrame, selected_mirns: list) -> BytesIO:
    if df.empty:
        return BytesIO()

    use = df.copy()
    use["gas_date"] = pd.to_datetime(use["gas_date"], errors="coerce")
    use["gj_consumption"] = pd.to_numeric(use["gj_consumption"], errors="coerce")
    use["mirn"] = use["mirn"].astype(str)
    use = use.dropna(subset=["gas_date", "gj_consumption"]).sort_values("gas_date")

    if use.empty:
        return BytesIO()
    
    use = use[use["mirn"].isin(selected_mirns)]
    if use.empty:
        return BytesIO()
    
    piv = use.pivot_table(index="gas_date", columns="mirn", values="gj_consumption", aggfunc="sum")

    start_date = piv.index.min()
    end_date   = piv.index.max()
    if pd.isna(start_date) or pd.isna(end_date):
        return BytesIO()
    

    fig_width_mm = 180
    fig_height_mm: float = 32.0
    fig = plt.figure(figsize=(fig_width_mm / 25.4, fig_height_mm / 25.4))
    ax = fig.add_subplot(111)

    light_blue = "#f0f8ff"
    fig.patch.set_facecolor(light_blue)
    ax.set_facecolor(light_blue)

    fig.subplots_adjust(left=0.12, right=0.98, top=0.94, bottom=0.32)

    ax.set_title(f"MIRN: Aggregated Consumption    Period: {billing_period}",
                 fontsize=6, fontweight="normal", pad=4)

    cols = sorted(list(piv.columns))
    x = piv.index
    bottom = np.zeros(len(piv), dtype=float)

    color_map = {
        "5600002119_8": "#2A9D8F",
        "5600002162_7": "#FE9666",
        "5600462393_7": "#A0A0A0",
    }
    fallback_palette = ["#264653"]

    for i, c in enumerate(cols):
        y = piv[c].values.astype(float)
        ax.bar(
            x, y,
            bottom=bottom,
            label=c,
            alpha=0.95,
            color=color_map.get(str(c), fallback_palette[i % len(fallback_palette)])
        )
        bottom += np.nan_to_num(y, nan=0.0)

    ax.axhline(y=3000, linewidth=1.5, color="#000000", label="Aggregated MDQ.", zorder=3)

    ax.set_xlim(start_date, end_date)
    ax.xaxis.set_major_locator(mdates.DayLocator(interval=1))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%d %b"))
    ax.margins(x=0.005)

    ax.tick_params(axis="x", labelrotation=45, labelsize=6, pad=3, length=0)
    for lbl in ax.get_xticklabels():
        lbl.set_fontweight("bold")

    max_stack = float(np.nanmax(bottom)) if len(bottom) else 0.0
    ymax = max(max_stack, 3000.0)
    ax.set_ylim(0, ymax * 1.08 if ymax > 0 else 10)
    ax.set_axisbelow(True)
    ax.grid(which="both", axis="both", linestyle="--", linewidth=0.5)
    ax.tick_params(axis="y", labelsize=6)
    for s in ("top", "right", "left"):
        ax.spines[s].set_visible(False)
    ax.spines["bottom"].set_linewidth(1)

    # Adding legend for 'mirn'
    handles, labels = ax.get_legend_handles_labels()
    ncols = max(1, len(labels))
    fig.legend(
        handles, labels,
        loc="lower center",
        bbox_to_anchor=(0.5, -0.1),
        ncol=ncols,
        frameon=False,
        prop=FontProperties(size=6, weight="bold")
    )

    buf_m = BytesIO()
    fig.savefig(buf_m, format="png", dpi=300, bbox_inches="tight", facecolor=fig.get_facecolor())
    buf_m.seek(0)
    plt.close(fig)
    return buf_m

def embed_chart_in_pdf(pdf: FPDF, chart_buf: BytesIO, chart_height_mm: float) -> None:
    """
    Embeds a chart (from a BytesIO buffer) into the PDF.
    
    Args:
        pdf (FPDF): The PDF document to embed the chart into.
        chart_buf (BytesIO): The BytesIO buffer containing the chart image data.
        chart_height_mm (float): The desired height for the chart in mm.
    """
    # Check the size of the chart buffer before embedding
    chart_size = len(chart_buf.getvalue())
    if chart_size == 0:
        logger.error("The chart buffer is empty. Skipping chart embedding.")
        return

    y_start = pdf.get_y()  # Current y position of the PDF
    CHART_W_MM = 180.0
    pdf.image(chart_buf, x=10, y=y_start, w=CHART_W_MM, h=chart_height_mm)
    pdf.set_y(y_start + chart_height_mm + 2)  # Update the y position for the next element
    logger.info(f"Chart embedded at y={y_start}, height={chart_height_mm}")

# =========================
# Writeback helpers
# =========================
def _to_str_or_none(x: Any) -> Optional[str]:
    s = None if x is None or (isinstance(x, float) and np.isnan(x)) else str(x).strip()
    return s if s else None

def _to_float_or_none(x: Any) -> Optional[float]:
    try:
        if x is None or (isinstance(x, float) and np.isnan(x)):
            return None
        return float(x)
    except Exception:
        return None

def _to_int_or_none(x: Any) -> Optional[int]:
    try:
        if x is None or (isinstance(x, float) and np.isnan(x)):
            return None
        return int(x)
    except Exception:
        return None

def _to_date_or_none(x: Any) -> Optional[date]:
    try:
        if x is None or (isinstance(x, float) and np.isnan(x)):
            return None
        dt = pd.to_datetime(x, errors="coerce")
        if pd.isna(dt):
            return None
        return dt.date()
    except Exception:
        return None

def _to_datetime_or_none(x: Any) -> Optional[datetime]:
    if x is None:
        return None
    try:
        dt = pd.to_datetime(x, errors="coerce")
        if pd.isna(dt):
            return None
        return dt.to_pydatetime().replace(tzinfo=None)
    except Exception:
        return None

def sanitize_history_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    cleaned: List[Dict[str, Any]] = []
    for r in rows:
        out: Dict[str, Any] = {
            "statement_number":                _to_str_or_none(r.get("statement_number")),
            
            "invoice_number":                  _to_str_or_none(r.get("invoice_number")),
            "purchase_order_number":           _to_str_or_none(r.get("purchase_order_number")),
            "company_name":                    _to_str_or_none(r.get("company_name")),
            "company_code":                    _to_str_or_none(r.get("company_code")),
            "account_number":                  _to_str_or_none(r.get("account_number")),
            "mirn":                            _to_str_or_none(r.get("mirn")),
            "distributor":                     _to_str_or_none(r.get("distributor")),
            "bill_start_date":                 _to_date_or_none(r.get("bill_start_date")),
            "bill_end_date":                   _to_date_or_none(r.get("bill_end_date")),
            "billing_days":                    _to_int_or_none(r.get("billing_days")),
            "bill_issue_date":                 _to_date_or_none(r.get("bill_issue_date")),
            "firm_gas_amount":                 _to_float_or_none(r.get("firm_gas_amount")),
            "spot_gas_amount":                 _to_float_or_none(r.get("spot_gas_amount")),
            "atco_usage_amount":               _to_float_or_none(r.get("atco_usage_amount")),
            "atco_demand_amount":              _to_float_or_none(r.get("atco_demand_amount")),
            "atco_standing_amount":            _to_float_or_none(r.get("atco_standing_amount")),
            "transport_firm_amount":           _to_float_or_none(r.get("transport_firm_amount")),
            "transport_overrun_amount":        _to_float_or_none(r.get("transport_overrun_amount")),
            "gas_adjustment_charges":          _to_float_or_none(r.get("gas_adjustment_charges")),
            "distribution_adjustment_charges": _to_float_or_none(r.get("distribution_adjustment_charges")),
            "regulatory_adjustment_charges":   _to_float_or_none(r.get("regulatory_adjustment_charges")),
            "admin_fee":                       _to_float_or_none(r.get("admin_fee")),
            "late_payment_fee":                _to_float_or_none(r.get("late_payment_fee")),
            "total_amount":                    _to_float_or_none(r.get("total_amount")),
            "gst_amount":                      _to_float_or_none(r.get("gst_amount")),
            "total_in_gst_amount":             _to_float_or_none(r.get("total_in_gst_amount")),
            "generated_at_utc":                _to_datetime_or_none(r.get("generated_at_utc") or datetime.utcnow()),
        }
        if not out["invoice_number"]:
            continue
        cleaned.append(out)
    return cleaned

def _norm2(x: Any) -> Decimal:
    """Two-decimal money for equality compares."""
    if x is None or (isinstance(x, float) and pd.isna(x)):
        x = "0"
    return Decimal(str(x)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

def _fetch_history_variants(engine, base_invoice_number: str) -> List[Tuple[str, int, Decimal]]:
    """
    Returns list of tuples (invoice_number, suffix_int, total_inc_norm)
    Only rows where invoice_number == base OR base_\\d+ are kept.
    """
    like = base_invoice_number + "%"
    sql = f"SELECT invoice_number, total_in_gst_amount FROM {HISTORY_TABLE} WHERE invoice_number LIKE ?"
    pat = re.compile(r"^" + re.escape(base_invoice_number) + r"(?:_(\d+))?$")

    with engine.begin() as con:
        rows = con.exec_driver_sql(sql, (like,)).fetchall()

    variants: List[Tuple[str, int, Decimal]] = []
    for inv, total_inc in rows:
        m = pat.match(inv or "")
        if not m:
            continue
        sfx = int(m.group(1) or "1")  # base counted as 1
        variants.append((inv, sfx, _norm2(total_inc)))
    return variants

def should_process_statement(eng, statement_number: str, total_in_gst_amount: float) -> Tuple[bool, str]:
    base = statement_number
    curr_norm = _norm2(total_in_gst_amount)

    # 1️⃣ Check SharePoint
    if sharepoint_file_exists(f"{base}.pdf"):
        prev_total = _get_sharepoint_total(base)
        if prev_total and _norm2(prev_total) == curr_norm:
            logger.info(f"Statement {base}.pdf already exists in SharePoint with same total; skipping.")
            return False, base
        else:
            logger.info(f"Statement {base}.pdf exists but total differs — applying increment.")
            suffix = _get_next_suffix_from_sharepoint(base)
            return True, f"{base}_{suffix}"

    # 2️⃣ Check DB history
    variants = _fetch_history_variants(eng, base)
    for inv_no, _, tot_norm in variants:
        if tot_norm == curr_norm:
            logger.info(f"Statement {base} already in DB with same total; skipping.")
            return False, inv_no

    # 3️⃣ Otherwise new
    next_suffix = (max([v[1] for v in variants]) + 1) if variants else 0
    new_statement_number = f"{base}_{next_suffix}" if next_suffix else base
    return True, new_statement_number


def _apply_history_increment_rule(engine, row: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], str, bool]:
    """
    One history row per invoice_number unless the *same invoice_number* has a different total:
      - If any existing variant (base or suffixed) has the SAME total -> SKIP insert and return that variant's invoice_number.
      - Else create next suffix and INSERT.
      - If no variants exist -> INSERT base as-is.

    Returns: (row_to_insert_or_None, invoice_number_used, inserted_bool)
    """
    base = row.get("invoice_number")
    if not base:
        return None, "", False

    curr_total_inc = row.get("total_in_gst_amount")
    if curr_total_inc is None:
        curr_total_inc = (float(row.get("total_amount") or 0) + float(row.get("gst_amount") or 0))
    curr_norm = _norm2(curr_total_inc)

    variants = _fetch_history_variants(engine, base)
    if not variants:
        return row, base, True

    for inv_no, _sfx, tot_norm in variants:
        if tot_norm == curr_norm:
            return None, inv_no, False

    next_suffix = max(v[1] for v in variants) + 1
    r2 = dict(row)
    r2["invoice_number"] = f"{base}_{next_suffix}"
    return r2, r2["invoice_number"], True

def insert_billing_history_batch(engine, rows: List[Dict[str, Any]]) -> List[str]:
    """
    Inserts one billing_history record per invoice.
    Matches dbo.billing_history schema.
    Skips duplicates based on invoice_number.
    """
    if not rows:
        logging.info("No billing_history rows to insert.")
        return []

    rows = sanitize_history_rows(rows)
    inserted_invoices = []

    cols = [
        "inv_agg_code", "itemlised", "statement_number", "invoice_number",
        "purchase_order_number", "company_name", "company_code", "account_number",
        "mirn", "distributor", "bill_start_date", "bill_end_date", "billing_days",
        "bill_issue_date", "gj_consumption", "firm_gas_amount", "spot_gas_amount",
        "atco_usage_amount", "atco_demand_amount", "atco_standing_amount",
        "transport_firm_amount", "transport_overrun_amount", "gas_adjustment_charges",
        "distribution_adjustment_charges", "regulatory_adjustment_charges",
        "admin_fee", "late_payment_fee", "total_amount", "gst_amount",
        "total_in_gst_amount", "statement_total_amount", "statement_gst_amount",
        "statement_total_in_gst_amount", "generated_at_utc"
    ]

    placeholders = ",".join(["?"] * len(cols))

    sql = f"""
    INSERT INTO {HISTORY_TABLE} ({','.join(cols)})
    SELECT {placeholders}
    WHERE NOT EXISTS (
        SELECT 1 FROM {HISTORY_TABLE} WHERE invoice_number = ?
    )
    """

    with engine.begin() as con:
        dbname = con.exec_driver_sql("SELECT DB_NAME()").scalar()
        before = con.exec_driver_sql(f"SELECT COUNT(*) FROM {HISTORY_TABLE}").scalar()
    logging.info(f"Target DB: {dbname}; {HISTORY_TABLE} count before insert: {before}")
    logging.info(f"Inserting {len(rows)} rows")

    inserted = 0
    with engine.begin() as con:
        for r in rows:
            inv_no = r.get("invoice_number")
            params = tuple(r.get(c) for c in cols) + (inv_no,)
            result = con.exec_driver_sql(sql, params)
            if result.rowcount > 0:
                inserted += 1
                inserted_invoices.append(inv_no)
            else:
                logging.info(f"Duplicate detected, skipping invoice {inv_no}")

    with engine.begin() as con:
        after = con.exec_driver_sql(f"SELECT COUNT(*) FROM {HISTORY_TABLE}").scalar()

    logging.info(f"Inserted {inserted} new rows. After count: {after} (delta={after - before})")

    return inserted_invoices

# =========================
# SharePoint file download
# =========================
def download_file_from_sharepoint(remote_filename: str) -> bytes | None:
    try:
        ctx = get_sharepoint_context()
        folder = ctx.web.get_folder_by_server_relative_url(FOLDER)
        file = folder.files.get_by_url(remote_filename)

        buf = BytesIO()
        file.download(buf).execute_query()
        buf.seek(0)
        return buf.read()
    except Exception as e:
        print(f"Error downloading {remote_filename}: {e}")
        return None
    
# =========================
# SharePoint duplicate helpers
# =========================

def sharepoint_file_exists(filename: str) -> bool:
    """Return True if a file with this name exists in the SharePoint folder."""
    try:
        ctx = get_sharepoint_context()
        folder = ctx.web.get_folder_by_server_relative_url(FOLDER)
        files = folder.files
        ctx.load(files)
        ctx.execute_query()

        for f in files:
            if f.name.lower() == filename.lower():
                return True
        return False
    except Exception as e:
        logging.warning(f"SharePoint existence check failed for {filename}: {e}")
        return False


def _get_next_suffix_from_sharepoint(base_filename: str) -> int:
    """Find the next available suffix (e.g., base_1.pdf, base_2.pdf) in SharePoint folder."""
    try:
        ctx = get_sharepoint_context()
        folder = ctx.web.get_folder_by_server_relative_url(FOLDER)
        files = folder.files
        ctx.load(files)
        ctx.execute_query()

        pattern = re.compile(rf"^{re.escape(base_filename)}(?:_(\d+))?\.pdf$", re.IGNORECASE)
        suffixes = []
        for f in files:
            m = pattern.match(f.name)
            if m:
                suffixes.append(int(m.group(1) or 0))

        return max(suffixes) + 1 if suffixes else 1
    except Exception as e:
        logging.warning(f"Suffix lookup failed for {base_filename}: {e}")
        return 1


def _get_sharepoint_total(base_filename: str) -> Optional[Decimal]:
    """
    Placeholder stub for retrieving statement total from SharePoint metadata or a log file.
    You can later extend this to read metadata or a stored CSV/JSON alongside the PDF.
    For now, return None to rely on DB comparisons only.
    """
    return None

# =========================
# DEBUG helpers
# =========================
def debug_describe_table(engine, table: str = "dbo.billing_history") -> None:
    print("\n[DEBUG] Table schema check …")
    with engine.begin() as con:
        exists = con.exec_driver_sql(
            "SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(?) AND type='U';",
            (table,)
        ).fetchone()
        print(f"  - exists: {bool(exists)}")

        perms = con.exec_driver_sql(
            "SELECT HAS_PERMS_BY_NAME(?, 'OBJECT', 'INSERT');",
            (table,)
        ).scalar()
        print(f"  - INSERT permission: {bool(perms)}")

        cnt = con.exec_driver_sql(f"SELECT COUNT(*) FROM {table};").scalar()
        print(f"  - current rows: {cnt}")

        cols = con.exec_driver_sql(
            """
            SELECT COLUMN_NAME, IS_NULLABLE, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA='dbo' AND TABLE_NAME='billing_history'
            ORDER BY ORDINAL_POSITION
            """
        ).fetchall()
        print("  - columns:")
        for c in cols:
            name, isnull, dtype, clen = c
            clen_str = "" if clen is None else f"({clen})"
            print(f"      {name:32s} {dtype}{clen_str}  NULLABLE={isnull}")
    print("")

def debug_preview_history_rows(history_rows: List[Dict[str, Any]], limit: int = 1) -> None:
    print(f"[DEBUG] history_rows prepared: {len(history_rows)}")
    if not history_rows:
        return
    for i, r in enumerate(history_rows[:limit]):
        print(f"\n[DEBUG] Sample row #{i+1}:")
        for k, v in r.items():
            print(f"   {k:32s} = {repr(v)}   (type={type(v).__name__})")

def debug_validate_not_nulls(engine, row: Dict[str, Any]) -> None:
    print("\n[DEBUG] Nullability validation against INFORMATION_SCHEMA …")
    not_null_cols: List[str] = []
    with engine.begin() as con:
        rows = con.exec_driver_sql(
            """
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA='dbo' AND TABLE_NAME='billing_history'
              AND IS_NULLABLE='NO' AND COLUMN_NAME <> 'id'
            """
        ).fetchall()
        not_null_cols = [r[0] for r in rows]
    issues: List[str] = []
    for c in not_null_cols:
        if row.get(c) is None:
            issues.append(c)
    if issues:
        print(f"  - FAIL: These NOT NULL columns are None: {issues}")
    else:
        print("  - PASS: All NOT NULL columns are non-null for the sample row.")

def compute_aggregated_mdq(daily_df: pd.DataFrame,
                           selected_mirns: list,
                           start_date: Optional[pd.Timestamp],
                           end_date: Optional[pd.Timestamp]) -> Optional[float]:
    """
    Max daily GJ consumption across the selected MIRNs within the billing period.
    """
    if daily_df.empty or not selected_mirns:
        return None

    df = daily_df.copy()
    df["gas_date"] = pd.to_datetime(df["gas_date"], errors="coerce")
    df["gj_consumption"] = pd.to_numeric(df["gj_consumption"], errors="coerce")
    df["mirn"] = df["mirn"].astype(str)
    df = df.dropna(subset=["gas_date", "gj_consumption"])

    # Filter to selected MIRNs and the invoice's billing window
    df = df[df["mirn"].isin([str(m) for m in selected_mirns])]
    if pd.notna(start_date):
        df = df[df["gas_date"] >= start_date.normalize()]
    if pd.notna(end_date):
        df = df[df["gas_date"] <= end_date.normalize()]

    if df.empty:
        return None

    # Sum all selected MIRNs per day, then take the maximum day total
    daily_totals = df.groupby("gas_date", as_index=False)["gj_consumption"].sum()
    return float(daily_totals["gj_consumption"].max()) if not daily_totals.empty else None

def write_label_value(pdf,
        label: str, value: Any, x: Optional[float] = None, y: Optional[float] = None,
        label_w: float = 35, value_w: Optional[float] = None, align: str = "L",
        bold_size: float = 9, reg_size: float = 9, wrap: bool = False,
        force_two_lines: bool = False, suffix: str = "", unit: str = "",
        move_down: bool = True, line_height: float = 5
    ) -> None:
        if x is not None and y is not None:
            pdf.set_xy(x, y)
        elif x is not None:
            pdf.set_x(x)
        pdf.set_font("Arial", "B", bold_size)
        pdf.cell(label_w, line_height, str(label), new_x=XPos.RIGHT, new_y=YPos.TOP, align=align)
        value_str = "" if (value is None or str(value).strip() == "") else str(value) + unit + suffix
        pdf.set_font("Arial", "", reg_size)                
        if wrap or force_two_lines:
            current_y = pdf.get_y()
            val_x = pdf.get_x()
            if value_w is None:
                value_w = 190 - val_x - 10
            pdf.set_xy(val_x, current_y)
            pdf.multi_cell(value_w, line_height, value_str)

            # Always consume two lines if force_two_lines=True
            if force_two_lines:
                lines_used = max(2, int((pdf.get_y() - current_y) / line_height + 0.5))
                target_y = current_y + (2 * line_height)
                pdf.set_y(target_y)
            elif move_down:
                pdf.set_y(pdf.get_y())

        else:
            if value_w:
                pdf.cell(value_w, line_height, value_str, new_x=XPos.LMARGIN, new_y=YPos.NEXT, align=align)
            else:
                pdf.cell(0, line_height, value_str, new_x=XPos.LMARGIN, new_y=YPos.NEXT, align=align)


# =========================
# PDF Class
# =========================
class PDF(FPDF):
    def __init__(self, logo_filename="Agora Logo.png", *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Download logo from the SharePoint folder specified by FOLDER_LOGO
        self.logo_bytes = self.download_file_from_sharepoint(logo_filename)

    def download_file_from_sharepoint(self, logo_filename):
        ctx = get_sharepoint_context()
        try:
            file = ctx.web.get_file_by_server_relative_url(
                f"{FOLDER_LOGO.rstrip('/')}/{logo_filename}"
            )
            buf = BytesIO()
            file.download(buf).execute_query()
            buf.seek(0)
            return buf.read()
        except ClientRequestException as ex:
            print(f"SharePoint error downloading {logo_filename}: {ex}")
            return None
        except Exception as e:
            print(f"Unexpected error downloading {logo_filename}: {e}")
            return None

    def header(self):
        if self.logo_bytes:
            try:
                self.image(BytesIO(self.logo_bytes), x=10, y=5, w=71)
            except Exception as e:
                logger.error(f"Error rendering logo: {e}")
        self.set_font("Arial", "", 8)
        right_edge = 190
        lines = [
            "PO Box Z5538, St Georges Terrace, WA 6865",
            "Tel: 61 8 9228 1930",
            "ABN 68 612 806 381",
            "www.agoraretail.com.au"
        ]
        for i, line in enumerate(lines):
            text_width = self.get_string_width(line)
            self.set_xy(right_edge - text_width, 8 + i * 4.4)
            self.cell(text_width, 4.4, line)
        self.ln(13.5)
        self.set_draw_color(0, 0, 0)
        self.set_line_width(0.35)
        self.line(10, self.get_y(), 190, self.get_y())

def unpack_invoice_fields(inv, breakdown):
    """Extracts all relevant invoice fields into local variables and returns them as a dict."""
    fields = {
        "inv_agg_code":inv.get("inv_agg_code"),
        "itemlised":inv.get("itemlised"),
        "company_name": inv.get("company_name"),
        "abn": inv.get("abn"),
        "postal_address": inv.get("postal_address"),
        "contact_number": inv.get("contact_number"),
        "distributor_name": inv.get("distributor"),
        "emergency_contact": inv.get("emergency_number"),
        "customer_number": inv.get("customer_number"),
        "statement_account_number": inv.get("statement_account_number"),
        "purchase_order": inv.get("purchase_order_number"),
        "statement_number": inv.get("statement_number"),
        "start_date": inv.get("bill_start_date"),
        "end_date": inv.get("bill_end_date"),
        "issue_date": inv.get("bill_issue_date"),
        "due_date": inv.get("bill_due_date"),
        "statement_total_amount": float(inv.get("statement_total_amount") or 0.0),
        "statement_gst_amount": float(inv.get("statement_gst_amount") or 0.0),
        "statement_in_gst_amount": float(inv.get("statement_total_in_gst_amount") or 0.0),
        "invoice_number": inv.get("invoice_number"),
        "acct": inv.get("account_number"),
        "mirn": inv.get("mirn"),
        "premises_address": inv.get("premises_address"),
        "transmission_pipeline": inv.get("transmission_pipeline"),
        "distributor_mhq": inv.get("distribution_mhq"),
        "charge_notes": inv.get("notes"),
        "total_amount": float(inv.get("total_amount") or 0.0),
        "gst_amount": float(inv.get("gst_amount") or 0.0),
        "total_in_gst_amount": float(inv.get("total_in_gst_amount") or 0.0),
        
    }

    # Build charges_df subset for this invoice
    invoice_number = fields["invoice_number"]
    fields["charges_df"] = build_charges_df_from_breakdown(
        breakdown[breakdown["invoice_number"] == invoice_number]
    )

    # Shared color palette
    fields["custom_colors"] = {
        "Firm Gas Sales": "#0089D0",
        "Spot Gas Sales": "#9DC0D7",
        "Transport Fee": "#425B7E",
        "Distribution Charges": "#F4A261",
        "Adjustment Charges": "#E76F51",
        "Other Charges": "#2A9D8F",
    }

    return fields


def generate_statement_summary_page(pdf, inv, breakdown, logger, daily):
    f = unpack_invoice_fields(inv, breakdown)
    inv_agg_code = f["inv_agg_code"]
    itemlised, company_name, abn, postal_address = f["itemlised"], f["company_name"], f["abn"], f["postal_address"]
    contact_number, distributor_name = f["contact_number"], f["distributor_name"]
    emergency_contact, customer_number = f["emergency_contact"], f["customer_number"]
    statement_account_number, purchase_order = f["statement_account_number"], f["purchase_order"]
    statement_number, start_date, end_date = f["statement_number"], f["start_date"], f["end_date"]
    issue_date, due_date = f["issue_date"], f["due_date"]
    statement_total_amount = f.get("statement_total_amount") or f.get("total_amount")
    statement_total_in_gst_amount = f.get("statement_total_in_gst_amount") or f.get("total_in_gst_amount")
    statement_gst_amount = f.get("statement_gst_amount") or f.get("gst_amount")
    charges_df, custom_colors, invoice_number = f["charges_df"], f["custom_colors"], f["invoice_number"]

    pdf.add_page()

    # Title
    pdf.set_font("Arial", "", 16)
    pdf.set_x(10); pdf.set_y(35)

    if inv_agg_code is not None and itemlised is None:
        pdf.cell(0, 10, "Statement", new_x=XPos.LMARGIN, new_y=YPos.NEXT, align="L")
    else:
        pdf.cell(0, 10, "Tax Invoice", new_x=XPos.LMARGIN, new_y=YPos.NEXT, align="L")
    
    pdf.ln(10)

    # Left: Customer block (use company, ABN, postal address)
    x0, y0 = 10, 45
    bg_w = 90
    line_h = 6

    # Compute an approximate block height without using split_only (compat with older fpdf2)
    postal_text = f"{postal_address or ''}\n"
    postal_lines_est = max(1, len(str(postal_text).splitlines()))
    block_h = 6 + 6 + 4 + 6 + 6 + 6

    pdf.set_fill_color(220, 230, 241)
    pdf.rect(x0, y0, bg_w, block_h, style='F')
    pdf.set_xy(x0, y0)
    pdf.set_font("Arial", "B", 10)
    pdf.cell(80, 6, str(company_name or ""), new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.cell(80, 6, f"ABN: {abn or ''}", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.cell(80, 4, "", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.cell(80, 6, "Accounts Payable", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.multi_cell(80, 6, postal_text, new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.ln(10)

    # Right: Contact & Emergency
    right_edge = 190
    block_start_x = 90
    label_width = 70
    value_width = (right_edge - block_start_x) - label_width

    pdf.set_xy(block_start_x, 45)
    pdf.set_font("Arial", "B", 9); pdf.cell(label_width, 9, "Contact Us", align='R')
    pdf.set_font("Arial", "", 9.5); pdf.cell(value_width, 9, str(contact_number or ""), new_x=XPos.LMARGIN, new_y=YPos.NEXT, align='R')
    pdf.set_x(block_start_x); pdf.set_font("Arial", "B", 9)
    pdf.multi_cell(label_width, 6, f"Distributor ({distributor_name or ''})\n Contact Number", align='R')
    pdf.set_xy(block_start_x + label_width, pdf.get_y() - 12)
    pdf.set_font("Arial", "", 9.5); pdf.multi_cell(value_width, 12, str(contact_number or ""), align='R')
    pdf.set_x(block_start_x); pdf.set_font("Arial", "BU", 9)
    pdf.multi_cell(label_width, 7, f"{distributor_name or ''} Gas Fault & Emergency\n Contact (24 Hrs)", align='R')
    pdf.set_xy(block_start_x + label_width, pdf.get_y() - 14)
    pdf.set_font("Arial", "U", 9.5); pdf.multi_cell(value_width, 12, str(emergency_contact or ""), align='R')

    # Divider
    y_top = 82; pdf.set_draw_color(0,0,0); pdf.set_line_width(0.35); pdf.line(10, y_top, 190, y_top)

    # Left: Account & invoice info
    pdf.set_xy(10, 83)
    write_label_value(pdf, "Customer Number", customer_number, x=10)
    write_label_value(pdf, "Account Number", statement_account_number, x=10)
    write_label_value(pdf, "Postal Address", postal_address, x=10, wrap=True, force_two_lines=True)
    write_label_value(pdf, "Purchase Order #", purchase_order, x=10)
    if inv_agg_code is not None and not itemlised:
        write_label_value(pdf, "Statement No.", statement_number, x=10)
    else:
        write_label_value(pdf, "Tax Invoice No.", statement_number, x=10)

    start_dt = pd.to_datetime(start_date, errors="coerce")
    end_dt   = pd.to_datetime(end_date,   errors="coerce")
    billing_period_lbl = (
        f"{start_dt.strftime('%d-%b-%y')} to {end_dt.strftime('%d-%b-%y')}"
        if pd.notna(start_dt) and pd.notna(end_dt) else ""
    )
    write_label_value(pdf, "Billing Period", billing_period_lbl, x=10)

    # Right: Issue/Due/Total
    right_start_x = block_start_x
    label_w_right = label_width
    value_w_right = value_width
    pdf.set_xy(right_start_x, 83)
    issue_dt = pd.to_datetime(issue_date, errors="coerce")
    due_dt   = pd.to_datetime(due_date,   errors="coerce")

    write_label_value(pdf, 
        "Issue Date",
        issue_dt.strftime("%d-%b-%y") if pd.notna(issue_dt) else "",
        x=right_start_x, label_w=label_w_right, value_w=value_w_right,
        align="R", bold_size=10, reg_size=10.5
    )

    write_label_value(pdf, 
        "Due Date",
        due_dt.strftime("%d-%b-%y") if pd.notna(due_dt) else "",
        x=right_start_x, label_w=label_w_right, value_w=value_w_right,
        align="R", bold_size=10, reg_size=10.5
    )

    pdf.ln(14)
    write_label_value(pdf, 
        "Total Amount Payable", f"${statement_total_amount:,.2f}",
        x=right_start_x, label_w=label_w_right, value_w=value_w_right,
        align="R", bold_size=10, reg_size=10.5)

    # Divider
    pdf.ln(10); pdf.set_draw_color(0,0,0); pdf.set_line_width(0.35); pdf.line(10, 120.5, 190, 120.5)

    # Gas Account Summary (left)
    pdf.set_xy(10, 120.5)
    pdf.set_font("Arial", "I", 8); pdf.cell(50, 8, "This statement is based on usage data provided by network providers", border=0)
    pdf.set_fill_color(220, 230, 241); pdf.set_xy(10, 126.5)
    pdf.set_font("Arial", "B", 9); pdf.cell(90.5, 6, "Gas Account Summary", fill=True)
    pdf.line(10, 132.5, 100, 132.5)

    # Current Charges summary
    pdf.set_xy(10, 145)
    pdf.set_font("Arial", "BU", 9)
    pdf.cell(80, 6, "Current Charges", border=0); pdf.ln(6)
    s_for_pie_cat_statement = (
        charges_df.groupby("Charge Category")["Statement Amount ex GST"]
                    .apply(lambda s: pd.to_numeric(s, errors="coerce").fillna(0.0).sum())
    )
    s_for_pie_cat_statement = s_for_pie_cat_statement[s_for_pie_cat_statement > 0]

    
    for category, statement_total_amount in s_for_pie_cat_statement.sort_values(ascending=False).items():
        pdf.set_x(10); pdf.set_font("Arial", "", 9)
        pdf.cell(50, 5, str(category), border=0, align="L")
        pdf.cell(40, 5, f"${float(statement_total_amount):,.2f}", border=0, new_x=XPos.LMARGIN, new_y=YPos.NEXT, align="R")

    pdf.set_xy(10, 185); pdf.set_font("Arial", "B", 9)
    pdf.cell(50, 4, "Total of Current Charges", align='L')
    pdf.set_font("Arial", "B", 9.5); pdf.cell(40, 4, f"${float(statement_total_amount):,.2f}", new_x=XPos.LMARGIN, new_y=YPos.NEXT, align='R')
    pdf.set_font("Arial", "B", 9); pdf.cell(50, 4, "GST Payable on Current Charges", align='L')
    pdf.set_font("Arial", "B", 9.5); pdf.cell(40, 4, f"${float(statement_gst_amount):,.2f}", new_x=XPos.LMARGIN, new_y=YPos.NEXT, align='R')
    pdf.line(75, 194, 100, 194)
    pdf.set_font("Arial", "B", 9); pdf.cell(50, 8, "Total of Current Charges Inc GST", align='L')
    pdf.set_font("Arial", "B", 9.5); pdf.cell(40, 8, f"${float(statement_total_in_gst_amount):,.2f}", new_x=XPos.LMARGIN, new_y=YPos.NEXT, align='R')
    pdf.line(10, 199.5, 100, 199.5)


    # ---- Pie Chart (right) ----
    if not s_for_pie_cat_statement.empty:
        buf1 = generate_pie_chart(s_for_pie_cat_statement, custom_colors)
        pdf.image(buf1, x=115, y=123.5, w=80)
        logger.info(f"Styled pie chart embedded for statement {statement_number} at x=115, y=123.5, w=80")

    # Footer notes & payments
    pdf.set_fill_color(220, 230, 241)
    pdf.set_xy(10, 202)
    pdf.set_font("Arial", "B", 9)
    pdf.multi_cell(180, 6, "Agora Retail also operates in the retail natural gas market in Victoria supplying gas to customers who consume over ten terajoule (TJ) of gas per annum.", fill=True)
    pdf.line(10, 217, 190, 217)

    # EFT Block (Left)
    pdf.set_xy(10, 219)
    pdf.set_font("Arial", "B", 9)
    pdf.cell(80, 6, "Electronic Fund Transfer (EFT)", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.cell(80, 6, f"          Reference No. {statement_number}", fill=True, new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.set_font("Arial", "B", 9); pdf.cell(30, 6, "Account Name", new_x=XPos.RIGHT, new_y=YPos.TOP)
    pdf.set_font("Arial", "", 9); pdf.cell(60, 6, "Agora Retail Pty Limited", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.set_font("Arial", "B", 9); pdf.cell(30, 6, "BSB", new_x=XPos.RIGHT, new_y=YPos.TOP)
    pdf.set_font("Arial", "", 9); pdf.cell(60, 6, "182 800  (Macquarie Bank Limited)", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.set_font("Arial", "B", 9); pdf.cell(30, 6, "Account Number", new_x=XPos.RIGHT, new_y=YPos.TOP)
    pdf.set_font("Arial", "", 9); pdf.cell(60, 6, "1165 7541", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.set_font("Arial", "", 8.5); pdf.cell(30, 4, "Please send remittance Advice To", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.set_font("Arial", "", 8.5); pdf.cell(60, 4, "accounts@agoraretail.com.au", new_x=XPos.LMARGIN, new_y=YPos.NEXT)

    # Alternative Block (Right)
    pdf.set_xy(110, 219)
    pdf.set_font("Arial", "B", 9)
    pdf.cell(80, 6, "Alternative Form of Payments*", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.set_x(110)
    pdf.cell(80, 6, f"          Reference No. {statement_number}", fill=True, new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.set_xy(110, pdf.get_y())
    pdf.set_font("Arial", "B", 9)
    pdf.cell(50, 4, "Call us at", new_x=XPos.RIGHT, new_y=YPos.TOP)
    pdf.set_x(145)
    pdf.set_font("Arial", "", 9)
    pdf.cell(60, 4, "1800 403 093", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.ln(14)
    pdf.set_x(110)
    pdf.set_font("Arial", "", 7.5)
    pdf.cell(50, 4, "*Surcharge fee may apply to the payment method other than EFT.")
    pdf.line(10, 257, 190, 257)
    
    pdf.set_xy(90, 270)
    pdf.set_font("Arial", "", 7.5)
    pdf.cell(180, 4, "Page 0 of 2")

def generate_invoice_page1(pdf, inv, breakdown, daily, logger):
    f = unpack_invoice_fields(inv, breakdown)
    inv_agg_code = f["inv_agg_code"]
    itemlised, company_name, abn, postal_address = f["itemlised"], f["company_name"], f["abn"], f["postal_address"]
    distributor_name, emergency_contact = f["distributor_name"], f["emergency_contact"]
    customer_number, acct, premises_address = f["customer_number"], f["acct"], f["premises_address"]
    purchase_order, invoice_number, statement_number = f["purchase_order"], f["invoice_number"], f["statement_number"]
    start_date, end_date, issue_date, due_date = f["start_date"], f["end_date"], f["issue_date"], f["due_date"]
    total_in_gst_amount, total_amount, gst_amount = f["statement_total_amount"], f["statement_total_amount"], f["statement_gst_amount"]
    charges_df, custom_colors = f["charges_df"], f["custom_colors"]
    contact_number, distributor_name = f["contact_number"], f["distributor_name"]

    pdf.add_page()

    # Title
    pdf.set_font("Arial", "", 16)
    pdf.set_x(10); pdf.set_y(35)
    pdf.cell(0, 10, "Tax Invoice", new_x=XPos.LMARGIN, new_y=YPos.NEXT, align="L")
    pdf.ln(10)

    # Left: Customer block (use company, ABN, postal address)
    x0, y0 = 10, 45
    bg_w = 90
    line_h = 6

    # Compute an approximate block height without using split_only (compat with older fpdf2)
    postal_text = f"{postal_address or ''}\n"
    postal_lines_est = max(1, len(str(postal_text).splitlines()))
    block_h = 6 + 6 + 4 + 6 + 6 + 6

    pdf.set_fill_color(220, 230, 241)
    pdf.rect(x0, y0, bg_w, block_h, style='F')
    pdf.set_xy(x0, y0)
    pdf.set_font("Arial", "B", 10)
    pdf.cell(80, 6, str(company_name or ""), new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.cell(80, 6, f"ABN: {abn or ''}", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.cell(80, 4, "", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.cell(80, 6, "Accounts Payable", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.multi_cell(80, 6, postal_text, new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.ln(10)

    # Right: Contact & Emergency
    right_edge = 190
    block_start_x = 90
    label_width = 70
    value_width = (right_edge - block_start_x) - label_width

    pdf.set_xy(block_start_x, 45)
    pdf.set_font("Arial", "B", 9); pdf.cell(label_width, 9, "Contact Us", align='R')
    pdf.set_font("Arial", "", 9.5); pdf.cell(value_width, 9, str(contact_number or ""), new_x=XPos.LMARGIN, new_y=YPos.NEXT, align='R')
    pdf.set_x(block_start_x); pdf.set_font("Arial", "B", 9)
    pdf.multi_cell(label_width, 6, f"Distributor ({distributor_name or ''})\n Contact Number", align='R')
    pdf.set_xy(block_start_x + label_width, pdf.get_y() - 12)
    pdf.set_font("Arial", "", 9.5); pdf.multi_cell(value_width, 12, str(contact_number or ""), align='R')
    pdf.set_x(block_start_x); pdf.set_font("Arial", "BU", 9)
    pdf.multi_cell(label_width, 7, f"{distributor_name or ''} Gas Fault & Emergency\n Contact (24 Hrs)", align='R')
    pdf.set_xy(block_start_x + label_width, pdf.get_y() - 14)
    pdf.set_font("Arial", "U", 9.5); pdf.multi_cell(value_width, 12, str(emergency_contact or ""), align='R')

    # Divider
    y_top = 82; pdf.set_draw_color(0,0,0); pdf.set_line_width(0.35); pdf.line(10, y_top, 190, y_top)

    # Left: Account & invoice info
    pdf.set_xy(10, 83)
    write_label_value(pdf, "Customer Number", customer_number, x=10)
    write_label_value(pdf, "Account Number", acct, x=10)
    write_label_value(pdf, "Premises Address", premises_address, x=10, wrap=True, force_two_lines=True)
    write_label_value(pdf, "Purchase Order #", purchase_order, x=10)
    if inv_agg_code is not None and itemlised == "Yes":
        write_label_value(pdf, "Tax Invoice No.", statement_number, x=10)
    else:
        write_label_value(pdf, "Tax Invoice No.", invoice_number, x=10)
    start_dt = pd.to_datetime(start_date, errors="coerce")
    end_dt   = pd.to_datetime(end_date,   errors="coerce")
    billing_period_lbl = (
        f"{start_dt.strftime('%d-%b-%y')} to {end_dt.strftime('%d-%b-%y')}"
        if pd.notna(start_dt) and pd.notna(end_dt) else ""
    )
    write_label_value(pdf, "Billing Period", billing_period_lbl, x=10)

    # Right: Issue/Due/Total
    right_start_x = block_start_x
    label_w_right = label_width
    value_w_right = value_width
    pdf.set_xy(right_start_x, 83)
    issue_dt = pd.to_datetime(issue_date, errors="coerce")
    due_dt   = pd.to_datetime(due_date,   errors="coerce")

    write_label_value(pdf, 
        "Issue Date",
        issue_dt.strftime("%d-%b-%y") if pd.notna(issue_dt) else "",
        x=right_start_x, label_w=label_w_right, value_w=value_w_right,
        align="R", bold_size=10, reg_size=10.5
    )

    write_label_value(pdf, 
        "Due Date",
        due_dt.strftime("%d-%b-%y") if pd.notna(due_dt) else "",
        x=right_start_x, label_w=label_w_right, value_w=value_w_right,
        align="R", bold_size=10, reg_size=10.5
    )

    pdf.ln(14)
    write_label_value(pdf, 
        "Total Amount Payable", f"${total_amount:,.2f}",
        x=right_start_x, label_w=label_w_right, value_w=value_w_right,
        align="R", bold_size=10, reg_size=10.5)

    # Divider
    pdf.ln(10); pdf.set_draw_color(0,0,0); pdf.set_line_width(0.35); pdf.line(10, 120.5, 190, 120.5)

    # Gas Account Summary (left)
    pdf.set_xy(10, 120.5)
    pdf.set_font("Arial", "I", 8); pdf.cell(50, 8, "This invoice is based on usage data provided by network providers", border=0)
    pdf.set_fill_color(220, 230, 241); pdf.set_xy(10, 126.5)
    pdf.set_font("Arial", "B", 9); pdf.cell(90.5, 6, "Gas Account Summary", fill=True)
    pdf.line(10, 132.5, 100, 132.5)

    # Current Charges summary
    pdf.set_xy(10, 145)
    pdf.set_font("Arial", "BU", 9)
    pdf.cell(80, 6, "Current Charges", border=0); pdf.ln(6)
    s_for_pie_cat = (
        charges_df.groupby("Charge Category")["Amount ex GST"]
                    .apply(lambda s: pd.to_numeric(s, errors="coerce").fillna(0.0).sum())
    )
    s_for_pie_cat = s_for_pie_cat[s_for_pie_cat > 0]

    
    for category, amount in s_for_pie_cat.sort_values(ascending=False).items():
        pdf.set_x(10); pdf.set_font("Arial", "", 9)
        pdf.cell(50, 5, str(category), border=0, align="L")
        pdf.cell(40, 5, f"${float(amount):,.2f}", border=0, new_x=XPos.LMARGIN, new_y=YPos.NEXT, align="R")

    pdf.set_xy(10, 185); pdf.set_font("Arial", "B", 9)
    pdf.cell(50, 4, "Total of Current Charges", align='L')
    pdf.set_font("Arial", "B", 9.5); pdf.cell(40, 4, f"${float(total_amount):,.2f}", new_x=XPos.LMARGIN, new_y=YPos.NEXT, align='R')
    pdf.set_font("Arial", "B", 9); pdf.cell(50, 4, "GST Payable on Current Charges", align='L')
    pdf.set_font("Arial", "B", 9.5); pdf.cell(40, 4, f"${float(gst_amount):,.2f}", new_x=XPos.LMARGIN, new_y=YPos.NEXT, align='R')
    pdf.line(75, 194, 100, 194)
    pdf.set_font("Arial", "B", 9); pdf.cell(50, 8, "Total of Current Charges Inc GST", align='L')
    pdf.set_font("Arial", "B", 9.5); pdf.cell(40, 8, f"${float(total_in_gst_amount):,.2f}", new_x=XPos.LMARGIN, new_y=YPos.NEXT, align='R')
    pdf.line(10, 199.5, 100, 199.5)


    # ---- Pie Chart (right) ----
    if not s_for_pie_cat.empty:
        buf1 = generate_pie_chart(s_for_pie_cat, custom_colors)
        pdf.image(buf1, x=115, y=123.5, w=80)
        logger.info(f"Styled pie chart embedded for invoice {invoice_number} at x=115, y=123.5, w=80")

    # Footer notes & payments
    pdf.set_fill_color(220, 230, 241)
    pdf.set_xy(10, 202)
    pdf.set_font("Arial", "B", 9)
    pdf.multi_cell(180, 6, "Agora Retail also operates in the retail natural gas market in Victoria supplying gas to customers who consume over ten terajoule (TJ) of gas per annum.", fill=True)
    pdf.line(10, 217, 190, 217)

    # EFT Block (Left)
    pdf.set_xy(10, 219)
    pdf.set_font("Arial", "B", 9)
    pdf.cell(80, 6, "Electronic Fund Transfer (EFT)", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.cell(80, 6, f"          Reference No. {invoice_number}", fill=True, new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.set_font("Arial", "B", 9); pdf.cell(30, 6, "Account Name", new_x=XPos.RIGHT, new_y=YPos.TOP)
    pdf.set_font("Arial", "", 9); pdf.cell(60, 6, "Agora Retail Pty Limited", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.set_font("Arial", "B", 9); pdf.cell(30, 6, "BSB", new_x=XPos.RIGHT, new_y=YPos.TOP)
    pdf.set_font("Arial", "", 9); pdf.cell(60, 6, "182 800  (Macquarie Bank Limited)", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.set_font("Arial", "B", 9); pdf.cell(30, 6, "Account Number", new_x=XPos.RIGHT, new_y=YPos.TOP)
    pdf.set_font("Arial", "", 9); pdf.cell(60, 6, "1165 7541", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.set_font("Arial", "", 8.5); pdf.cell(30, 4, "Please send remittance Advice To", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.set_font("Arial", "", 8.5); pdf.cell(60, 4, "accounts@agoraretail.com.au", new_x=XPos.LMARGIN, new_y=YPos.NEXT)

    # Alternative Block (Right)
    pdf.set_xy(110, 219)
    pdf.set_font("Arial", "B", 9)
    pdf.cell(80, 6, "Alternative Form of Payments*", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.set_x(110)
    pdf.cell(80, 6, f"          Reference No. {invoice_number}", fill=True, new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.set_xy(110, pdf.get_y())
    pdf.set_font("Arial", "B", 9)
    pdf.cell(50, 4, "Call us at", new_x=XPos.RIGHT, new_y=YPos.TOP)
    pdf.set_x(145)
    pdf.set_font("Arial", "", 9)
    pdf.cell(60, 4, "1800 403 093", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.ln(14)
    pdf.set_x(110)
    pdf.set_font("Arial", "", 7.5)
    pdf.cell(50, 4, "*Surcharge fee may apply to the payment method other than EFT.")
    pdf.line(10, 257, 190, 257)
    
    pdf.set_xy(90, 270)
    pdf.set_font("Arial", "", 7.5)
    pdf.cell(180, 4, "Page 1 of 2")

def generate_invoice_page2(pdf, inv, breakdown, daily, logger):
    f = unpack_invoice_fields(inv, breakdown)
    itemlised = f["itemlised"]
    invoice_number = f["invoice_number"]
    acct, mirn, transmission_pipeline = f["acct"], f["mirn"], f["transmission_pipeline"]
    distributor_name, distributor_mhq = f["distributor_name"], f["distributor_mhq"]
    billing_period = f"{f['start_date']} to {f['end_date']}"
    premises_address, charges_df = f["premises_address"], f["charges_df"]
    total_amount, gst_amount = f["total_amount"], f["gst_amount"]
    charge_notes = f["charge_notes"]
    start_date_dt = pd.to_datetime(f["start_date"], errors="coerce")
    end_date_dt = pd.to_datetime(f["end_date"], errors="coerce")


    pdf.add_page()

    label_width_left = 48
    label_width_right = 35
    left_x = 10
    right_x = 105
    right_value_width = 50
    font_main = "Arial"

    def set_font(weight: str = "", size: float = 8.5) -> None:
        pdf.set_font(font_main, weight, size)

    # Account Details (Left Block)
    pdf.set_xy(left_x, 35)
    set_font("", 9.5)
    write_label_value(pdf, "Tax Invoice No.", invoice_number, x=10, label_w=label_width_left)
    write_label_value(pdf, "Account No.", acct, x=10, label_w=label_width_left)
    write_label_value(pdf, "MIRN", mirn, x=10, label_w=label_width_left)
    write_label_value(pdf, "Trading Name", "To be Added", x=10, label_w=label_width_left)
    write_label_value(pdf, "Transmission Pipeline (if any)", transmission_pipeline, x=10, label_w=label_width_left)
    pdf.ln(2)

    # Billing Details (Right Block)
    pdf.set_xy(right_x, 35)
    write_label_value(pdf, "Billing Period", billing_period, x=105, label_w=label_width_right, value_w=right_value_width)
    write_label_value(pdf, "Distributor", distributor_name, x=105, label_w=label_width_right, value_w=right_value_width)
    write_label_value(pdf, 
                      "Distributor MHQ", 
                      (f"{float(distributor_mhq):.2f}" if (pd.notna(distributor_mhq) and str(distributor_mhq).strip() != "") else ""),
                      x=105, label_w=label_width_right, value_w=right_value_width, unit=" GJ")
    write_label_value(pdf, "Premises Address", premises_address, x=105, wrap=True, force_two_lines=True, label_w=label_width_right, value_w=right_value_width)

    # Divider & Section Header
    pdf.line(10, 60, 190, 60)
    pdf.set_y(62)
    set_font("BU", 8)
    pdf.cell(60, 4, "Your Gas usage and Charges summary")
    pdf.ln(5.8)

    columns = [
        {"header": "Charges", "width": 60, "align": "L"},
        {"header": "Rate", "width": 30, "align": "R"},
        {"header": "Unit", "width": 15, "align": "L"},
        {"header": "Total Qty", "width": 30, "align": "R"},
        {"header": "Unit", "width": 15, "align": "L"},
        {"header": "$      ", "width": 30, "align": "R"},
    ]
    stripe_height = 4
    stripe_colors = [(220, 230, 241), (255, 255, 255)]
    x_start = 10
    y_start = 72.6
    total_width = sum(col["width"] for col in columns)

    def draw_table_bands() -> None:
        for i in range(15):
            pdf.set_fill_color(*stripe_colors[i % 2])
            pdf.rect(x_start, y_start + i * stripe_height, total_width, stripe_height, style='F')

    def draw_table_header() -> None:
        pdf.set_font("Arial", "B", 8)
        pdf.set_fill_color(0, 137, 208)
        pdf.set_text_color(255, 255, 255)
        for col in columns:
            pdf.cell(col["width"], 4.1, col["header"], border="TB", align=col["align"], fill=True)
        pdf.ln()
        pdf.set_text_color(0, 0, 0)

    draw_table_bands()
    draw_table_header()

    row_index = 0

    def new_detail_page() -> None:
        nonlocal row_index
        pdf.add_page()
        pdf.set_y(35)
        set_font("BU", 10); pdf.cell(100, 6, "Gas usage and Charges (continued)")
        pdf.ln(6)
        draw_table_bands()
        draw_table_header()
        row_index = 0

    for category, cat_df in charges_df.groupby("Charge Category", sort=False):
        if cat_df.empty:
            continue
        # Category header row
        if row_index >= 22:
            new_detail_page()
        pdf.set_y(y_start + row_index * stripe_height)
        pdf.set_x(x_start)
        pdf.set_font("Arial", "BU", 8)
        pdf.set_fill_color(*stripe_colors[row_index % 2])
        pdf.cell(columns[0]["width"], stripe_height, str(category), border=0, align="L", fill=True)
        for col in columns[1:]:
            pdf.cell(col["width"], stripe_height, "", border=0, align=col["align"], fill=True)
        pdf.ln(stripe_height)
        row_index += 1

        # Detail rows
        pdf.set_font("Arial", "", 8)
        for _, row in cat_df.iterrows():
            if row_index >= 22:
                new_detail_page()

            _amt = pd.to_numeric(row.get("Amount ex GST"), errors="coerce")
            amt_ex_gst_fmt = f"${_amt:,.2f}" if pd.notna(_amt) else ""

            values = [
                "          " + str(row.get("Charge Type", "")),
                "" if pd.isna(row.get("Rate")) else str(row.get("Rate")),
                str(row.get("Rate UOM", "")) if pd.notna(row.get("Rate UOM")) else "",
                "" if pd.isna(row.get("Unit")) else str(row.get("Unit")),
                str(row.get("Unit UOM", "")) if pd.notna(row.get("Unit UOM")) else "",
                amt_ex_gst_fmt,
            ]

            pdf.set_y(y_start + row_index * stripe_height)
            pdf.set_x(x_start)
            pdf.set_fill_color(*stripe_colors[row_index % 2])
            for i, col in enumerate(columns):
                pdf.cell(col["width"], stripe_height, values[i], border=0, align=col["align"], fill=True)
            pdf.ln(stripe_height)
            row_index += 1

    # Totals box
    pdf.set_y(133)
    pdf.set_font("Arial", "B", 8.5)
    pdf.cell(90, 4, "Total of Current Charges", align='L')
    pdf.cell(90, 4, f"${float(total_amount):,.2f}", new_x=XPos.LMARGIN, new_y=YPos.NEXT, align='R')
    pdf.cell(90, 4, "GST Payable on Current Charges", align='L')
    pdf.cell(90, 4, f"${float(gst_amount):,.2f}", new_x=XPos.LMARGIN, new_y=YPos.NEXT, align='R')
    pdf.line(10, pdf.get_y()+1, 190, pdf.get_y()+1)

    # === Daily consumption ===
    cust_consumption = pd.DataFrame()
    if not daily.empty and "mirn" in daily.columns:
        cust_consumption = daily[daily["mirn"] == mirn].copy()

    # Calculate monthly_mdq (maximum daily consumption)
    monthly_mdq = None
    if not cust_consumption.empty:
        monthly_mdq = cust_consumption["gj_consumption"].max()  # Maximum daily consumption

    contract_mdq = cust_consumption["retail_mdq"].iloc[0]

    # === MDQ box above Consumption Chart ===
    pdf.set_xy(10, 145)
    label_w = 30
    value_w = 60

    def fmt_gj(x: Any) -> str:
        try:
            return f"{float(x):.2f} GJ"
        except (TypeError, ValueError):
            return ""

    # MDQ box for monthly and contract MDQ
    pdf.set_font("Arial", "BU", 8)
    pdf.cell(label_w, 4, "MDQ of the month")
    pdf.set_font("Arial", "U", 8)
    pdf.cell(value_w, 4, fmt_gj(monthly_mdq), new_x=XPos.LMARGIN, new_y=YPos.NEXT)

    pdf.set_font("Arial", "BU", 8)
    pdf.cell(label_w, 4, "Contract MDQ")
    val = "" if contract_mdq is None else fmt_gj(contract_mdq)
    pdf.set_font("Arial", "U", 8)
    pdf.cell(value_w, 4, val, new_x=XPos.LMARGIN, new_y=YPos.NEXT)

    pdf.rect(10, 145, 70, 8)
    pdf.ln(1)


    # === Chart sizing constants ===
    CHART_W_MM      = 180.0
    CHART_FULL_H_MM = 64.0
    CHART_HALF_H_MM = 32.0

    # === CHARTS ===
    selected_mirns = ['5600002119_8', '5600002162_7', '5600462393_7']
    multi_mirn_ok = False

    # === Full-height Consumption Chart ===
    if mirn not in selected_mirns:
        consumption_chart_buf = generate_consumption_chart(
            mirn=mirn,
            billing_period=f"{inv['bill_start_date']} to {inv['bill_end_date']}",
            cust_consumption_df=cust_consumption,  
            contract_mdq=contract_mdq,
            fig_height_mm=CHART_FULL_H_MM  # Full height for other accounts
        )
        embed_chart_in_pdf(pdf, consumption_chart_buf, CHART_FULL_H_MM)

    # === Half-height Consumption Chart + Aggregated MDQ Chart ===
    if mirn in selected_mirns:
        # Half-height consumption chart
        consumption_chart_buf = generate_consumption_chart(
            mirn=mirn,
            billing_period=f"{inv['bill_start_date']} to {inv['bill_end_date']}",
            cust_consumption_df=cust_consumption, 
            contract_mdq=contract_mdq,
            fig_height_mm=CHART_HALF_H_MM  # Half height for specified accounts
        )
        embed_chart_in_pdf(pdf, consumption_chart_buf, CHART_HALF_H_MM)

        # === Aggregated MDQ Block ===
        aggregated_monthly_mdq = compute_aggregated_mdq(
            daily_df=daily,
            selected_mirns=selected_mirns,
            start_date=start_date_dt,
            end_date=end_date_dt
        )
        pdf.rect(10, pdf.get_y(), 70, 8)

        pdf.rect(10, pdf.get_y(), 70, 8)
        pdf.set_font("Arial", "BU", 8)
        pdf.cell(label_w + 20, 4, "MDQ of the month (aggregated)")
        pdf.set_font("Arial", "U", 8)
        pdf.cell(value_w, 4, fmt_gj(aggregated_monthly_mdq), new_x=XPos.LMARGIN, new_y=YPos.NEXT)

        pdf.set_font("Arial", "BU", 8)
        pdf.cell(label_w + 20, 4, "Contract MDQ (aggregated)")
        pdf.set_font("Arial", "U", 8)
        pdf.cell(value_w, 4, fmt_gj(3000), new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        pdf.ln(2)

        # === Multi-MIRN Chart (Stacked) ===
        accounts_mirn_chart_buf = generate_accounts_mirn_chart(
            billing_period=f"{inv['bill_start_date']} to {inv['bill_end_date']}",
            df=daily,
            selected_mirns=selected_mirns  
        )

        embed_chart_in_pdf(pdf, accounts_mirn_chart_buf, CHART_HALF_H_MM)
        multi_mirn_ok = True

    # # Ensure new page if needed before drawing charts
    # def _ensure_space(h_mm: float) -> None:
    #     try:
    #         page_h = getattr(pdf, "h", 297)
    #         b_margin = getattr(pdf, "b_margin", 10)
    #         if pdf.get_y() + h_mm > (page_h - b_margin):
    #             pdf.add_page()
    #     except Exception:
    #         pdf.add_page()

    # ================= “Please Note” block =================
    pdf.ln(2)
    NOTE_MIN_H = 25

    try:
        page_h = getattr(pdf, "h", 297)
        b_margin = getattr(pdf, "b_margin", 10)
        if pdf.get_y() + NOTE_MIN_H > (page_h - b_margin):
            pdf.add_page()
    except Exception:
        pass

    box_y = pdf.get_y()
    pdf.set_font("Arial", "B", 8)
    pdf.multi_cell(180, 6, "Please Note", new_x=XPos.LMARGIN, new_y=YPos.NEXT)

    pdf.set_font("Arial", "", 7)
    text1 = "* Agora Retail will reconcile Network Charges for this month against the actual charges invoiced by the Network Operator and the adjustment charges will be applied accordingly on the next invoice."
    text2 = "* gasTrading Spot Prices are published on the website: https://gastrading.com.au/spot-market/historical-prices-and-volume/bid-and-scheduled"
    text3 = f"* {str(charge_notes).strip()}" if (pd.notna(charge_notes) and str(charge_notes).strip()) else ""

    pdf.multi_cell(180, 4, text1, border=0, new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.multi_cell(180, 4, text2, border=0, new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.multi_cell(180, 4, text3, border=0)

    box_bottom = pdf.get_y()
    box_h = max(NOTE_MIN_H, box_bottom - box_y)
    pdf.rect(10, box_y, 180, box_h)

    pdf.set_xy(90, 270)
    pdf.set_font("Arial", "", 7.5)
    pdf.cell(180, 4, "Page 2 of 2")

# =========================
# MAIN
# =========================
def main():
    # ---- Logging setup ----
    log_filename = "Bill_Generating_Log.log"   
    global logger
    logger = logging.getLogger("SharePointLogger")
    logger.setLevel(logging.INFO)

    sp_handler = SharePointLogHandler(log_filename)
    sp_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger.addHandler(sp_handler)

    logger.info("=== Starting PDF generation with charts ===")
    skipped_duplicates = 0

    # ----- Connect to SQL ----
    monthly, breakdown, daily = load_views()
    if monthly.empty:
        logger.error("No data returned from dbo.vw_test_charges_monthly. Exiting.")
        return
    if breakdown.empty:
        logger.error("No data returned from dbo.vw_test_charges_breakdown. Exiting.")
        return

    headers = build_invoice_headers_from_monthly(monthly)

    # Engine & table check
    eng = get_engine()
    ensure_billing_history_table(eng)

    # ---- For each statement ----
    for statement_number, statement_group in headers.groupby("statement_number"):
        total_statement_amount = statement_group["total_in_gst_amount"].sum()
        process, final_statement_number = should_process_statement(eng, statement_number, total_statement_amount)

        if not process:
            skipped_duplicates += 1
            continue  # skip this entire statement

        logger.info(f"Processing Statement {final_statement_number} with {len(statement_group)} invoices")
        pdf = PDF()
        generate_statement_summary_page(pdf, statement_group.iloc[0], breakdown, logger, daily)

        # ---- For each invoice in this statement only ----
        for _, inv in statement_group.iterrows():
            hist_row = build_history_row_from_monthly(inv)
            r2, invoice_number, do_insert = _apply_history_increment_rule(eng, hist_row)

            if not do_insert:
                skipped_duplicates += 1
                logger.info(f"Duplicate invoice (same total) detected: {invoice_number}; skipping PDF and history.")
                continue

            # === PAGE 1 Conditions ===
            # 1️⃣ Skip if invoice == statement
            # 2️⃣ Skip if invoice != statement and itemlised == 'yes'
            # 3️⃣ Generate page 1 if invoice != statement and (itemlised != 'yes' or empty)
            if inv["invoice_number"] != statement_number:
                if not (inv["itemlised"] == "Yes"):
                    generate_invoice_page1(pdf, inv, breakdown, daily, logger)
                else:
                    logger.info(f"Skipping Page 1 for itemlised invoice: {invoice_number}")

            # === PAGE 2 always ===
            generate_invoice_page2(pdf, inv, breakdown, daily, logger)

            # === Insert into billing history ===
            try:
                if r2 is not None and do_insert:
                    insert_billing_history_batch(eng, [r2])
                    logger.info(f"Inserted billing history row for {invoice_number}")
            except Exception as e:
                logger.error(f"Failed inserting billing_history for {invoice_number}: {e}")


        # ---- Save 1 PDF per statement ----
        pdf_filename = f"{statement_number}_Generated_{GENERATE_DATE}.pdf"
        pdf_bytes = BytesIO()
        pdf.output(pdf_bytes)
        pdf_bytes.seek(0)
        upload_bytes_to_sharepoint(pdf_bytes.read(), pdf_filename)
        logger.info(f"Statement uploaded: {pdf_filename}")

    logger.info(f"=== Completed PDF generation; Skipped {skipped_duplicates} duplicate invoices ===")

if __name__ == "__main__":
    main()
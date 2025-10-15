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

def list_files_in_sharepoint_folder(folder: str) -> List[str]:
    """
    Returns list of filenames in the target SharePoint folder.
    """
    try:
        ctx = get_sharepoint_context()
        target_folder = ctx.web.get_folder_by_server_relative_url(folder)
        files = target_folder.files.get().execute_query()
        return [f.name for f in files]
    except ClientRequestException as e:
        logging.error(f"Failed to list SharePoint folder {folder}: {e}")
        return []
    except Exception as e:
        logging.error(f"Unexpected error listing SharePoint folder {folder}: {e}")
        return []

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
        monthly_df: dbo.vw_billing_charges_monthly (1 row per invoice)
        breakdown_df: dbo.vw_billing_charges_breakdown (charge lines)
        daily_df: dbo.vw_billing_charges_daily (daily consumption)
    """
    eng = get_engine()
    with eng.begin() as conn:
        logging.info("Connected via SQLAlchemy engine.")
        monthly_df = pd.read_sql("SELECT * FROM dbo.vw_billing_charges_monthly;", conn)
        breakdown_df = pd.read_sql("SELECT * FROM dbo.vw_billing_charges_breakdown;", conn)
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
            "Charge Category","Charge Type","Rate","Rate UOM","Unit","Unit UOM","Amount ex GST"
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
    })
    order = ["Firm Gas Sales", "Spot Gas Sales", "Distribution Charges", "Transport Fee", "Adjustment Charges", "Other Charges"]
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
    """
    Build a dbo.billing_history row from a single vw_billing_charges_monthly row.
    Robust to small column-name variations.
    """
    def get(*names, **kw):
        default = kw.get("default", None)
        for n in names:
            if n in m and pd.notna(m[n]):
                return m[n]
        return default

    # Dates
    bs = pd.to_datetime(get("bill_start_date", "start_date"), errors="coerce")
    be = pd.to_datetime(get("bill_end_date", "end_date"), errors="coerce")
    billing_days = get("billing_days")
    if pd.isna(billing_days) and bs is not None and be is not None:
        billing_days = int((bs.normalize() - pd.Timestamp("1970-01-01")).days - (be.normalize() - pd.Timestamp("1970-01-01")).days)
        billing_days = abs(billing_days) + 1

    # Totals (prefer explicit inc-GST if present)
    total_ex  = get("total_amount", "total_ex_gst", default=0)
    gst_amt   = get("gst_amount", "gst", default=0)
    total_inc = get("total_in_gst_amount", "total_inc_gst")
    if total_inc is None:
        total_inc = (float(total_ex or 0) + float(gst_amt or 0))

    amount_cols = [
        "firm_gas_amount","spot_gas_amount","atco_usage_amount","atco_demand_amount","atco_standing_amount",
        "transport_firm_amount","transport_overrun_amount",
        "gas_adjustment_charges","distribution_adjustment_charges","regulatory_adjustment_charges",
        "admin_fee","late_payment_fee",
    ]

    h: Dict[str, Any] = {
        "invoice_number":        get("invoice_number"),
        "purchase_order_number": get("purchase_order_number","po_number"),
        "company_name":          get("company_name"),
        "company_code":          get("company_code"),
        "account_number":        get("account_number"),
        "mirn":                  get("mirn"),
        "distributor":           get("distributor","network","distributor_name"),
        "bill_start_date":       bs,
        "bill_end_date":         be,
        "billing_days":          billing_days,
        "bill_issue_date":       pd.to_datetime(get("bill_issue_date","issue_date"), errors="coerce"),
        "total_amount":          float(total_ex or 0),
        "gst_amount":            float(gst_amt or 0),
        "total_in_gst_amount":   float(total_inc or 0),
        "generated_at_utc":      datetime.utcnow()
    }

    for c in amount_cols:
        h[c] = float(get(c, default=0) or 0)

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
        bbox_to_anchor=(0.5, 0),
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
    output_path: str,
    fig_height_mm: float = 80.0,
) -> bool:
    if cust_consumption_df.empty:
        return False

    df = cust_consumption_df.copy()
    df["gas_date"] = pd.to_datetime(df["gas_date"], errors="coerce")
    df["gj_consumption"] = pd.to_numeric(df["gj_consumption"], errors="coerce")
    df = df.dropna(subset=["gas_date", "gj_consumption"]).sort_values("gas_date")
    if df.empty:
        return False

    start_date = df["gas_date"].min()
    end_date   = df["gas_date"].max()
    if pd.isna(start_date) or pd.isna(end_date):
        return False

    fig_width_mm = 180.0
    fig = plt.figure(figsize=(fig_width_mm/25.4, fig_height_mm/25.4))
    ax = fig.add_subplot(111)

    light_blue = "#f0f8ff"
    fig.patch.set_facecolor(light_blue)
    ax.set_facecolor(light_blue)

    fig.subplots_adjust(left=0.12, right=0.98, top=0.94, bottom=0.32)

    ax.set_title(f"MIRN: {mirn}    Period: {billing_period}",
                 fontsize=7, fontweight="bold", pad=2)

    ax.bar(df["gas_date"], df["gj_consumption"], label="Daily Consumption", alpha=0.95)

    if contract_mdq is not None and np.isfinite(float(contract_mdq)) and float(contract_mdq) > 0:
        ax.axhline(
            y=float(contract_mdq),
            linewidth=1.5,
            color="#000000",
            label="Contract MDQ",
            zorder=3
        )

    ax.set_xlim(start_date - timedelta(days=1), end_date + timedelta(days=1))
    ax.xaxis.set_major_locator(mdates.AutoDateLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%d %b"))
    ax.margins(x=0.005)

    ax.tick_params(axis="x", labelrotation=45, labelsize=6, pad=1, length=0)
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
            bbox_to_anchor=(0.5, 0.0),
            ncol=max(1, len(labels)),
            frameon=False,
            prop=FontProperties(size=6, weight="bold")
        )

    fig.savefig(output_path, dpi=300, bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close(fig)
    return True


def generate_accounts_mirn_chart(billing_period: str, df: pd.DataFrame, output_path: str) -> bool:
    if df.empty:
        return False

    use = df.copy()
    use["gas_date"] = pd.to_datetime(use["gas_date"], errors="coerce")
    use["gj_consumption"] = pd.to_numeric(use["gj_consumption"], errors="coerce")
    use["mirn"] = use["mirn"].astype(str)
    use = use.dropna(subset=["gas_date", "gj_consumption", "mirn"])
    if use.empty:
        return False

    piv = (use.pivot_table(index="gas_date", columns="mirn", values="gj_consumption", aggfunc="sum")
               .sort_index())
    if piv.empty:
        return False

    start_date = piv.index.min()
    end_date   = piv.index.max()
    if pd.isna(start_date) or pd.isna(end_date):
        return False

    fig_width_mm = 180
    fig_height_mm = 32
    fig = plt.figure(figsize=(fig_width_mm/25.4, fig_height_mm/25.4))
    ax = fig.add_subplot(111)

    light_blue = "#f0f8ff"
    fig.patch.set_facecolor(light_blue)
    ax.set_facecolor(light_blue)

    fig.subplots_adjust(left=0.12, right=0.98, top=0.94, bottom=0.32)

    ax.set_title(f"Aggregated Consumption    Period: {billing_period}",
                 fontsize=7, fontweight="bold", pad=2)

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

    ax.set_xlim(start_date - timedelta(days=1), end_date + timedelta(days=1))
    ax.xaxis.set_major_locator(mdates.AutoDateLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%d %b"))
    ax.margins(x=0.005)

    ax.tick_params(axis="x", labelrotation=45, labelsize=6, pad=1, length=0)
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

    handles, labels = ax.get_legend_handles_labels()
    ncols = max(1, len(labels))
    fig.legend(
        handles, labels,
        loc="lower center",
        bbox_to_anchor=(0.5, 0),
        ncol=ncols,
        frameon=False,
        prop=FontProperties(size=6, weight="bold")
    )

    fig.savefig(output_path, dpi=300, bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close(fig)
    return True

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

def _fetch_history_variants(engine, base_invoice_no: str) -> List[Tuple[str, int, Decimal]]:
    """
    Returns list of tuples (invoice_number, suffix_int, total_inc_norm)
    Only rows where invoice_number == base OR base_\\d+ are kept.
    """
    like = base_invoice_no + "%"
    sql = f"SELECT invoice_number, total_in_gst_amount FROM {HISTORY_TABLE} WHERE invoice_number LIKE ?"
    pat = re.compile(r"^" + re.escape(base_invoice_no) + r"(?:_(\d+))?$")

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

def _apply_history_increment_rule(engine, row: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], str, bool]:
    """
    One history row per invoice_number unless the *same invoice_number* has a different total:
      - If any existing variant (base or suffixed) has the SAME total -> SKIP insert and return that variant's invoice_no.
      - Else create next suffix and INSERT.
      - If no variants exist -> INSERT base as-is.

    Returns: (row_to_insert_or_None, invoice_no_used, inserted_bool)
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

def insert_billing_history_batch(engine, rows: List[Dict[str, Any]]) -> List[Optional[str]]:
    if not rows:
        logging.info("No billing_history rows to insert.")
        return []

    rows = sanitize_history_rows(rows)
    used_invoice_numbers = [r.get("invoice_number") for r in rows if r.get("invoice_number")]

    cols = [
        "invoice_number","purchase_order_number","company_name","company_code","account_number","mirn","distributor",
        "bill_start_date","bill_end_date","billing_days","bill_issue_date",
        "firm_gas_amount","spot_gas_amount","atco_usage_amount","atco_demand_amount","atco_standing_amount",
        "transport_firm_amount","transport_overrun_amount",
        "gas_adjustment_charges","distribution_adjustment_charges","regulatory_adjustment_charges",
        "admin_fee","late_payment_fee",
        "total_amount","gst_amount","total_in_gst_amount",
        "generated_at_utc"
    ]
    placeholders = ",".join(["?"] * len(cols))
    sql = f"INSERT INTO {HISTORY_TABLE} ({','.join(cols)}) VALUES ({placeholders})"

    with engine.begin() as con:
        dbname = con.exec_driver_sql("SELECT DB_NAME()").scalar()
        before = con.exec_driver_sql(f"SELECT COUNT(*) FROM {HISTORY_TABLE}").scalar()
    logging.info(f"Target DB: {dbname}; {HISTORY_TABLE} count before insert: {before}")
    logging.info(f"Inserting {len(rows)} rows; first invoice={rows[0].get('invoice_number') if rows else 'N/A'}")

    inserted = 0
    try:
        with engine.begin() as con:
            for r in rows:
                params = tuple(r.get(c) for c in cols)
                con.exec_driver_sql(sql, params)
                inserted += 1
    except IntegrityError as ie:
        logging.exception(f"IntegrityError inserting billing_history: {ie}")
        raise
    except SQLAlchemyError as se:
        logging.exception(f"SQLAlchemyError inserting billing_history: {se}")
        raise

    with engine.begin() as con:
        after = con.exec_driver_sql(f"SELECT COUNT(*) FROM {HISTORY_TABLE}").scalar()
    logging.info(f"Inserted {inserted} rows. After count: {after} (delta={after - before})")

    return used_invoice_numbers


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
# DEBUG helpers (optional)
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


# =========================
# PDF Class
# =========================
class PDF(FPDF):
    def __init__(self, logo_filename="Agora logo.png", *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Download logo from the SharePoint folder specified by FOLDER_LOGO
        self.logo_bytes = self.download_file_from_sharepoint(logo_filename)

    def download_file_from_sharepoint(self, logo_filename):
        # This function downloads the file from SharePoint and returns the byte data
        ctx = get_sharepoint_context()
        folder = ctx.web.get_folder_by_server_relative_url(FOLDER_LOGO)
        try:
            file = folder.get_file_by_server_relative_url(logo_filename)
            file_bytes = file.download().execute_query()
            return file_bytes
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
    # load views
    monthly, breakdown, daily = load_views()
    if monthly.empty:
        logging.error("No data returned from dbo.vw_billing_charges_monthly. Exiting.")
        return
    if breakdown.empty:
        logging.error("No data returned from dbo.vw_billing_charges_breakdown. Exiting.")
        return

    headers = build_invoice_headers_from_monthly(monthly)

    # engine & table check
    eng = get_engine()
    ensure_billing_history_table(eng)
    history_rows: List[Dict[str, Any]] = []

    # ---- For each invoice ----
    # iterate invoices
    for _, inv in headers.iterrows():
        pdf: Optional[PDF] = None

        invoice_no = inv.get("invoice_number")

        # Build charges
        invoice_lines = breakdown[breakdown["invoice_number"] == invoice_no].copy()
        charges_df = build_charges_df_from_breakdown(invoice_lines)

        if charges_df.empty:
            logger.info(f"No charge lines found for invoice {invoice_no}; skipping PDF and history.")
            continue

        # Header/meta (directly from monthly)
        acct = inv.get("account_number")
        mirn = inv.get("mirn")
        company_name = inv.get("company_name")
        company_code = inv.get("company_code")
        customer_number = inv.get("customer_number")
        abn = inv.get("abn")
        state = inv.get("state")
        distributor_name = inv.get("distributor")
        purchase_order = inv.get("purchase_order_number")
        postal_address = inv.get("postal_address")
        premises_address = inv.get("premises_address")
        contact_number = inv.get("contact_number")
        emergency_contact = inv.get("emergency_number")
        transmission_pipeline = inv.get("transmission_pipeline")
        issue_date = inv.get("bill_issue_date")
        due_date = inv.get("bill_due_date")
        start_date = inv.get("bill_start_date")
        end_date = inv.get("bill_end_date")
        billing_days = inv.get("billing_days")
        start_date_dt = pd.to_datetime(start_date, errors="coerce")
        end_date_dt   = pd.to_datetime(end_date,   errors="coerce")
        billing_period = (
            f"{start_date_dt.strftime('%d-%b-%y')} to {end_date_dt.strftime('%d-%b-%y')}"
            if pd.notna(start_date_dt) and pd.notna(end_date_dt) else "TBC"
        )
        distributor_mhq = inv.get("distribution_mhq")
        charge_notes = inv.get("notes")

        # Totals from monthly (already numeric/rounded)
        invoice_ex_gst = float(inv.get("total_amount") or 0.0)
        gst = float(inv.get("gst_amount") or 0.0)
        invoice_amount = float(inv.get("total_in_gst_amount") or (invoice_ex_gst + gst))

        # History row check
        hist_row = build_history_row_from_monthly(inv)
        r2, invoice_no, do_insert = _apply_history_increment_rule(eng, hist_row)

        if not do_insert:
            skipped_duplicates += 1
            logger.info(f"Duplicate invoice (same total) detected: {invoice_no}; skipping PDF and history.")
            continue   # skip everything (PDF + SharePoint)

        # ---- PDF Setup ----
        pdf = PDF()
        pdf.add_page()

        # Title
        pdf.set_font("Arial", "", 16)
        pdf.set_x(10); pdf.set_y(35)
        pdf.cell(0, 10, "Tax Invoice", new_x=XPos.LMARGIN, new_y=YPos.NEXT, align="L")
        pdf.ln(10)

        # Helper
        def write_label_value(
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
        write_label_value("Customer Number", customer_number, x=10)
        write_label_value("Account Number", acct, x=10)
        write_label_value("Premises Address", premises_address, x=10, wrap=True, force_two_lines=True)
        write_label_value("Purchase Order #", purchase_order, x=10)
        write_label_value("Tax Invoice No.", invoice_no, x=10)
        start_dt = pd.to_datetime(start_date, errors="coerce")
        end_dt   = pd.to_datetime(end_date,   errors="coerce")
        billing_period_lbl = (
            f"{start_dt.strftime('%d-%b-%y')} to {end_dt.strftime('%d-%b-%y')}"
            if pd.notna(start_dt) and pd.notna(end_dt) else ""
        )
        write_label_value("Billing Period", billing_period_lbl, x=10)

        # Right: Issue/Due/Total
        right_start_x = block_start_x
        label_w_right = label_width
        value_w_right = value_width
        pdf.set_xy(right_start_x, 83)
        issue_dt = pd.to_datetime(issue_date, errors="coerce")
        due_dt   = pd.to_datetime(due_date,   errors="coerce")

        write_label_value(
            "Issue Date",
            issue_dt.strftime("%d-%b-%y") if pd.notna(issue_dt) else "",
            x=right_start_x, label_w=label_w_right, value_w=value_w_right,
            align="R", bold_size=10, reg_size=10.5
        )

        write_label_value(
            "Due Date",
            due_dt.strftime("%d-%b-%y") if pd.notna(due_dt) else "",
            x=right_start_x, label_w=label_w_right, value_w=value_w_right,
            align="R", bold_size=10, reg_size=10.5
        )

        pdf.ln(14)
        write_label_value(
            "Total Amount Payable", f"${invoice_amount:,.2f}",
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
        pdf.set_font("Arial", "B", 9.5); pdf.cell(40, 4, f"${float(invoice_ex_gst):,.2f}", new_x=XPos.LMARGIN, new_y=YPos.NEXT, align='R')
        pdf.set_font("Arial", "B", 9); pdf.cell(50, 4, "GST Payable on Current Charges", align='L')
        pdf.set_font("Arial", "B", 9.5); pdf.cell(40, 4, f"${float(gst):,.2f}", new_x=XPos.LMARGIN, new_y=YPos.NEXT, align='R')
        pdf.line(75, 194, 100, 194)
        pdf.set_font("Arial", "B", 9); pdf.cell(50, 8, "Total of Current Charges Inc GST", align='L')
        pdf.set_font("Arial", "B", 9.5); pdf.cell(40, 8, f"${float(invoice_amount):,.2f}", new_x=XPos.LMARGIN, new_y=YPos.NEXT, align='R')
        pdf.line(10, 199.5, 100, 199.5)


        # ---- Pie Chart (right) ----
        if not s_for_pie_cat.empty:
            buf1 = generate_pie_chart(s_for_pie_cat, custom_colors)
            pdf.image(buf1, x=115, y=123.5, w=80)
            logger.info(f"Styled pie chart embedded for invoice {invoice_no} at x=115, y=123.5, w=80")

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
        pdf.cell(80, 6, f"          Reference No. {invoice_no}", fill=True, new_x=XPos.LMARGIN, new_y=YPos.NEXT)
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
        pdf.cell(80, 6, f"          Reference No. {invoice_no}", fill=True, new_x=XPos.LMARGIN, new_y=YPos.NEXT)
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

        # ====================================================== PAGE 2 ====================================================== 
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
        write_label_value("Account No.", acct, x=10, label_w=label_width_left)
        write_label_value("MIRN", mirn, x=10, label_w=label_width_left)
        write_label_value("Transmission Pipeline (if any)", transmission_pipeline, x=10, label_w=label_width_left)
        write_label_value("Distributor", distributor_name, x=10, label_w=label_width_left)
        write_label_value(
            "Distributor MHQ",
            (f"{float(distributor_mhq):.2f}" if (pd.notna(distributor_mhq) and str(distributor_mhq).strip() != "") else ""),
            x=10, label_w=label_width_left, unit=" GJ",
        )
        pdf.ln(2)

        # Billing Details (Right Block)
        pdf.set_xy(right_x, 35)
        write_label_value("Billing Period", billing_period, x=105, label_w=label_width_right, value_w=right_value_width)
        pdf.ln(5.5)
        write_label_value("Premises Address", premises_address, x=105, wrap=True, force_two_lines=True, label_w=label_width_right, value_w=right_value_width)

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
        pdf.cell(90, 4, f"${float(invoice_ex_gst):,.2f}", new_x=XPos.LMARGIN, new_y=YPos.NEXT, align='R')
        pdf.cell(90, 4, "GST Payable on Current Charges", align='L')
        pdf.cell(90, 4, f"${float(gst):,.2f}", new_x=XPos.LMARGIN, new_y=YPos.NEXT, align='R')
        pdf.line(10, pdf.get_y()+1, 190, pdf.get_y()+1)

        # Daily consumption
        cust_consumption = pd.DataFrame()
        if not daily.empty and "mirn" in daily.columns:
            cust_consumption = daily[daily["mirn"] == mirn].copy()
        
        contract_mdq: Optional[float] = None
        monthly_mdq: Optional[float] = None

        # === MDQ box above Consumption Chart ===
        pdf.set_xy(10, 145)
        label_w = 30
        value_w = 60

        def fmt_gj(x: Any) -> str:
            try:
                return f"{float(x):.2f} GJ"
            except (TypeError, ValueError):
                return ""

        pdf.set_font("Arial", "BU", 8); pdf.cell(label_w, 4, "MDQ of the month")
        pdf.set_font("Arial", "U", 8);  pdf.cell(value_w, 4, fmt_gj(monthly_mdq), new_x=XPos.LMARGIN, new_y=YPos.NEXT)

        pdf.set_font("Arial", "BU", 8); pdf.cell(label_w, 4, "Contract MDQ")
        val = "" if contract_mdq is None else fmt_gj(contract_mdq)
        pdf.set_font("Arial", "U", 8);  pdf.cell(value_w, 4, val, new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        pdf.rect(10, 145, 70, 8)
        pdf.ln(1)

        # === NEW CHART: only for these accounts ===
        accounts = (30007, 30008, 30009)
        multi_mirn_ok = False
        aggregated_monthly_mdq: Optional[float] = None

        acct_num = pd.to_numeric(acct, errors="coerce")
        if pd.notna(acct_num) and int(acct_num) in accounts:
            need_cols = {"account_number", "gas_date", "gj_consumption", "mirn"}
            if not daily.empty and need_cols.issubset(daily.columns):
                acc_df = daily[list(need_cols)].copy()
                acc_df["account_number"] = pd.to_numeric(acc_df["account_number"], errors="coerce").astype("Int64")
                acc_df["gas_date"] = pd.to_datetime(acc_df["gas_date"], errors="coerce")
                acc_df["gj_consumption"] = pd.to_numeric(acc_df["gj_consumption"], errors="coerce")
                acc_df = acc_df.dropna(subset=["account_number", "gas_date", "gj_consumption", "mirn"])
                acc_df = acc_df[acc_df["account_number"].isin(accounts)]

                if not acc_df.empty and pd.notna(start_date_dt) and pd.notna(end_date_dt):
                    acc_df = acc_df[(acc_df["gas_date"] >= start_date_dt) & (acc_df["gas_date"] <= end_date_dt)]

                if not acc_df.empty:
                    daily_totals = (acc_df.groupby("gas_date")["gj_consumption"].sum(min_count=1))
                    if not daily_totals.empty:
                        aggregated_monthly_mdq = float(daily_totals.max())

                    # --- Generate Multi-MIRN Chart in memory ---
                    fig_m, ax_m = plt.subplots(figsize=(7, 3))
                    ax_m.plot(daily_totals.index, daily_totals.values, marker="o")
                    ax_m.set_title(f"Aggregated MIRN Consumption ({billing_period})")
                    ax_m.xaxis.set_major_formatter(mdates.DateFormatter("%d %b"))
                    plt.xticks(rotation=45, fontsize=6)
                    plt.tight_layout()

                    buf_m = BytesIO()
                    fig_m.savefig(buf_m, format="png", dpi=300, bbox_inches="tight")
                    buf_m.seek(0)
                    plt.close(fig_m)

                    multi_mirn_ok = True

        # === Chart sizing constants ===
        CHART_W_MM      = 180.0
        CHART_FULL_H_MM = 64.0
        CHART_HALF_H_MM = 32.0

        want_half = bool(multi_mirn_ok)
        first_h_mm = CHART_HALF_H_MM if want_half else CHART_FULL_H_MM

        # Ensure new page if needed before drawing charts
        def _ensure_space(h_mm: float) -> None:
            try:
                page_h = getattr(pdf, "h", 297)
                b_margin = getattr(pdf, "b_margin", 10)
                if pdf.get_y() + h_mm > (page_h - b_margin):
                    pdf.add_page()
            except Exception:
                pdf.add_page()

        # === Consumption chart ===
        if not cust_consumption.empty:
            fig_c, ax_c = plt.subplots(figsize=(7, 3))
            ax_c.bar(cust_consumption["gas_date"], cust_consumption["gj_consumption"], alpha=0.95)
            pdf.set_font("Arial", "", 8)
            ax_c.set_title(f"{mirn}    Period {billing_period}")
            ax_c.xaxis.set_major_formatter(mdates.DateFormatter("%d %b"))
            plt.xticks(rotation=45, fontsize=6)
            plt.tight_layout()

            buf_c = BytesIO()
            fig_c.savefig(buf_c, format="png", dpi=300, bbox_inches="tight")
            buf_c.seek(0)
            plt.close(fig_c)

            _ensure_space(first_h_mm + 2)
            y_start = pdf.get_y()
            pdf.image(buf_c, x=10, y=y_start, w=CHART_W_MM, h=first_h_mm)
            pdf.set_y(y_start + first_h_mm + 2)
            logger.info(f"Consumption chart embedded for invoice {invoice_no}")

        # === Multi-MIRN chart (stacked below) ===
        if multi_mirn_ok:
            current_y = pdf.get_y()

            # Aggregated MDQ box
            label_w = 30
            value_w = 60
            pdf.set_font("Arial", "BU", 8); pdf.cell(label_w + 20, 4, "MDQ of the month (aggregated)")
            pdf.set_font("Arial", "U", 8);  pdf.cell(value_w, 4, fmt_gj(aggregated_monthly_mdq), new_x=XPos.LMARGIN, new_y=YPos.NEXT)

            pdf.set_font("Arial", "BU", 8); pdf.cell(label_w + 20, 4, "Contract MDQ (aggregated)")
            pdf.set_font("Arial", "U", 8);  pdf.cell(value_w, 4, fmt_gj(3000), new_x=XPos.LMARGIN, new_y=YPos.NEXT)

            pdf.rect(10, current_y, 70, 8)
            pdf.ln(2)

            # Embed chart directly below
            pdf.image(buf_m, x=10, w=CHART_W_MM, h=CHART_HALF_H_MM)
            pdf.set_y(current_y + CHART_HALF_H_MM + 10)
            logger.info(f"Aggregated multi-MIRN chart embedded for invoice {invoice_no}")



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


        # ---- Save PDF to SharePoint (filename always matches SQL invoice_no) ----
        pdf_filename = f"{invoice_no}_Generated_{GENERATE_DATE}.pdf"
        pdf_bytes = BytesIO()
        pdf.output(pdf_bytes)
        pdf_bytes.seek(0)

        upload_bytes_to_sharepoint(pdf_bytes.read(), pdf_filename)
        logger.info(f"Bill uploaded: {pdf_filename}")

        # ---- Insert into history ----
        try:
            if r2 is not None and do_insert:
                insert_billing_history_batch(eng, [r2])
                logger.info(f"Inserted billing history row for {invoice_no}")
            else:
                logger.info(f"No new history row inserted for {invoice_no} (duplicate or skipped)")
        except Exception as e:
            logger.error(f"Failed inserting billing_history for {invoice_no}: {e}")




if __name__ == "__main__":
    main()

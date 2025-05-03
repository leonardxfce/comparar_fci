import datetime
import functools
import logging

from fredapi import Fred
import os
import time
import requests
import json
import pandas as pd


# --- Configuration & Constants ---

# File Paths
FCI_TSV_PATH = "fci.tsv"
FCI_XLSX_PATH = "fci.xlsx"

# Column Names
COL_FONDO = "Fondo_Fondo"
COL_PLAZO_LIQ = "Plazo Liq._Plazo Liq."
COL_CODIGO_CLAS = "Código de Clasificación_Código de Clasificación"
COL_MONEDA_FONDO = "Moneda Fondo_Moneda Fondo"
COL_MINIMO_INV = "Mínimo de Inversión_Mínimo de Inversión"
COL_VARIACION_DIARIA = "Variac. %"
COL_VARIACION_YTD_REF = "30/12/24"  # Consider renaming if date changes

# Filtering Constants
FONDOS_PLAZO_CERO_MODIFICAR = [
    "Cocos Daruma Renta Mixta - Clase A",
    "Cocos Ahorro Dólares - Clase A",
]
CLASE_GENERIC_STR = "Clase"
CLASE_A_STR = "Clase A"
MAX_MIN_INVESTMENT_THRESHOLD = 100001
PLAZO_LIQ_CERO = "0"
PLAZO_LIQ_UNO = "1"
PLAZOS_LIQ_PERMITIDOS = {PLAZO_LIQ_CERO, PLAZO_LIQ_UNO}
CURRENCIES_USD = {"USD", "USB"}
MONEY_MARKET_CODE = 3
DEFAULT_CLASSIFICATION_CODE = 100
TOP_N_COUNT = 10

# Financial Calculation Constants
DAYS_IN_YEAR = 365
DAYS_IN_MONTH = 30

# API Configuration
FRED_API_KEY = os.getenv("FRED_API_KEY")
FRED_SERIES_CPI_US = "CPIAUCSL"
URL_ARG_DATOS_UVA = "https://api.argentinadatos.com/v1/finanzas/indices/uva"
URL_ARG_DATOS_DOLAR = "https://api.argentinadatos.com/v1/cotizaciones/dolares"
API_DATE_REF_START_YEAR = (
    "2024-12-31"  # Based on original code '30/12/24' logic for YTD
)

# API Data Keys
API_KEY_FECHA = "fecha"
API_KEY_VALOR = "valor"
API_KEY_CASA = "casa"
API_KEY_COMPRA = "compra"
API_VALUE_CASA_BOLSA = "bolsa"

# Benchmark Configuration Keys
KEY_TNA = "tna"
KEY_LIMITE = "limite"
KEY_NOMBRE = "nombre"

# Benchmark Options Data
BENCHMARK_OPCIONES_CAR = [
    {
        KEY_TNA: 0.228,
        KEY_LIMITE: 500_000,
        KEY_NOMBRE: "Cuenta Remunerada Banco Bica 30% TNA",
    },
    {
        KEY_TNA: 0.3564,
        KEY_LIMITE: 600_000,
        KEY_NOMBRE: "Cuenta Remunerada NaranjaX 31% TNA",
    },
    {
        KEY_TNA: 0.4082,
        KEY_LIMITE: 1_000_000,
        KEY_NOMBRE: "Cuenta Remunerada Uala 35% TNA",
    },
    {
        KEY_TNA: 0.020184,
        KEY_LIMITE: 1_000_000,
        KEY_NOMBRE: "Cuenta Remunerada IOL 2% TNA",
    },
    {
        KEY_TNA: 0.3470,
        KEY_LIMITE: 1_000_000,
        KEY_NOMBRE: "Cuenta Remunerada Uala Base 30% TNA",
    },
]

# --- Data Loading and Preprocessing ---


def process_raw_xlsx_to_tsv(input_path=FCI_XLSX_PATH, output_path=FCI_TSV_PATH):
    try:
        df = pd.read_excel(input_path, header=None)
        header_top_idx = df[df.eq("Fondo").any(axis=1)].index[0]
        header_df = df.iloc[header_top_idx : header_top_idx + 2].copy().ffill(axis=0)
        combined_headers = (
            header_df.iloc[0].astype(str) + "_" + header_df.iloc[1].astype(str)
        )
        combined_headers = combined_headers.str.replace(
            "nan_|_nan|nan_nan", "", regex=True
        ).str.strip()
        df.columns = combined_headers
        df = df.iloc[header_top_idx + 2 :].reset_index(drop=True)

        # Specific row drop logic from original code
        if len(df) > 9 and 9 in df.index:
            df = df.drop(index=9).reset_index(drop=True)

        col_variacion = next(
            (col for col in df.columns if COL_VARIACION_DIARIA in col), None
        )
        if col_variacion:
            df = df[df[col_variacion].notna()].reset_index(drop=True)

        df.to_csv(output_path, sep="\t", index=False)
        print(f"Successfully processed '{input_path}' to '{output_path}'")
    except FileNotFoundError:
        print(f"Error: Input file not found at '{input_path}'")
    except Exception as e:
        print(f"Error processing Excel file: {e}")


def load_prepared_fci_data(
    tsv_path=FCI_TSV_PATH, apply_plazo_zero_mods=True, filter_clase_a=False
):
    try:
        df = pd.read_csv(tsv_path, sep="\t")
        df.columns = df.columns.str.strip()

        if apply_plazo_zero_mods:
            mask_modificar = df.iloc[:, 0].isin(FONDOS_PLAZO_CERO_MODIFICAR)
            df.loc[mask_modificar, COL_PLAZO_LIQ] = 0  # Assuming 0 should be numeric
        if filter_clase_a:
            condicion_clase_a = (
                df[COL_FONDO].str.contains(CLASE_GENERIC_STR, na=False)
            ) & (df[COL_FONDO].str.contains(CLASE_A_STR, na=False))
            condicion_sin_clase = ~df[COL_FONDO].str.contains(
                CLASE_GENERIC_STR, na=False
            )
            df = df[condicion_clase_a | condicion_sin_clase]

        # Ensure correct types for filtering columns
        df[COL_MINIMO_INV] = (
            df[COL_MINIMO_INV]
            .astype(str)
            .str.replace(r"[\.,].*$", "", regex=True)
            .fillna("0")
        )
        df[COL_MINIMO_INV] = (
            pd.to_numeric(df[COL_MINIMO_INV], errors="coerce").fillna(0).astype(int)
        )

        df[COL_PLAZO_LIQ] = df[COL_PLAZO_LIQ].astype(
            str
        )  # Keep as string for filtering '0', '1'

        df[COL_CODIGO_CLAS] = (
            df[COL_CODIGO_CLAS].astype(str).str.replace(r'[," ]', "", regex=True)
        )
        df[COL_CODIGO_CLAS] = (
            pd.to_numeric(df[COL_CODIGO_CLAS], errors="coerce")
            .fillna(DEFAULT_CLASSIFICATION_CODE)
            .astype(int)
        )

        df[COL_MONEDA_FONDO] = df[COL_MONEDA_FONDO].fillna("").astype(str)

        return df

    except FileNotFoundError:
        print(
            f"Error: Prepared TSV file not found at '{tsv_path}'. Run processing function."
        )
        return pd.DataFrame()
    except Exception as e:
        print(f"Error loading prepared FCI data: {e}")
        return pd.DataFrame()


# --- DataFrame Filtering Functions ---


def filter_by_min_investment(df, max_amount=MAX_MIN_INVESTMENT_THRESHOLD):
    if COL_MINIMO_INV not in df.columns:
        print(f"Warning: Column '{COL_MINIMO_INV}' not found for filtering.")
        return df
    return df[df[COL_MINIMO_INV] < max_amount].copy()


def filter_by_plazo_liq(df, plazos_allowed=PLAZOS_LIQ_PERMITIDOS):
    if COL_PLAZO_LIQ not in df.columns:
        print(f"Warning: Column '{COL_PLAZO_LIQ}' not found for filtering.")
        return df
    mask = df[COL_PLAZO_LIQ].astype(str).isin(plazos_allowed)
    return df[mask].copy()


def filter_by_currency(df, currencies=CURRENCIES_USD):
    if COL_MONEDA_FONDO not in df.columns:
        print(f"Warning: Column '{COL_MONEDA_FONDO}' not found for filtering.")
        return df
    mask = df[COL_MONEDA_FONDO].isin(currencies)
    return df[mask].copy()


def filter_by_money_market(df, include_mm=True):
    if COL_CODIGO_CLAS not in df.columns:
        print(f"Warning: Column '{COL_CODIGO_CLAS}' not found for filtering.")
        return df

    if include_mm:
        mask = df[COL_CODIGO_CLAS] == MONEY_MARKET_CODE
    else:
        mask = df[COL_CODIGO_CLAS] != MONEY_MARKET_CODE
    return df[mask].copy()


# --- Analysis and Reporting Functions ---


def get_top_performing_funds(df, n=TOP_N_COUNT, use_ytd=False):
    performance_col = COL_VARIACION_YTD_REF if use_ytd else COL_VARIACION_DIARIA
    required_cols = [
        COL_FONDO,
        COL_VARIACION_DIARIA,
        COL_VARIACION_YTD_REF,
        COL_MONEDA_FONDO,
        COL_CODIGO_CLAS,
        COL_MINIMO_INV,
    ]

    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        print(f"Warning: Missing columns for top performance analysis: {missing_cols}")
        return json.dumps([])  # Return empty JSON array

    df_select = df[required_cols].copy()

    if performance_col not in df_select.columns:
        print(f"Warning: Performance column '{performance_col}' not found.")
        return json.dumps([])

    df_select[performance_col] = pd.to_numeric(
        df_select[performance_col], errors="coerce"
    )
    df_select.dropna(subset=[performance_col], inplace=True)

    df_sorted = df_select.sort_values(by=performance_col, ascending=False)
    top_n_df = df_sorted.head(n).copy()

    return top_n_df.to_json(orient="records", force_ascii=False)


def calculate_remunerated_account_metrics(
    tna: float, limite: float, nombre: str
) -> dict:
    if (
        not isinstance(tna, (int, float))
        or not isinstance(limite, (int, float))
        or tna < 0
        or limite < 0
    ):
        return {
            "nombre": nombre,
            "error": "Invalid input: TNA and limite must be non-negative numbers.",
        }

    tnd = tna / DAYS_IN_YEAR if DAYS_IN_YEAR > 0 else 0
    p0 = (
        limite / ((1 + tnd) ** DAYS_IN_MONTH)
        if (1 + tnd) != 0 and DAYS_IN_MONTH >= 0
        else 0
    )

    if p0 == 0 or DAYS_IN_MONTH == 0:
        rendimiento_promedio_diario_a = 0
    else:
        rendimiento_promedio_diario_a = ((limite / p0) - 1) / DAYS_IN_MONTH

    rendimiento_promedio_diario_b = tnd

    return {
        "nombre": nombre,
        "monto_inicial_recomendado": round(p0, 0),
        "rendimiento_mejor_%": round(rendimiento_promedio_diario_a * 100, 3),
        "rendimiento_topeado_%": round(rendimiento_promedio_diario_b * 100, 3),
    }


def get_benchmark_remunerated_accounts():
    return [
        calculate_remunerated_account_metrics(
            opcion[KEY_TNA], opcion[KEY_LIMITE], opcion[KEY_NOMBRE]
        )
        for opcion in BENCHMARK_OPCIONES_CAR
    ]


def get_us_ytd_inflation(series_id=FRED_SERIES_CPI_US, api_key=FRED_API_KEY):
    if not api_key or api_key == "YOUR_API_KEY_HERE":  # Basic check
        print("Error: FRED API key not configured.")
        return None
    try:
        fred = Fred(api_key=api_key)
        current_year = datetime.datetime.now().year
        start_date = f"{current_year}-01-01"
        end_date = datetime.date.today().strftime("%Y-%m-%d")

        cpi_data = fred.get_series(series_id, start_date=start_date)

        if cpi_data is None or cpi_data.empty:
            print(f"No data retrieved for series {series_id} starting {start_date}.")
            return None

        # Ensure we have data for the start and end of the period
        cpi_data = cpi_data.dropna()
        if len(cpi_data) < 2:
            print(
                f"Insufficient data points ({len(cpi_data)}) for YTD calculation for series {series_id} in {current_year}."
            )
            return None

        # Use first available point in the year and the last available point
        month = (datetime.datetime.now().month - 1) * -1
        cpi_start = cpi_data.iloc[month]
        cpi_end = cpi_data.iloc[-1]

        if cpi_start == 0:
            print(
                f"Error: Starting CPI value is zero for series {series_id} in {current_year}."
            )
            return None

        inflation_ytd = ((cpi_end / cpi_start) - 1) * 100
        return round(inflation_ytd, 3)

    except Exception as e:
        print(f"Error getting US YTD inflation from FRED: {e}")
        return None


def fetch_api_data(url):
    try:
        response = requests.get(url, timeout=10)  # Add timeout
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from {url}: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON from {url}: {e}")
        return None


def get_argentina_financial_indicators():
    results = {}
    try:
        today = datetime.date.today()
        start_of_year_date = datetime.date(today.year, 1, 1)
        days_elapsed = (today - start_of_year_date).days + 1
        yesterday_date_str = (today - datetime.timedelta(days=1)).strftime("%Y-%m-%d")

        results["calculation_date"] = today.strftime("%Y-%m-%d")
        results["days_elapsed_current_year"] = days_elapsed
        results["reference_date_yesterday"] = yesterday_date_str
        results["reference_date_start_year_api"] = API_DATE_REF_START_YEAR

        # --- US Inflation (YTD) ---
        us_inflation = get_us_ytd_inflation()
        results["inflacion_usa_ytd_%"] = (
            us_inflation if us_inflation is not None else "Error fetching"
        )

        # --- Argentina UVA Inflation ---
        data_uva = fetch_api_data(URL_ARG_DATOS_UVA)
        if data_uva:
            points_uva = {
                item[API_KEY_FECHA]: item[API_KEY_VALOR]
                for item in data_uva
                if item.get(API_KEY_FECHA)
                in [API_DATE_REF_START_YEAR, yesterday_date_str]
                and item.get(API_KEY_VALOR) is not None
            }

            start_val_uva = points_uva.get(API_DATE_REF_START_YEAR)
            end_val_uva = points_uva.get(yesterday_date_str)

            if (
                start_val_uva is not None
                and end_val_uva is not None
                and start_val_uva != 0
            ):
                total_inflacion = ((end_val_uva / start_val_uva) - 1) * 100
                anualizada_inflacion = (
                    (total_inflacion / days_elapsed) * DAYS_IN_YEAR
                    if days_elapsed > 0
                    else 0
                )
                results["inflacion_uva"] = {
                    "ytd_%": round(total_inflacion, 2),
                    "anualizada_estimada_%": round(anualizada_inflacion, 2),
                }
            else:
                results["inflacion_uva"] = {
                    "error": "Missing required UVA data points or start value is zero."
                }
        else:
            results["inflacion_uva"] = {"error": "Failed to fetch UVA data from API."}

        # --- Argentina Dolar Bolsa Variation ---
        data_dolar = fetch_api_data(URL_ARG_DATOS_DOLAR)
        if data_dolar:
            points_dolar = {
                item[API_KEY_FECHA]: item[API_KEY_COMPRA]
                for item in data_dolar
                if item.get(API_KEY_CASA) == API_VALUE_CASA_BOLSA
                and item.get(API_KEY_FECHA)
                in [API_DATE_REF_START_YEAR, yesterday_date_str]
                and item.get(API_KEY_COMPRA) is not None
            }

            start_val_dolar = points_dolar.get(API_DATE_REF_START_YEAR)
            end_val_dolar = points_dolar.get(yesterday_date_str)

            if (
                start_val_dolar is not None
                and end_val_dolar is not None
                and start_val_dolar != 0
            ):
                total_variacion = ((end_val_dolar / start_val_dolar) - 1) * 100
                results["variacion_dolar_bolsa_compra_ytd_%"] = round(
                    total_variacion, 2
                )
            else:
                results["variacion_dolar_bolsa_compra_ytd_%"] = {
                    "error": "Missing required Dolar Bolsa data points or start value is zero."
                }
        else:
            results["variacion_dolar_bolsa_compra_ytd_%"] = {
                "error": "Failed to fetch Dolar data from API."
            }

    except Exception as e:
        print(f"Error calculating financial indicators: {e}")
        results["general_error"] = str(e)

    return results


def download_cafci_xlsx(output_filename="fci.xlsx"):
    unix_timestamp = int(time.time())
    base_url = "https://api.cafci.org.ar/pb_get"
    url = f"{base_url}?d={unix_timestamp}"

    print(f"Intentando descargar desde: {url}")

    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()  # Lanza un HTTPError para respuestas de error (4xx o 5xx)
        with open(output_filename, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        print(f"Archivo descargado exitosamente y guardado como {output_filename}")
        return True
    except requests.exceptions.RequestException as e:
        print(f"Error durante la solicitud: {e}")
        return False
    except Exception as e:
        print(f"Ocurrió un error inesperado: {e}")
        return False

@functools.cache
def actualizar_plazo_liquidacion_fci():
    print("Consultando PPI para arreglar T+0")
    url = "https://api.portfoliopersonal.com/api/Cotizaciones/FCI/Obtener"
    payload = {
        "tipo": 160,
        "categoria": [17, 4],
        "familia": [],
        "riesgo": [],
        "plazoRescate": [],
        "permanenciaSugerida": [],
        "moneda": [],
        "patrimonio": [],
        "benchmark": [],
    }

    headers = {
        "accept": "application/json, text/plain, */*",
        "accept-language": "en-US,en;q=0.9,es-AR;q=0.8,es;q=0.7",
        "authorizedclient": "321321321",
        "cache-control": "no-cache",
        "clientkey": "pp123456",
        "content-type": "application/json",
    }

    isins_from_api = []
    try:
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        data = response.json()

        if data and data.get("status") == 0 and isinstance(data.get("payload"), list):
            for item_data in data.get("payload", []):
                isin = item_data.get("item", {}).get("isin")
                if isin:
                    isins_from_api.append(isin)

    except requests.exceptions.RequestException as e:
        logging.error(e)

    return isins_from_api


def fix_missing_t0(df):
    isins_from_api = actualizar_plazo_liquidacion_fci()
    if (
        isins_from_api
        and "Código CAFCI_Código CAFCI" in df.columns
        and "Plazo Liq._Plazo Liq." in df.columns
    ):
        rows_to_update = (
            df["Código CAFCI_Código CAFCI"].astype(str).isin(isins_from_api)
        )
        df.loc[rows_to_update, "Plazo Liq._Plazo Liq."] = 0
    return df

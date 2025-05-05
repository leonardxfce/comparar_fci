import json
from bs4 import BeautifulSoup

try:
    from utils import (
        load_prepared_fci_data,
        filter_by_plazo_liq,
        filter_by_money_market,
        filter_by_currency,
        get_top_performing_funds,
        get_benchmark_remunerated_accounts,
        get_argentina_financial_indicators,
        process_raw_xlsx_to_tsv,
        PLAZO_LIQ_CERO,
        download_cafci_xlsx,
        fix_missing_t0,
    )
except ImportError:
    print(
        "Error: Could not import from 'base_refactored.py'. "
        "Ensure the refactored base file exists and is accessible."
    )
    exit()


# --- Configuration & Constants ---

HTML_FILENAME = "index.html"
HTML_ENCODING = "utf-8"
SCRIPT_TYPE = "application/json"
SCRIPT_CLASS_DATA = "fci_data"  # Class to identify data scripts for removal/updates
ID_PREFIX = "data_"
ID_BASE_SUFFIX = "base"
ID_BENCHMARK = "benchmarkGarantizado"
ID_FINANCIAL_DATA = "datosFinancieros"

# Define the combinations of filters and their corresponding ID parts
FILTER_COMBINATIONS = [
    # (filter_clase_a, use_ytd, filter_mm, filter_usd), id_suffix_parts
    {
        "flags": {
            "filter_clase_a": False,
            "use_ytd": False,
            "filter_mm": False,
            "filter_usd": False,
        },
        "id_parts": [],
    },
    {
        "flags": {
            "filter_clase_a": False,
            "use_ytd": False,
            "filter_mm": False,
            "filter_usd": True,
        },
        "id_parts": ["usd"],
    },
    {
        "flags": {
            "filter_clase_a": False,
            "use_ytd": False,
            "filter_mm": True,
            "filter_usd": False,
        },
        "id_parts": ["mm"],
    },
    {
        "flags": {
            "filter_clase_a": False,
            "use_ytd": False,
            "filter_mm": True,
            "filter_usd": True,
        },
        "id_parts": ["mm", "usd"],
    },  # Note: Original code didn't filter MM and USD together on Plazo 0? Added for completeness. Review if needed.
    {
        "flags": {
            "filter_clase_a": False,
            "use_ytd": True,
            "filter_mm": False,
            "filter_usd": False,
        },
        "id_parts": ["ytd"],
    },
    {
        "flags": {
            "filter_clase_a": False,
            "use_ytd": True,
            "filter_mm": False,
            "filter_usd": True,
        },
        "id_parts": ["ytd", "usd"],
    },
    {
        "flags": {
            "filter_clase_a": False,
            "use_ytd": True,
            "filter_mm": True,
            "filter_usd": False,
        },
        "id_parts": ["ytd", "mm"],
    },
    {
        "flags": {
            "filter_clase_a": False,
            "use_ytd": True,
            "filter_mm": True,
            "filter_usd": True,
        },
        "id_parts": ["ytd", "mm", "usd"],
    },  # As above
    {
        "flags": {
            "filter_clase_a": True,
            "use_ytd": False,
            "filter_mm": False,
            "filter_usd": False,
        },
        "id_parts": ["sa"],
    },
    {
        "flags": {
            "filter_clase_a": True,
            "use_ytd": False,
            "filter_mm": False,
            "filter_usd": True,
        },
        "id_parts": ["sa", "usd"],
    },
    {
        "flags": {
            "filter_clase_a": True,
            "use_ytd": False,
            "filter_mm": True,
            "filter_usd": False,
        },
        "id_parts": ["sa", "mm"],
    },
    {
        "flags": {
            "filter_clase_a": True,
            "use_ytd": False,
            "filter_mm": True,
            "filter_usd": True,
        },
        "id_parts": ["sa", "mm", "usd"],
    },  # As above
    {
        "flags": {
            "filter_clase_a": True,
            "use_ytd": True,
            "filter_mm": False,
            "filter_usd": False,
        },
        "id_parts": ["sa", "ytd"],
    },
    {
        "flags": {
            "filter_clase_a": True,
            "use_ytd": True,
            "filter_mm": False,
            "filter_usd": True,
        },
        "id_parts": ["sa", "ytd", "usd"],
    },
    {
        "flags": {
            "filter_clase_a": True,
            "use_ytd": True,
            "filter_mm": True,
            "filter_usd": False,
        },
        "id_parts": ["sa", "ytd", "mm"],
    },
    {
        "flags": {
            "filter_clase_a": True,
            "use_ytd": True,
            "filter_mm": True,
            "filter_usd": True,
        },
        "id_parts": ["sa", "ytd", "mm", "usd"],
    },  # As above
]

# --- Core Logic Functions ---


def generate_fci_json_for_combination(filter_clase_a, use_ytd, filter_mm, filter_usd):
    """
    Loads data, applies filters based on flags, and returns top 10 funds as JSON.

    Note: This function implicitly filters for Plazo Liq = 0 based on the
          original script's logic within the loop structure.
          It also assumes base data loading handles 'Clase A' filtering if needed.
          The df_min filter from the original code is omitted as it was commented out.
    """
    # Load base data - applying 'Clase A' filter here if specified
    df = load_prepared_fci_data(filter_clase_a=filter_clase_a)
    if df.empty:
        return json.dumps([])  # Return empty list if loading failed
    df = fix_missing_t0(df)
    # Apply mandatory Plazo Liq = 0 filter (based on original script structure)
    df = filter_by_plazo_liq(df, plazos_allowed={PLAZO_LIQ_CERO})

    # Apply conditional filters
    if filter_mm:
        df = filter_by_money_market(df, include_mm=True)
    if filter_usd:
        df = filter_by_currency(
            df
        )  # Assumes default USD currencies from base_refactored

    # Get top 10 based on YTD flag
    top_10_json = get_top_performing_funds(df, use_ytd=use_ytd)
    return top_10_json


# Potential Migration Candidate:
# The function `generate_fci_json_for_combination` could potentially be moved
# to the `base_refactored.py` file if that file is intended to hold all
# data processing and report generation logic. However, keeping it here
# maintains a clearer separation between the main HTML update process and
# the underlying data utilities.


def create_script_tag(tag_id, json_data, script_type=SCRIPT_TYPE, css_class=None):
    """Creates an HTML script tag string with the given ID and JSON data."""
    class_attribute = f' class="{css_class}"' if css_class else ""
    # Use json.dumps for proper JSON formatting within the script tag
    json_string = json.dumps(json_data, indent=2, ensure_ascii=False)
    return f'<script type="{script_type}" id="{tag_id}"{class_attribute}>\n{json_string}\n</script>\n'


def update_html_with_json_data(html_path=HTML_FILENAME):
    """
    Reads an HTML file, removes old data scripts, generates new data scripts
    based on filter combinations, and writes the updated HTML back.
    """
    # 1. Ensure source data is processed (XLSX -> TSV)
    print("Processing source Excel file...")
    process_raw_xlsx_to_tsv()  # Assumes this function handles its own errors/output

    # 2. Read existing HTML
    try:
        with open(html_path, "r", encoding=HTML_ENCODING) as f:
            html_content = f.read()
        print(f"Successfully read '{html_path}'")
    except IOError as e:
        print(f"Error reading HTML file '{html_path}': {e}")
        return

    # 3. Parse HTML and find body
    soup = BeautifulSoup(html_content, "html.parser")
    body = soup.find("body")
    if not body:
        print(f"Error: No <body> tag found in '{html_path}'. Cannot proceed.")
        return

    # 4. Remove old data script tags
    print(
        f"Removing old script tags with type='{SCRIPT_TYPE}' and class='{SCRIPT_CLASS_DATA}'..."
    )
    removed_count = 0
    for old_script in body.find_all(
        "script", type=SCRIPT_TYPE, class_=SCRIPT_CLASS_DATA
    ):
        print(f"  Removing script with ID: {old_script.get('id', 'N/A')}")
        old_script.decompose()
        removed_count += 1
    # Also remove specific benchmark/financial data scripts by ID if they might exist without the class
    for specific_id in [ID_BENCHMARK, ID_FINANCIAL_DATA]:
        script_to_remove = body.find("script", id=specific_id)
        if script_to_remove:
            print(f"  Removing script with ID: {specific_id}")
            script_to_remove.decompose()
            removed_count += 1
    print(f"Removed {removed_count} old script tag(s).")

    # 5. Generate new script tags
    print("Generating new data script tags...")
    new_scripts_html_string = ""

    # Generate FCI combination scripts
    for combo in FILTER_COMBINATIONS:
        flags = combo["flags"]
        id_parts = combo["id_parts"]

        if not id_parts:
            script_id = ID_PREFIX + ID_BASE_SUFFIX
        else:
            script_id = ID_PREFIX + "_".join(id_parts)

        print(f"  Generating data for ID: {script_id}...")
        # Generate JSON string using the dedicated function
        json_string = generate_fci_json_for_combination(**flags)
        # Create the script tag HTML - use json.loads then json.dumps for consistent formatting
        try:
            json_data_obj = json.loads(json_string)
            script_tag_html = create_script_tag(
                script_id, json_data_obj, css_class=SCRIPT_CLASS_DATA
            )
            new_scripts_html_string += script_tag_html
            print(f"    Generated script tag for ID: {script_id}")
        except json.JSONDecodeError:
            print(
                f"    Warning: Could not decode JSON for ID {script_id}. Skipping script tag."
            )

    # Generate Benchmark script
    print(f"  Generating data for ID: {ID_BENCHMARK}...")
    benchmark_data = get_benchmark_remunerated_accounts()
    script_tag_html = create_script_tag(
        ID_BENCHMARK, benchmark_data
    )  # No class needed? Add if required.
    new_scripts_html_string += script_tag_html
    print(f"    Generated script tag for ID: {ID_BENCHMARK}")

    # Generate Financial Data script
    print(f"  Generating data for ID: {ID_FINANCIAL_DATA}...")
    financial_data = get_argentina_financial_indicators()
    script_tag_html = create_script_tag(
        ID_FINANCIAL_DATA, financial_data
    )  # No class needed? Add if required.
    new_scripts_html_string += script_tag_html
    print(f"    Generated script tag for ID: {ID_FINANCIAL_DATA}")

    # 6. Append new scripts to HTML body
    print("Appending new script tags to HTML body...")
    new_scripts_soup = BeautifulSoup(new_scripts_html_string, "html.parser")
    # Iterate through the parsed tags and append them
    for new_tag in new_scripts_soup.find_all("script"):
        body.append(new_tag)

    # 7. Write updated HTML back to file
    try:
        with open(html_path, "w", encoding=HTML_ENCODING) as f:
            # Use prettify to maintain readable HTML structure
            f.write(soup.prettify(formatter="html5"))
        print(f"Successfully updated '{html_path}' with new data scripts.")
    except IOError as e:
        print(f"Error writing updated HTML file '{html_path}': {e}")


# --- Main Execution ---

if __name__ == "__main__":
    print("Starting HTML update process...")
    download_cafci_xlsx()
    update_html_with_json_data()
    print("HTML update process finished.")

import logging
from datetime import datetime, timedelta, timezone
from ingest_app.app.api.client import EntsoeClient
from ingest_app.app.config import settings
from ingest_app.app.exceptions import NoDataFoundError, InvalidIntervalError

# Setup simple logging
logging.basicConfig(level=logging.ERROR)

def check_codes():
    print(f"--- Starting EIC Code Audit for Day-Ahead Prices ---")
    print(f"Checking {len(settings.EIC_CODES_GENERATION)} codes from generation list...")

    # Initialize client
    client = EntsoeClient(api_key=settings.ENTSOE_API_KEY)
    
    # Window: Use yesterday to ensure data is definitely published
    end = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    start = end - timedelta(days=1)
    
    # Parameters for Day-Ahead Prices (A44)
    base_params = {
        "documentType": "A44",
        "contract_MarketAgreement.type": "A01", # Day-ahead only
        "periodStart": start.strftime("%Y%m%d%H%M"),
        "periodEnd": end.strftime("%Y%m%d%H%M"),
    }
    
    working_codes = []
    failed_codes = []

    for code in settings.EIC_CODES_GENERATION:
        params = base_params.copy()
        params["in_Domain"] = code
        params["out_Domain"] = code
        
        try:
            # We don't need to parse the XML, just seeing if we get bytes back is enough
            client.get_data(params)
            print(f"✅ [OK]   {code}")
            working_codes.append(code)

        except (NoDataFoundError, InvalidIntervalError):
            print(f"❌ [FAIL] {code} (No Data/Invalid)")
            failed_codes.append(code)
        except Exception as e:
            print(f"⚠️ [ERR]  {code} ({str(e)})")
            failed_codes.append(code)
            
    print(f"\n--- Audit Complete ---")
    print(f"Working: {len(working_codes)}")
    print(f"Failed:  {len(failed_codes)}")
    
    # Save the working codes to a new config file
    output_path = "ingest_app/config/eic_codes_pricing.txt"
    with open(output_path, "w") as f:
        for code in working_codes:
            f.write(f"{code}\n")
    
    print(f"Saved working codes to {output_path}")

if __name__ == "__main__":
    check_codes()
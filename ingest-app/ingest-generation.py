import json
import logging
import os
import requests
import sys
import time
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv
from kafka import KafkaProducer
from lxml import etree
from eic_codes import EIC_CODES_GENERATION

load_dotenv()

# ENTSOE API Setup
API_URL = "https://web-api.tp.entsoe.eu/api"
DOCUMENT_TYPE = "A75"
PROCESS_TYPE = "A16"
NSMAP = {"doc": "urn:iec62325.351:tc57wg16:451-6:generationloaddocument:3:0"} # XML namespace for Market Documents
ENTSOE_API_KEY = os.getenv("ENTSOE_API_KEY")
if not ENTSOE_API_KEY:
    raise ValueError("No API key found. Set up the ENTSOE_API_KEY variable")

# Kafka Setup
GENERATION_DATA_TOPIC = "generation-data-raw"
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
if not KAFKA_BOOTSTRAP_SERVERS:
    raise ValueError("No Kafka Bootstrap Servers found. Set up the KAFKA_BOOTSTRAP_SERVERS variable.")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Converts a Period resolution into a relativedelta value to determine the beginning of each Point
def parse_resolution_string(resolution):
    match resolution:
        case "PT15M":
            return relativedelta(minutes=15)
        case "PT30M":
            return relativedelta(minutes=30)
        case "PT60M":
            return relativedelta(minutes=60)
        case "P1D":
            return relativedelta(days=1)
        case "P7D":
            return relativedelta(days=7)
        case "P1M":
            return relativedelta(months=1)
        case "P1Y":
            return relativedelta(years=1)
        case _:
            return None


# Converts a <Period> xml element to a list of dicts with attributes "start_time", "end_time", and "quantity"
def parse_period_xml(period_xml, curve_type="A01"):

    # Get start and end intervals
    start_str = period_xml.xpath(".//doc:start", namespaces=NSMAP)[0].text.replace("Z", "+00:00").strip()
    end_str = period_xml.xpath(".//doc:end", namespaces=NSMAP)[0].text.replace("Z", "+00:00").strip()
    start_dt = datetime.fromisoformat(start_str)
    end_dt = datetime.fromisoformat(end_str)

    # Get the duration of each Point in the Period (i.e. the resolution)
    resolution_str = period_xml.xpath(".//doc:resolution", namespaces=NSMAP)[0].text.strip()
    resolution_delta = parse_resolution_string(resolution_str)

    # Get the Points and convert them to intervals with quantities
    intervals = []
    points = period_xml.xpath(".//doc:Point", namespaces=NSMAP)
    if curve_type == "A01":
        curr_dt = start_dt
        next_dt = curr_dt + resolution_delta
        for point in points:
            quantity = float(point.xpath(".//doc:quantity", namespaces=NSMAP)[0].text.strip())
            if quantity == None:
                quantity = 0
            interval = {
                "start_time": curr_dt.isoformat(),
                "end_time": next_dt.isoformat(),
                "quantity": quantity,
            }
            intervals.append(interval)
            curr_dt = next_dt
            next_dt += resolution_delta

    elif curve_type == "A03":

        # The "A03" curve type uses a segmented Point representation to save space
        # Each Point in the Period can represent more than 1 interval
        num_positions = int((end_dt - start_dt) / resolution_delta)
        num_points = len(points)
        curr_dt = start_dt
        next_dt = curr_dt + resolution_delta
        for i, point in enumerate(points):
            
            curr_pos = int(point.xpath(".//doc:position", namespaces=NSMAP)[0].text.strip())
            quantity = float(point.xpath(".//doc:quantity", namespaces=NSMAP[0].text.strip()))
            if quantity == None:
                quantity = 0

            # If you're at the final position, interpolate all remaining Points until the end of the Period's interval
            # Otherwise, interpolate up to the next Point
            if i + 1 == num_points:
                next_pos = num_positions
            else:
                next_pos = int(point.xpath(".//doc:position", namespaces=NSMAP)[0].text.strip())
            
            for _ in range(curr_pos, next_pos):
                interval = {
                    "start_time": curr_dt,
                    "end_time": next_dt,
                    "quantity": quantity,
                }
                intervals.append(interval)
                curr_dt = next_dt
                next_dt += resolution_delta
    else:
        return None

    # Ensure the points end at the correct time
    if curr_dt != end_dt:
        print(f"Incorrect final time:\nEnd DateTime: {end_dt}. Final recorded DateTime: {curr_dt}.\n")

    return intervals

def parse_timeseries_xml(timeseries_xml):

    # Attributes
    business_type = timeseries_xml.xpath(".//doc:businessType", namespaces=NSMAP)[0].text.strip()
    curve_type = timeseries_xml.xpath(".//doc:curveType", namespaces=NSMAP)[0].text.strip()
    psr_type = timeseries_xml.xpath(".//doc:psrType", namespaces=NSMAP)[0].text.strip()

    # Convert the data from the Timeseries' period to intervals
    period = timeseries_xml.xpath(".//doc:Period", namespaces=NSMAP)[0]
    intervals = parse_period_xml(period, curve_type=curve_type)

    return {
        "business_type": business_type,
        "psr_type": psr_type,
        "intervals": intervals
    }


def parse_gen_load_doc_xml(doc_xml):

    # Parse each timeseries in the doc to get the generation for each energy (psr) type
    generation_timeseries = []
    timeseries = doc_xml.xpath(".//doc:TimeSeries", namespaces=NSMAP)
    
    # If no Timeseries was found, it's most likely due to an invald period start/end in the API request
    if not timeseries:
        return None
    
    for ts in timeseries:

        # Since we're only looking for energy generation, we only need to parse TimeSeries from inBiddingZones
        if not ts.xpath(".//doc:inBiddingZone_Domain.mRID", namespaces=NSMAP):
            continue

        generation_timeseries.append(parse_timeseries_xml(ts))

    return generation_timeseries


def get_latest_15min_interval():
    now_utc = datetime.now(timezone.utc)
    minutes_to_subtract = now_utc.minute % 15

    end_of_interval = now_utc - relativedelta(
        minutes=minutes_to_subtract,
        seconds=now_utc.second,
        microseconds=now_utc.microsecond
    )
    start_of_interval = end_of_interval - relativedelta(minutes=15)

    return start_of_interval, end_of_interval


# Converts a datetime object into a String that's suitable for the ENTSOE API (format: yyyyMMddHHmm)
def datetime_to_period_str(dt: datetime):
    return dt.strftime("%Y%m%d%H%M")


def build_url(eic_code, start_time, end_time):
    start_time_str = datetime_to_period_str(start_time)
    end_time_str = datetime_to_period_str(end_time)
    return f"{API_URL}?documentType={DOCUMENT_TYPE}&processType={PROCESS_TYPE}&in_Domain={eic_code}&periodStart={start_time_str}&periodEnd={end_time_str}&securityToken={ENTSOE_API_KEY}"


def calculate_time_window(now_utc, current_duration_minutes):
    """
        Determines the time window to be used in the API request based on the given duration.
        Acceptable durations are 15, 30, and 60.
    """    
    minutes_to_subtract = now_utc.minute % current_duration_minutes

    end_of_interval = now_utc - relativedelta(
        minutes=minutes_to_subtract,
        seconds=now_utc.second,
        microseconds=now_utc.microsecond
    )
    start_of_interval = end_of_interval - relativedelta(minutes=current_duration_minutes)
    
    return start_of_interval, end_of_interval


# Runs a single ingestion cycle for all specified EIC codes
def run_ingestion_cycle(producer, eic_configs):
    logging.info("--- Starting new ingestion cycle ---")

    successful_domains = 0
    for eic_code, config in eic_configs.items():
        try:

            # Build and send the ENTSOE API request
            now_utc = datetime.now(timezone.utc)

            # For a dry run, we use data from yesterday
            if "--dry-run" in sys.argv:
                now_utc = now_utc - relativedelta(days=1)

            current_duration = config['query_duration_minutes']
            start_time, end_time = calculate_time_window(now_utc, current_duration)
            url = build_url(eic_code, start_time, end_time)
            
            response = requests.get(url, timeout=30)
            
            # Adjust the request time interval if it wasn't valid
            if "Delivered time interval is not valid" in response.text:
                logging.warning(f"Adapting duration for {eic_code} from {current_duration}mins.")
                if current_duration == 15:
                    eic_configs[eic_code]['query_duration_minutes'] = 30
                elif current_duration == 30:
                    eic_configs[eic_code]['query_duration_minutes'] = 60
                continue
            
            response.raise_for_status()


            # Parse the energy generation data to get each generation interval
            generation_load_doc = etree.XML(response.content)
            generation_per_type = parse_gen_load_doc_xml(generation_load_doc)

            if not generation_per_type:
                logging.warning(f"No valid generation data found for EIC {eic_code}")
                continue

            # Add the data to Kafka
            for record in generation_per_type:
                record['eic_code'] = eic_code
                producer.send(GENERATION_DATA_TOPIC, record)
            
            successful_domains += 1

        except requests.RequestException as e:
            logging.error(f"API request failed for EIC {eic_code}. Error: {e}")
        except etree.XMLSyntaxError as e:
            logging.error(f"XML parsing failed for EIC {eic_code}. Error: {e}")
        except Exception as e:
            logging.error(f"Unexpected error for EIC {eic_code}. Error: {e}")
        finally:
            time.sleep(1) # Wait 3 secs after each request

    if successful_domains > 0:
        producer.flush()
        logging.info(f"Cycle complete. Sent {successful_domains}/{len(eic_configs.keys())} domains to Kafka.")
    else:
        logging.info("Cycle complete. No records were sent.")


class MockKafkaProducer:
    """A mock producer that prints messages to the console instead of sending to Kafka."""
    def send(self, topic, value):
        return

    def flush(self):
        return


def main():
    # Default time intervals for each EIC code
    # We start with 15 minute intervals, and increment duration every time that a request fails
    EIC_CONFIGS = {
        code: {
            'query_duration_minutes': 60,
        }
        for code in EIC_CODES_GENERATION
    }

    # For testing, we use a Mock KafkaProducer
    if "--dry-run" in sys.argv:
        logging.info("--- RUNNING DRY-RUN ---")
        producer = MockKafkaProducer()
    else:
        logging.info("--- RUNNING LIVE ---")
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )

    runs = 0
    while True:
        run_ingestion_cycle(producer, EIC_CONFIGS)

        # The dry-run only lasts for one cycle 
        if "--dry-run" in sys.argv:
            logging.info("--- DRY RUN COMPLETE ---")
            break

        runs += 1
        if runs == 2:
            break

        time.sleep(900)


if __name__ == "__main__":
    main()
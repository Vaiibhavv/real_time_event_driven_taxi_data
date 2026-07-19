import json
import csv
import boto3
import logging
import time
import random
from datetime import datetime, timedelta

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---- Tunable "realism" parameters ----
CANCELLATION_RATE = 0.05          # 5% of trips never send an end event (cancelled)
MIN_TRIP_GAP_SECONDS = 2          # fastest a start->end gap can be
MAX_TRIP_GAP_SECONDS = 20         # slowest a start->end gap can be (simulates long trips / delays)
MIN_ARRIVAL_INTERVAL = 0.2        # fastest gap between two consecutive "new trip" events
MAX_ARRIVAL_INTERVAL = 2.5        # slowest gap between two consecutive "new trip" events
DELAYED_END_PROBABILITY = 0.08    # 8% of end events arrive noticeably later (simulates a stuck/delayed record)
DELAYED_END_EXTRA_SECONDS = (30, 120)


def send_record_to_kinesis(kinesis_client, record, stream_name):
    """Send a single record to the Kinesis stream."""
    try:
        kinesis_client.put_record(
            StreamName=stream_name,
            Data=json.dumps(record),
            PartitionKey=record['trip_id']
        )
        logger.info(f"Sent trip_id={record['trip_id']} to {stream_name}")
        return True
    except Exception as e:
        logger.error(f"Error sending trip_id={record.get('trip_id')} to {stream_name}: {str(e)}")
        return False


def read_csv_file(file_path):
    """Read CSV file and return rows."""
    try:
        with open(file_path, mode='r', encoding='utf-8') as file_content:
            csv_reader = csv.DictReader(file_content)
            return [row for row in csv_reader]
    except Exception as e:
        logger.error(f"Error reading {file_path}: {str(e)}")
        return []


def now_iso(offset_seconds: float = 0):
    """Current timestamp (+ optional offset) formatted like the original data: 'YYYY-MM-DD HH:MM:SS'."""
    ts = datetime.now() + timedelta(seconds=offset_seconds)
    return ts.strftime('%Y-%m-%d %H:%M:%S')


def build_start_record(row):
    """Take a CSV row and stamp it with a realistic current pickup time."""
    record = dict(row)
    # Preserve the original trip duration (estimated) so the dropoff offset still makes sense
    try:
        pickup_dt = datetime.strptime(row['pickup_datetime'], '%Y-%m-%d %H:%M:%S')
        est_dropoff_dt = datetime.strptime(row['estimated_dropoff_datetime'], '%Y-%m-%d %H:%M:%S')
        duration_seconds = max((est_dropoff_dt - pickup_dt).total_seconds(), 60)
    except Exception:
        duration_seconds = 600  # fallback: 10 min

    record['pickup_datetime'] = now_iso()
    record['estimated_dropoff_datetime'] = now_iso(offset_seconds=duration_seconds)

    # Slightly jitter the estimated fare so repeated runs of the same CSV don't look identical
    try:
        base_fare = float(row['estimated_fare_amount'])
        record['estimated_fare_amount'] = round(base_fare * random.uniform(0.95, 1.05), 2)
    except (ValueError, KeyError):
        pass

    return record, duration_seconds


def build_end_record(row, delayed=False):
    """Take a CSV row and stamp it with a realistic current dropoff time."""
    record = dict(row)
    record['dropoff_datetime'] = now_iso(offset_seconds=random.randint(*DELAYED_END_EXTRA_SECONDS) if delayed else 0)

    try:
        base_fare = float(row['fare_amount'])
        record['fare_amount'] = round(base_fare * random.uniform(0.95, 1.05), 2)
    except (ValueError, KeyError):
        pass

    return record


def main():
    region_name = 'us-east-1'
    trip_start_file = 'data/trip_start.csv'
    trip_end_file = 'data/trip_end.csv'
    start_stream_name = 'trip_start_stream'
    end_stream_name = 'trip_end_stream'

    kinesis_client = boto3.client('kinesis', region_name=region_name)

    start_trips = read_csv_file(trip_start_file)
    end_trips = read_csv_file(trip_end_file)
    end_trips_by_id = {row['trip_id']: row for row in end_trips}

    # Shuffle so replay order doesn't mirror the CSV's original order
    random.shuffle(start_trips)

    logger.info(f"Loaded {len(start_trips)} start trips. Beginning simulated real-time stream...")

    for row in start_trips:
        trip_id = row['trip_id']

        # Simulate a live "new trip requested" event
        start_record, duration_seconds = build_start_record(row)
        send_record_to_kinesis(kinesis_client, start_record, start_stream_name)

        # Simulate some trips getting cancelled and never producing an end event
        if random.random() < CANCELLATION_RATE:
            logger.info(f"Trip {trip_id} simulated as CANCELLED - no end event will be sent.")
        elif trip_id in end_trips_by_id:
            # Wait an amount of time proportional to (a fraction of) the trip's duration,
            # capped so the demo doesn't take forever
            gap = random.uniform(MIN_TRIP_GAP_SECONDS, MAX_TRIP_GAP_SECONDS)
            time.sleep(gap)

            delayed = random.random() < DELAYED_END_PROBABILITY
            end_record = build_end_record(end_trips_by_id[trip_id], delayed=delayed)
            send_record_to_kinesis(kinesis_client, end_record, end_stream_name)
            if delayed:
                logger.info(f"Trip {trip_id} end event simulated as DELAYED.")
        else:
            logger.warning(f"No matching end record found for trip_id={trip_id}; skipping end event.")

        # Randomized gap before the next "new trip" arrives, instead of a fixed interval
        time.sleep(random.uniform(MIN_ARRIVAL_INTERVAL, MAX_ARRIVAL_INTERVAL))

    logger.info("Finished simulated real-time trip stream.")


if __name__ == "__main__":
    main()
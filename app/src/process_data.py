import duckdb
import time
from datetime import datetime, timedelta
import os
import logging

# Local imports
from config import process_all

input_path = os.path.join("../data", "input")
output_path = os.path.join("../data", "output")

logger = logging.getLogger(__name__)


def read_input_data(conn: duckdb.DuckDBPyConnection):
    logger.info("Reading input csv files...")

    try:
        input_files = ["deposit.csv", "withdrawl.csv", "event.csv"]
        for file in input_files:
            if os.path.exists(f"{input_path}/{file}"):
                logger.info(f"Reading {file} from {input_path}/{file}")
                table_name = file.replace(".csv", "")
                conn.sql(
                    f"CREATE TABLE {table_name} AS SELECT * FROM read_csv('{input_path}/{file}');"
                )
            else:
                logger.error(
                    f"Couldn't find {file} in {input_path}/. Please check if the file exists."
                )
                raise FileNotFoundError
    except Exception as e:
        raise e


def read_output_data(conn: duckdb.DuckDBPyConnection):
    logger.info("Reading output table csv files...")

    input_files = ["transaction.csv", "user_login.csv"]
    try:
        for file in input_files:
            if os.path.exists(f"{output_path}/{file}"):
                logger.info(f"Reading {file} from {output_path}/{file}")
                table_name = file.replace(".csv", "")
                conn.sql(
                    f"CREATE TABLE {table_name} AS SELECT * FROM read_csv('{output_path}/{file}');"
                )

            else:
                logger.error(
                    f"Couldn't find {file} in {output_path}/. Please check if the file exists."
                )
                raise FileNotFoundError

    except Exception as e:
        raise e


def create_tables(conn: duckdb.DuckDBPyConnection):
    # Create transaction table
    conn.execute(
        "CREATE TABLE transaction (user_id VARCHAR, transaction_id BIGINT, transaction_type VARCHAR, amount DOUBLE, currency VARCHAR, event_timestamp TIMESTAMP);"
    )

    # Create user_login table
    conn.execute(
        "CREATE TABLE user_login (user_id VARCHAR, login_id BIGINT, event_timestamp TIMESTAMP);"
    )


def process_data():
    process_start_time = time.time()

    logger.info("Starting ETL...")
    logger.info(f"Process all: {process_all}")
    # Start duckdb connection
    conn = duckdb.connect(":memory:")

    # Read csv files from input_data folder
    logger.info("Reading csv files...")
    read_input_data(conn)

    # Calculate start dates based on the data in the tables
    if process_all:
        logger.info("Calculating batches counts...")

        deposit_start_date = (
            conn.sql("SELECT MIN(event_timestamp) FROM deposit").fetchone()[0].date()
        )
        withdrawl_start_date = (
            conn.sql("SELECT MIN(event_timestamp) FROM withdrawl").fetchone()[0].date()
        )
        event_start_date = (
            conn.sql("SELECT MIN(event_timestamp) FROM event").fetchone()[0].date()
        )

        # Create empty transaction and user_login tables
        create_tables(conn)
    else:
        # Read transaction and user_login tables from previous runs *ONLY NEEDED BECAUSE WE'RE NOT RUNNING THIS IN PUSHDOWN MODE*
        read_output_data(conn)

        # If we are not processing all data, we will use the max date as the start date.
        # This is useful when we want to process only the data that has been added since the last run.

        # Get max date from transaction(for deposit and withdrawl separated) and user_login tables
        deposit_start_date = (
            conn.sql(
                "SELECT MAX(event_timestamp) FROM transaction WHERE transaction_type = 'deposit';"
            )
            .fetchone()[0]
            .date()
        )

        withdrawl_start_date = (
            conn.sql(
                "SELECT MAX(event_timestamp) FROM transaction WHERE transaction_type = 'withdrawl';"
            )
            .fetchone()[0]
            .date()
        )

        event_start_date = (
            conn.sql("SELECT MAX(event_timestamp) FROM user_login").fetchone()[0].date()
        )

    # Get count of days between the start date and today
    deposit_days = (datetime.now().date() - deposit_start_date).days
    withdrawl_days = (datetime.now().date() - withdrawl_start_date).days
    event_days = (datetime.now().date() - event_start_date).days

    # Deposit table
    if deposit_days == 0:
        logger.info("No new deposit data to process.")
    else:
        logger.info(f"Processing deposit data for {deposit_days} days...")
        for i in range(deposit_days):
            date_threshold = deposit_start_date + timedelta(days=i)

            logger.debug(f"[Deposit] Processing chunk {i+1} / {deposit_days}")

            # Insert data into transaction table for each day
            conn.sql(
                f"INSERT INTO transaction SELECT DISTINCT user_id, id, 'deposit', amount, currency, event_timestamp FROM deposit WHERE tx_status='complete' AND event_timestamp::date = '{date_threshold}'::date;"
            )

    # Withdrawl table
    if withdrawl_days == 0:
        logger.info("No new withdrawl data to process.")
    else:
        logger.info(f"Processing withdrawl data for {withdrawl_days} days...")
        for i in range(withdrawl_days):
            date_threshold = withdrawl_start_date + timedelta(days=i)

            logger.debug(f"[Withdrawl] Processing chunk {i+1} / {withdrawl_days}")

            # Insert data into transaction table for each day
            conn.sql(
                f"INSERT INTO transaction SELECT DISTINCT user_id, id, 'withdrawl', amount, currency, event_timestamp FROM withdrawl WHERE tx_status='complete' AND event_timestamp::date = '{date_threshold}'::date;"
            )

    # Event table
    if event_days == 0:
        logger.info("No new event data to process.")
    else:
        logger.info(f"Processing event data for {event_days} days...")
        for i in range(event_days):
            date_threshold = event_start_date + timedelta(days=i)

            logger.debug(f"[Event] Processing chunk {i+1} / {event_days}")

            # Insert data into user_login table for each day
            conn.sql(
                f"INSERT INTO user_login SELECT DISTINCT user_id, id, event_timestamp FROM event WHERE event_timestamp::date = '{date_threshold}'::date AND event_name = 'login';"
            )

    # Write transaction and user_login tables to csv files *ONLY NEEDED BECAUSE WE'RE NOT RUNNING THIS IN PUSHDOWN MODE*
    logger.info("Writing transaction and user_login tables to csv files...")
    conn.sql(f"COPY transaction TO '{output_path}/transaction.csv';")
    conn.sql(f"COPY user_login TO '{output_path}/user_login.csv';")

    # Close duckdb connection
    conn.close()

    # Calculate process duration
    process_end_time = time.time()
    process_duration = process_end_time - process_start_time

    logger.info(
        f"ETL process completed in {process_duration/60:.0f} minutes and {process_duration%60:.0f} seconds."
    )

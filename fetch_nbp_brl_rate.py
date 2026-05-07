#!/usr/bin/env python3
"""
Fetch current BRL/PLN exchange rate from the public NBP API and store it in olist_dwh.dbo.dim_exchange_rate.
Run after the DWH database exists.
"""

import os
import sys
from datetime import datetime

import pymssql
import requests

NBP_URL = "https://api.nbp.pl/api/exchangerates/rates/a/brl/?format=json"


def parse_server(server: str):
    if "," in server:
        host, port = server.split(",", 1)
        return host, int(port)
    if ":" in server:
        host, port = server.split(":", 1)
        return host, int(port)
    return server, 1433


def get_connection():
    server = os.getenv("DWH_SQL_SERVER", "localhost,1433")
    user = os.getenv("DWH_SQL_USER", "sa")
    password = os.getenv("DWH_SQL_PASSWORD", "Test123!")
    database = os.getenv("DWH_SQL_DATABASE", "olist_dwh")
    host, port = parse_server(server)
    return pymssql.connect(
        server=f"{host}:{port}",
        user=user,
        password=password,
        database=database,
        as_dict=True,
    )


def fetch_rate():
    response = requests.get(NBP_URL, headers={"Accept": "application/json"}, timeout=30)
    response.raise_for_status()
    payload = response.json()
    rate = payload["rates"][0]
    return {
        "source_system": "NBP",
        "currency_code": payload["code"].upper(),
        "base_currency": "PLN",
        "rate_date": rate["effectiveDate"],
        "rate_value": float(rate["mid"]),
    }


def ensure_table(cursor):
    cursor.execute(
        """
        IF OBJECT_ID('dbo.dim_exchange_rate', 'U') IS NULL
        BEGIN
            CREATE TABLE dbo.dim_exchange_rate (
                exchange_rate_key INT IDENTITY(1,1) PRIMARY KEY,
                source_system VARCHAR(50) NOT NULL,
                currency_code CHAR(3) NOT NULL,
                base_currency CHAR(3) NOT NULL,
                rate_date DATE NOT NULL,
                rate_value DECIMAL(18,6) NOT NULL,
                fetched_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
                is_current BIT NOT NULL DEFAULT 1,
                CONSTRAINT uq_exchange_rate UNIQUE (source_system, currency_code, base_currency, rate_date)
            );
        END;
        """
    )


def upsert_rate(cursor, rate):
    cursor.execute(
        """
        UPDATE dbo.dim_exchange_rate
        SET is_current = 0
        WHERE source_system = %s
          AND currency_code = %s
          AND base_currency = %s;
        """,
        (rate["source_system"], rate["currency_code"], rate["base_currency"]),
    )

    cursor.execute(
        """
        MERGE dbo.dim_exchange_rate AS target
        USING (
            SELECT
                %s AS source_system,
                %s AS currency_code,
                %s AS base_currency,
                CAST(%s AS DATE) AS rate_date,
                CAST(%s AS DECIMAL(18,6)) AS rate_value
        ) AS source
        ON target.source_system = source.source_system
           AND target.currency_code = source.currency_code
           AND target.base_currency = source.base_currency
           AND target.rate_date = source.rate_date
        WHEN MATCHED THEN
            UPDATE SET
                rate_value = source.rate_value,
                fetched_at = SYSUTCDATETIME(),
                is_current = 1
        WHEN NOT MATCHED THEN
            INSERT (source_system, currency_code, base_currency, rate_date, rate_value, fetched_at, is_current)
            VALUES (source.source_system, source.currency_code, source.base_currency, source.rate_date, source.rate_value, SYSUTCDATETIME(), 1);
        """,
        (
            rate["source_system"],
            rate["currency_code"],
            rate["base_currency"],
            rate["rate_date"],
            rate["rate_value"],
        ),
    )


def main():
    print("Fetching current BRL/PLN rate from NBP API...")
    rate = fetch_rate()
    print(f"Fetched: 1 {rate['currency_code']} = {rate['rate_value']} {rate['base_currency']} on {rate['rate_date']}")

    with get_connection() as conn:
        cursor = conn.cursor()
        ensure_table(cursor)
        upsert_rate(cursor, rate)
        conn.commit()

    print("Rate saved successfully to olist_dwh.dbo.dim_exchange_rate")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)

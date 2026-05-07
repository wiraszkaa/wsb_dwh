# DWH plus additions

Pliki dodatkowe do elementów punktowanych na plus:

- `6_etl_audit_schema.sql` — logowanie przebiegu ETL i wyników walidacji.
- `7_scd_type2_product.sql` — demonstracja SCD Type 2 dla wymiaru produktu.
- `fetch_nbp_brl_rate.py` — pobranie aktualnego kursu BRL/PLN z publicznego API NBP.
- `8_external_nbp_rates.sql` — tabela kursów i widok zamówień przeliczonych na PLN.
- `9_advanced_reports.sql` — zaawansowane raporty SQL.

Kolejność uruchomienia:

1. `python main_staging.py --force`
2. `python main_dwh.py --etl --force`
3. Uruchom w VS Code: `6_etl_audit_schema.sql`
4. Uruchom w VS Code: `7_scd_type2_product.sql`
5. `pip install requests`
6. `python fetch_nbp_brl_rate.py`
7. Uruchom w VS Code: `8_external_nbp_rates.sql`
8. Uruchom w VS Code: `9_advanced_reports.sql`

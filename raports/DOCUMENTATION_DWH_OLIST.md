# Projekt hurtowni danych Olist - dokumentacja projektu zaliczeniowego

**Przedmiot:** Hurtownie danych - laboratoria  
**Zbiór danych:** Olist Brazilian E-Commerce Public Dataset  
**Bazy danych:** `olist` - staging, `olist_dwh` - hurtownia danych  
**Technologie:** Python, Microsoft SQL Server 2022, Docker, SQL, Power BI  
**Autorzy:** Marcel Żukowski Jakub Wiraszka

---

## 1. Cel projektu

Celem projektu było przygotowanie kompletnego przykładu hurtowni danych dla danych e-commerce. Projekt pokazuje przepływ od danych źródłowych w plikach CSV, przez staging, ETL, model wymiarowy, walidacje oraz raportowanie SQL i Power BI.

Cel biznesowy rozwiązania to analiza sprzedaży, zamówień, dostaw, ocen klientów, kategorii produktów i sprzedawców na podstawie danych sklepu internetowego.

## 2. Status wymagań

| Wymaganie | Status | Gdzie zrealizowano |
|---|---|---|
| Model hurtowni danych | Wykonane | `2_dwh_schema.sql`, `olist_dwh` |
| Minimum 1 tabela faktów | Wykonane | `fact_orders`, `fact_order_items`, `fact_reviews`, `fact_payments` |
| Minimum 3 wymiary, w tym wymiar dat | Wykonane | `dim_date`, `dim_customer`, `dim_product`, `dim_seller` itd. |
| Proces ETL | Wykonane | `main_staging.py`, `main_dwh.py`, `scripts/` |
| Agregacje i transformacje | Wykonane | `agg_monthly_sales`, transformacje w `main_dwh.py` |
| Podstawowa delta | Wykonane | `5_delta_demo.sql` |
| 3 raporty | Wykonane | `3_reports.sql`, Power BI |
| Walidacje/logowanie | Wykonane jako plus | `4_validation_queries.sql`, `6_etl_audit_schema.sql` |
| SCD Type 2 | Wykonane demonstracyjnie | `7_scd_type2_product.sql` |
| Dane live/API | Wykonane | `fetch_nbp_brl_rate.py`, `8_external_nbp_rates.sql` |
| Power BI | Wykonane | dashboardy: Sprzedaż, Dostawy, Sprzedawcy/Kategorie |
| SSAS/SSIS | Nie wykonano | pominięte świadomie |

## 3. Struktura projektu

```text
project/
  data/
    DATA.md
    *.csv
  scripts/
    database.py
    loader.py
    schema.py
  1_staging_tables.sql
  2_dwh_schema.sql
  3_reports.sql
  4_validation_queries.sql
  5_delta_demo.sql
  6_etl_audit_schema.sql
  7_scd_type2_product.sql
  8_external_nbp_rates.sql
  9_advanced_reports.sql
  fetch_nbp_brl_rate.py
  docker-compose.yml
  main_staging.py
  main_dwh.py
  requirements.txt
  reports/
  screenshots/
```

## 4. Uruchomienie projektu

```powershell
docker compose up -d
docker ps
python -m venv .venv
.\\.venv\\Scripts\\activate
pip install -r requirements.txt
pip install requests
python main_staging.py --force
python main_dwh.py --etl --force
python fetch_nbp_brl_rate.py
```

![Docker](screenshots/01_docker_ps.png)

Pełne ładowanie danych może trwać dłużej. Jako dowód wykonania ETL przedstawiono wyniki walidacji po zakończonym ładowaniu.

## 5. Model hurtowni danych

Model jest oparty na strukturze gwiazdy: wymiary opisują dane, a fakty przechowują miary biznesowe. Dodatkowo utworzono agregację miesięczną `agg_monthly_sales`.

| Typ | Tabela | Rola |
|---|---|---|
| Wymiar | `dim_date` | kalendarz i hierarchia czasu |
| Wymiar | `dim_customer` | klienci i regiony |
| Wymiar | `dim_product` | produkty i kategorie |
| Wymiar | `dim_seller` | sprzedawcy |
| Fakt | `fact_order_items` | pozycje zamówień |
| Fakt | `fact_orders` | zamówienia |
| Fakt | `fact_reviews` | oceny |
| Fakt | `fact_payments` | płatności |
| Agregacja | `agg_monthly_sales` | sprzedaż miesięczna |

## 6. Walidacja danych

![Liczba rekordów](screenshots/04_validation_row_counts.png)

Po ETL liczby rekordów wynoszą m.in. `fact_orders = 99441`, `fact_order_items = 112650`, `fact_reviews = 97685`, `fact_payments = 103886`, `agg_monthly_sales = 24902`.

![Brakujące klucze](screenshots/05_validation_status_keys.png)

Brakujące klucze wymiarów w `fact_order_items` wynoszą 0.

![Spójność przychodu](screenshots/06_validation_revenue_consistency.png)

Przychód z faktów i agregacji wynosi 15 843 553.24, a różnica wynosi 0.00.

## 7. Raporty SQL

![Raport miesięcznej sprzedaży](screenshots/07_report_monthly_sales.png)

Raport pokazuje sprzedaż według roku, miesiąca i kategorii produktu.

![Opóźnienia według regionu](screenshots/08_report_delays_by_region.png)

Region Northeast ma najwyższy procent opóźnień: 13.80%.

![Ranking sprzedawców](screenshots/09_report_seller_ranking.png)

Ranking sprzedawców wykorzystuje CTE i funkcję `RANK()`.

## 8. Delta danych

```sql
WHERE NOT EXISTS (
    SELECT 1
    FROM fact_orders fo
    WHERE fo.order_id = o.order_id
);
```

![Delta](screenshots/10_delta_demo.png)

`0 rows affected` oznacza brak nowych rekordów po pełnym wcześniejszym ładowaniu. Agregacja została odświeżona - `24902 rows affected`.

## 9. Logowanie ETL

![Logowanie ETL](screenshots/11_etl_audit_log.png)

Tabele `etl_run_log` i `etl_validation_result` zapisują status walidacji po ETL.

## 10. SCD Type 2

![SCD2](screenshots/12_scd2_product.png)

Demonstracyjna tabela `dim_product_scd2` pokazuje wersję historyczną i aktualną produktu.

## 11. API NBP i PLN

![NBP PLN](screenshots/13_nbp_pln_view.png)

Widok `vw_fact_orders_pln` przelicza wartości zamówień z BRL na PLN przy użyciu kursu pobranego z API NBP.

## 12. Raporty zaawansowane

![Raport zaawansowany](screenshots/14_advanced_report.png)

Raporty wykorzystują m.in. `LAG()`, `RANK()` i `NTILE()`.

## 13. Power BI

![Power BI Sprzedaż](screenshots/Sprzedaz.png)

![Power BI Dostawy](screenshots/Dostawy.png)

![Power BI Sprzedawcy i kategorie](screenshots/Sprzedawcy_Kategorie.png)

## 14. Wnioski

Projekt spełnia główne wymagania zaliczeniowe: posiada model DWH, wymiary, fakty, wymiar dat, działający ETL, agregacje, transformacje, podstawową deltę, raporty SQL i dokumentację. Dodatkowo wykonano walidacje, logowanie, SCD2, integrację z API NBP, raporty zaawansowane oraz Power BI.

Analiza danych wykazała, że region Southeast generuje największy wolumen i przychód, a region Northeast ma najwyższy procent opóźnień. Opóźnienia silnie obniżają średnią ocenę klientów: zamówienia opóźnione mają średnią ocenę 2.57, a terminowe lub bez opóźnienia 4.24.

## 15. Zawartość paczki do oddania

Dołączyć: kod Python, skrypty SQL, `docker-compose.yml`, `requirements.txt`, dokumentację, screeny i opcjonalnie plik Power BI. Nie dołączać `.venv`, `__pycache__` ani dużych plików CSV, jeżeli prowadzący nie wymaga danych w paczce.

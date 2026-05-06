# Plan prezentacji projektu — 10 minut

## Slajd 1 — Tytuł
Projekt hurtowni danych dla danych e-commerce Olist.

## Slajd 2 — Cel biznesowy
- analiza sprzedaży,
- analiza opóźnień dostaw,
- analiza sprzedawców,
- analiza płatności,
- analiza ocen klientów.

## Slajd 3 — Źródła danych
- dataset Olist z Kaggle,
- 9 plików CSV,
- dane o klientach, zamówieniach, produktach, sprzedawcach, płatnościach, opiniach i geolokalizacji.

## Slajd 4 — Architektura
CSV -> staging `olist` -> ETL Python/SQL -> DWH `olist_dwh` -> raporty SQL.

## Slajd 5 — Model hurtowni
Pokazać tabele:
- wymiary: `dim_date`, `dim_customer`, `dim_product`, `dim_seller`, itd.,
- fakty: `fact_orders`, `fact_order_items`, `fact_payments`, `fact_reviews`,
- agregacja: `agg_monthly_sales`.

## Slajd 6 — ETL i transformacje
- region na podstawie stanu,
- tłumaczenie kategorii,
- objętość produktu,
- klasyfikacja rozmiaru,
- opóźnienia dostaw,
- pozytywne/negatywne opinie.

## Slajd 7 — Walidacje i delta
- liczba rekordów,
- brak duplikatów kluczy biznesowych,
- spójność przychodu fakt/agregacja,
- podstawowa delta przez `NOT EXISTS`.

## Slajd 8 — Raport 1 i 2
- sprzedaż miesięczna według kategorii,
- opóźnienia dostaw według regionu.

## Slajd 9 — Raport 3, 4 i 5
- ranking sprzedawców,
- metody płatności,
- kategorie produktów i oceny.

## Slajd 10 — Wnioski
- model DWH pozwala analizować sprzedaż i logistykę,
- staging oddziela dane źródłowe od hurtowni,
- raporty pokazują wartość biznesową,
- projekt można rozbudować o Power BI, SCD Type 2 lub Airflow.

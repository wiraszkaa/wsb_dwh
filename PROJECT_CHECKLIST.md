# Checklist projektu DWH przed oddaniem

## Obowiązkowe wymagania
- [ ] Projekt uruchamia SQL Server przez `docker compose up -d`.
- [ ] Pliki CSV są pobrane z Kaggle i znajdują się w folderze `data/`.
- [ ] `python main_staging.py --force` kończy się sukcesem.
- [ ] `python main_dwh.py --etl --force` kończy się sukcesem.
- [ ] W DWH istnieje co najmniej 1 tabela faktów.
- [ ] W DWH istnieją co najmniej 3 wymiary.
- [ ] W DWH istnieje wymiar daty `dim_date`.
- [ ] Tabele faktów mają dane.
- [ ] Projekt zawiera transformacje danych.
- [ ] Projekt zawiera agregację danych.
- [ ] Projekt zawiera podstawową obsługę delty danych.
- [ ] Projekt zawiera minimum 3 raporty SQL.
- [ ] Projekt zawiera dokumentację `.md` albo `.pdf`.
- [ ] Projekt ma instrukcję uruchomienia.
- [ ] Projekt ma wnioski.

## Elementy pod ocenę dodatkową
- [ ] Walidacje jakości danych.
- [ ] Logi z uruchomienia ETL.
- [ ] Tabela agregująca `agg_monthly_sales` jest zasilana.
- [ ] Raporty korzystają z różnych technik SQL: `GROUP BY`, `JOIN`, `CTE`, funkcje okna, agregacje.
- [ ] Jest opis podstawowej delty danych.
- [ ] Jest przygotowany pokaz testowego uruchomienia ETL.
- [ ] Są screeny z działania ETL i raportów.

## Przed spakowaniem ZIP
- [ ] Usuń foldery `__pycache__`.
- [ ] Usuń `.venv`, jeśli znajduje się w folderze projektu.
- [ ] Usuń duże pliki CSV, jeśli prowadzący nie wymaga ich wrzucania.
- [ ] Zostaw `data/DATA.md` z informacją, skąd pobrać dane.
- [ ] Dołącz `3_reports.sql`.
- [ ] Dołącz `4_validation_queries.sql`.
- [ ] Dołącz `5_delta_demo.sql`.
- [ ] Dołącz dokumentację.
- [ ] Dołącz screeny lub folder `screenshots/`, jeśli je przygotujesz.

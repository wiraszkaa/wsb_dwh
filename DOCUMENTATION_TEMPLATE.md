# Projekt hurtowni danych — Olist Brazilian E-Commerce

## 1. Cel projektu
Celem projektu jest przygotowanie procesu ETL oraz hurtowni danych dla danych e-commerce pochodzących z platformy Olist. Hurtownia umożliwia analizę sprzedaży, zamówień, płatności, ocen klientów, opóźnień dostaw oraz wyników sprzedawców i kategorii produktowych.

## 2. Cel biznesowy
Projekt odpowiada na pytania biznesowe:
- które kategorie produktów generują największy przychód,
- które regiony mają największy problem z opóźnieniami dostaw,
- którzy sprzedawcy osiągają najwyższe wyniki sprzedażowe,
- jakie metody płatności są najczęściej używane,
- jak oceny klientów wiążą się z kategoriami produktów i logistyką.

## 3. Źródła danych
W projekcie wykorzystano publiczny zbiór danych **Brazilian E-Commerce Public Dataset by Olist** dostępny na Kaggle.

Wymagane pliki CSV:
- `olist_customers_dataset.csv`
- `olist_geolocation_dataset.csv`
- `olist_orders_dataset.csv`
- `olist_order_items_dataset.csv`
- `olist_order_payments_dataset.csv`
- `olist_order_reviews_dataset.csv`
- `olist_products_dataset.csv`
- `olist_sellers_dataset.csv`
- `product_category_name_translation.csv`

Ze względu na rozmiar danych pliki CSV nie muszą być dołączane do paczki projektu. W dokumentacji wskazano źródło oraz wymagane nazwy plików.

## 4. Architektura rozwiązania
Projekt składa się z dwóch głównych warstw:

1. **Staging database: `olist`**
   - przechowuje dane źródłowe w formie zbliżonej do CSV,
   - tabele staging tworzone są przez plik `1_staging_tables.sql`,
   - dane ładowane są skryptem `main_staging.py`.

2. **Data warehouse database: `olist_dwh`**
   - przechowuje dane w modelu wymiarowym,
   - schemat tworzony jest przez plik `2_dwh_schema.sql`,
   - proces ETL uruchamiany jest skryptem `main_dwh.py --etl`.

Przepływ danych:

```text
CSV files -> staging database olist -> ETL Python/SQL -> data warehouse olist_dwh -> raporty SQL
```

## 5. Technologie
- Docker Desktop
- Microsoft SQL Server 2022 Express
- Python 3.x
- biblioteka `pymssql`
- biblioteka `tqdm`
- SQL Server Management Studio / Azure Data Studio / VS Code jako narzędzie do uruchamiania zapytań SQL

## 6. Model hurtowni danych
Model hurtowni ma charakter modelu gwiazdy. Tabele faktów korzystają ze współdzielonych wymiarów.

### Tabele wymiarów
- `dim_date` — wymiar daty, jedna linia na dzień,
- `dim_customer` — wymiar klienta,
- `dim_seller` — wymiar sprzedawcy,
- `dim_product` — wymiar produktu,
- `dim_payment_type` — wymiar typu płatności,
- `dim_order_status` — wymiar statusu zamówienia,
- `dim_geolocation` — wymiar geograficzny.

### Tabele faktów
- `fact_orders` — poziom zamówienia,
- `fact_order_items` — poziom pozycji zamówienia,
- `fact_payments` — poziom płatności,
- `fact_reviews` — poziom opinii klienta.

### Tabela agregująca
- `agg_monthly_sales` — miesięczna sprzedaż według kategorii i sprzedawcy.

## 7. Transformacje danych
W projekcie wykonano m.in. następujące transformacje:
- przypisanie regionu Brazylii na podstawie stanu klienta i sprzedawcy,
- tłumaczenie kategorii produktów z portugalskiego na angielski,
- wyliczenie objętości produktu na podstawie wymiarów,
- klasyfikacja produktów według rozmiaru,
- oznaczanie produktów ciężkich,
- wyliczenie liczby dni dostawy,
- oznaczenie zamówień opóźnionych,
- wyliczenie długości komentarza w opinii,
- oznaczenie pozytywnej opinii,
- agregacja sprzedaży miesięcznej.

## 8. Proces ETL
Proces ETL przebiega w następujących etapach:

1. Uruchomienie SQL Servera w Dockerze.
2. Utworzenie bazy staging `olist`.
3. Utworzenie tabel staging.
4. Załadowanie plików CSV do stagingu.
5. Utworzenie bazy hurtowni `olist_dwh`.
6. Utworzenie tabel wymiarów, faktów i tabeli agregującej.
7. Zasilenie wymiarów.
8. Zasilenie tabel faktów.
9. Odświeżenie tabeli agregującej.
10. Walidacja liczby rekordów i spójności danych.

## 9. Instrukcja uruchomienia

### 9.1. Uruchomienie SQL Servera
```powershell
docker compose up -d
```

Sprawdzenie działania kontenera:

```powershell
docker ps
```

### 9.2. Przygotowanie środowiska Python
```powershell
python -m venv .venv
.\.venv\Scripts\activate
pip install -r requirements.txt
```

### 9.3. Pobranie danych
Pobrać dane z Kaggle i umieścić wszystkie pliki CSV bezpośrednio w folderze `data/`.

### 9.4. Uruchomienie stagingu
```powershell
python main_staging.py --force
```

### 9.5. Uruchomienie hurtowni danych i ETL
```powershell
python main_dwh.py --etl --force
```

### 9.6. Uruchomienie raportów
Raporty znajdują się w pliku:

```text
3_reports.sql
```

Walidacje znajdują się w pliku:

```text
4_validation_queries.sql
```

## 10. Raporty
W projekcie przygotowano raporty:

1. Miesięczna sprzedaż według kategorii produktu.
2. Opóźnienia dostaw według regionu klienta.
3. Ranking sprzedawców według przychodu, liczby zamówień i średniej oceny.
4. Analiza metod płatności.
5. Kategorie produktów — przychód, fracht, ocena i udział ciężkich produktów.

## 11. Podstawowa obsługa delty danych
Projekt zawiera podstawową obsługę delty danych:
- podczas ładowania stagingu wykorzystywany jest plik `load_status.json`, który pozwala pominąć już poprawnie załadowane tabele,
- w pliku `5_delta_demo.sql` pokazano podejście do dopisywania tylko nowych rekordów do tabel faktów na podstawie warunku `NOT EXISTS`,
- nie jest konieczne pełne odtwarzanie hurtowni przy każdym dopisaniu nowych rekordów.

## 12. Walidacje i logowanie
Walidacje obejmują:
- liczby rekordów w tabelach DWH,
- sprawdzenie, czy tabele faktów nie są puste,
- sprawdzenie duplikatów kluczy biznesowych w wymiarach,
- sprawdzenie brakujących kluczy wymiarów,
- porównanie przychodu z tabeli faktów i tabeli agregującej.

Logi z uruchomienia można zapisać do folderu `logs/`, np.:

```powershell
mkdir logs
python main_staging.py --force 2>&1 | Tee-Object logs\staging_run.txt
python main_dwh.py --etl --force 2>&1 | Tee-Object logs\dwh_etl_run.txt
```

## 13. Wnioski
Projekt pokazuje pełny przepływ danych od plików CSV przez staging do modelu hurtowni danych. Zastosowany model wymiarowy ułatwia analizę danych sprzedażowych, logistycznych i opinii klientów. Największą wartością biznesową projektu jest możliwość obserwacji trendów sprzedaży, identyfikacji opóźnień dostaw oraz porównania wyników sprzedawców i kategorii produktów.

## 14. Możliwe dalsze rozszerzenia
- pełna implementacja SCD Type 2 dla wybranych wymiarów,
- dashboard w Power BI,
- harmonogramowanie ETL w Airflow,
- rozbudowana tabela logowania uruchomień ETL,
- automatyczne testy jakości danych,
- dane live lub dodatkowe źródło danych.

# Projekt hurtowni danych — Olist Brazilian E-Commerce

**Przedmiot:** Hurtownie danych — projekt zaliczeniowy  
**Zespół:** [UZUPEŁNIĆ: imiona i nazwiska członków zespołu]  
**Data:** [UZUPEŁNIĆ]  
**Repozytorium / folder projektu:** [UZUPEŁNIĆ, jeżeli wymagane]

---

## 1. Cel projektu

Celem projektu było przygotowanie kompletnego procesu przetwarzania danych e-commerce: od danych źródłowych w plikach CSV, przez warstwę staging, do hurtowni danych w modelu wymiarowym. Projekt obejmuje również przygotowanie raportów analitycznych, walidacji jakości danych oraz elementów dodatkowych takich jak podstawowa delta danych, logowanie walidacji, demonstracja SCD Type 2 oraz integracja z zewnętrznym źródłem danych.

Projekt realizuje trzy główne obszary wymagane w ramach laboratoriów:

- zapoznanie z architekturą hurtowni danych,
- utworzenie procesu ETL,
- raportowanie przy użyciu SQL oraz dodatkowych narzędzi analitycznych.

---

## 2. Cel biznesowy

Analizowany zbiór danych dotyczy brazylijskiej platformy e-commerce Olist. Hurtownia danych została przygotowana w taki sposób, aby umożliwić analizę sprzedaży, zamówień, płatności, dostaw, opinii klientów oraz wyników sprzedawców i kategorii produktowych.

Najważniejsze pytania biznesowe, na które odpowiada projekt:

- które kategorie produktów generują największy przychód,
- jak zmieniała się sprzedaż w czasie,
- które regiony mają największe problemy z opóźnieniami dostaw,
- którzy sprzedawcy generują najwyższą sprzedaż,
- jakie metody płatności są najczęściej używane,
- czy opóźnienia dostaw wpływają na oceny klientów,
- jaka jest wartość zamówień po przeliczeniu z BRL na PLN na podstawie kursu z API NBP.

---

## 3. Źródła danych

W projekcie wykorzystano publiczny zbiór danych **Brazilian E-Commerce Public Dataset by Olist**, dostępny na platformie Kaggle:

```text
https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce
```

Do uruchomienia projektu wymagane są następujące pliki CSV umieszczone bezpośrednio w folderze `data/`:

```text
olist_customers_dataset.csv
olist_geolocation_dataset.csv
olist_orders_dataset.csv
olist_order_items_dataset.csv
olist_order_payments_dataset.csv
olist_order_reviews_dataset.csv
olist_products_dataset.csv
olist_sellers_dataset.csv
product_category_name_translation.csv
```

W paczce projektu znajduje się plik `data/DATA.md`, który opisuje źródło danych i wymagane nazwy plików. Ze względu na rozmiar danych pliki CSV mogą nie być dołączane do końcowego ZIP-a, jeżeli prowadzący dopuszcza opis i wskazanie źródła danych zamiast dołączania dużych zbiorów.

**Miejsce na screen:** struktura folderu `data/` z widocznymi plikami CSV.

```text
[SCREEN_01_DATA_FOLDER]
```

---

## 4. Technologie i narzędzia

W projekcie wykorzystano:

- **Docker Desktop** — uruchomienie lokalnej instancji SQL Servera,
- **Microsoft SQL Server 2022 Express** — baza staging oraz baza hurtowni danych,
- **Python 3.x** — skrypty ETL,
- **pymssql** — połączenie Pythona z SQL Serverem,
- **tqdm** — pasek postępu podczas ładowania danych CSV,
- **requests** — pobieranie kursu waluty z publicznego API NBP,
- **Visual Studio Code** — uruchamianie skryptów Python i SQL,
- **SQL Server extension / mssql w VS Code** — wykonywanie zapytań SQL,
- **Power BI Desktop** — opcjonalna wizualizacja raportowa, jeżeli zostanie dołączona do projektu.

Plik `requirements.txt` zawiera podstawowe zależności projektu:

```text
pymssql>=2.2.0
tqdm>=4.0.0
```

Dodatkowo dla integracji z API NBP instalowana jest biblioteka:

```powershell
pip install requests
```

---

## 5. Architektura rozwiązania

Projekt został podzielony na dwie główne warstwy bazodanowe:

### 5.1. Warstwa staging — baza `olist`

Warstwa staging przechowuje dane w formie zbliżonej do plików źródłowych CSV. Jej zadaniem jest oddzielenie surowych danych od docelowego modelu hurtowni danych.

Za utworzenie tabel staging odpowiada plik:

```text
1_staging_tables.sql
```

Za załadowanie danych odpowiada skrypt:

```text
main_staging.py
```

### 5.2. Warstwa hurtowni danych — baza `olist_dwh`

Warstwa DWH przechowuje dane w modelu wymiarowym. Zawiera tabele wymiarów, tabele faktów oraz tabelę agregującą.

Za utworzenie schematu DWH odpowiada plik:

```text
2_dwh_schema.sql
```

Za zasilenie hurtowni odpowiada skrypt:

```text
main_dwh.py
```

### 5.3. Przepływ danych

```text
Pliki CSV
   ↓
Python ETL — main_staging.py
   ↓
Baza staging — olist
   ↓
Python/SQL ETL — main_dwh.py
   ↓
Baza hurtowni danych — olist_dwh
   ↓
Raporty SQL / walidacje / Power BI
```

**Miejsce na screen:** uruchomiony kontener Docker `olist-mssql`.

```text
[SCREEN_02_DOCKER_PS]
```

---

## 6. Struktura projektu

Najważniejsze pliki i foldery projektu:

```text
data/
  DATA.md
  [pliki CSV pobrane z Kaggle]

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
main_staging.py
main_dwh.py
docker-compose.yml
requirements.txt
```

Opis najważniejszych elementów:

| Element | Rola w projekcie |
|---|---|
| `docker-compose.yml` | Uruchamia SQL Server 2022 Express w kontenerze Docker. |
| `main_staging.py` | Tworzy bazę staging `olist`, tabele staging i ładuje dane CSV. |
| `main_dwh.py` | Tworzy bazę `olist_dwh`, tabele wymiarów, faktów i uruchamia ETL. |
| `1_staging_tables.sql` | Definicje surowych tabel staging. |
| `2_dwh_schema.sql` | Definicje wymiarów, faktów i tabeli agregującej. |
| `3_reports.sql` | Podstawowe raporty SQL wymagane w projekcie. |
| `4_validation_queries.sql` | Zapytania walidujące poprawność danych w DWH. |
| `5_delta_demo.sql` | Demonstracja podstawowej obsługi delty danych. |
| `6_etl_audit_schema.sql` | Tabele audytowe i zapis wyników walidacji ETL. |
| `7_scd_type2_product.sql` | Demonstracja SCD Type 2 dla wymiaru produktu. |
| `8_external_nbp_rates.sql` | Tabela kursów walut i widok zamówień przeliczonych na PLN. |
| `9_advanced_reports.sql` | Zaawansowane raporty SQL. |
| `fetch_nbp_brl_rate.py` | Pobiera kurs BRL/PLN z API NBP i zapisuje go w DWH. |

---

## 7. Uruchomienie środowiska

### 7.1. Uruchomienie SQL Servera w Dockerze

W folderze projektu należy uruchomić:

```powershell
docker compose up -d
```

Sprawdzenie działania kontenera:

```powershell
docker ps
```

Oczekiwany efekt: na liście kontenerów powinien być widoczny kontener `olist-mssql`.

**Miejsce na screen:** wynik `docker ps`.

```text
[SCREEN_02_DOCKER_PS]
```

### 7.2. Przygotowanie środowiska Python

```powershell
python -m venv .venv
.\.venv\Scripts\activate
pip install -r requirements.txt
pip install requests
```

### 7.3. Uruchomienie stagingu

```powershell
python main_staging.py --force
```

Skrypt:

- łączy się z SQL Serverem,
- tworzy lub odtwarza bazę `olist`,
- tworzy tabele staging,
- ładuje dane CSV z folderu `data/`.

**Miejsce na screen:** końcówka logów z komunikatem o poprawnym zakończeniu stagingu.

```text
[SCREEN_03_STAGING_SUCCESS]
```

### 7.4. Uruchomienie hurtowni danych i ETL

```powershell
python main_dwh.py --etl --force
```

Skrypt:

- tworzy lub odtwarza bazę `olist_dwh`,
- tworzy schemat hurtowni,
- ładuje tabele wymiarów,
- ładuje tabele faktów,
- odświeża tabelę agregującą,
- wypisuje podstawowe informacje walidacyjne.

**Miejsce na screen:** końcówka logów z komunikatem o poprawnym zakończeniu DWH ETL.

```text
[SCREEN_04_DWH_ETL_SUCCESS]
```

---

## 8. Najważniejsze fragmenty kodu i ich opis

### 8.1. Konfiguracja SQL Servera w Dockerze

Fragment z pliku `docker-compose.yml`:

```yaml
services:
  mssql:
    image: mcr.microsoft.com/mssql/server:2022-latest
    container_name: olist-mssql
    environment:
      ACCEPT_EULA: "Y"
      MSSQL_SA_PASSWORD: "Test123!"
      MSSQL_PID: "Express"
    ports:
      - "1433:1433"
```

Ten fragment uruchamia lokalny SQL Server 2022 Express w kontenerze Docker. Port `1433` jest wystawiony na komputer hosta, dzięki czemu skrypty Python i VS Code mogą łączyć się z bazą przez `localhost,1433`.

### 8.2. Konfiguracja połączenia z bazą

Fragment z pliku `scripts/database.py`:

```python
@dataclass
class DatabaseConfig:
    server: str = "localhost,1433"
    username: str = "sa"
    password: str = "Test123!"
    database: str = "olist"
```

Ten fragment definiuje domyślne parametry połączenia z SQL Serverem. Dzięki temu skrypty mogą łączyć się zarówno z bazą staging `olist`, jak i z bazą hurtowni `olist_dwh`.

### 8.3. Mapowanie plików CSV na tabele staging

Fragment z pliku `scripts/loader.py`:

```python
CSV_TABLE_MAPPING = {
    "data/olist_customers_dataset.csv": "olist_customers",
    "data/olist_geolocation_dataset.csv": "olist_geolocation",
    "data/olist_orders_dataset.csv": "olist_orders",
    "data/olist_order_items_dataset.csv": "olist_order_items",
    "data/olist_order_payments_dataset.csv": "olist_order_payments",
    "data/olist_order_reviews_dataset.csv": "olist_order_reviews",
    "data/olist_products_dataset.csv": "olist_products",
    "data/olist_sellers_dataset.csv": "olist_sellers",
    "data/product_category_name_translation.csv": "product_category_name_translation",
}
```

Ten słownik określa, który plik CSV ma zostać załadowany do której tabeli staging. Dzięki temu ładowanie danych jest zautomatyzowane i nie wymaga ręcznego importowania każdego pliku.

### 8.4. Uruchamianie stagingu

Fragment z pliku `main_staging.py`:

```python
if not skip_data_load:
    data_loader = DataLoader(db_conn)
    if not data_loader.load_all_data(
        skip_existing=not force_recreate,
        reload_tables=reload_tables,
    ):
        logger.warning("Some CSV data failed to load")
```

Ten fragment odpowiada za uruchomienie procesu ładowania danych CSV. Parametr `skip_existing` pozwala pominąć tabele, które zostały już wcześniej poprawnie załadowane, co stanowi prostą formę kontroli ponownego ładowania danych.

### 8.5. Schemat wymiaru daty

Fragment z pliku `2_dwh_schema.sql`:

```sql
CREATE TABLE dim_date (
    date_key INT PRIMARY KEY,
    calendar_date DATE NOT NULL UNIQUE,
    day_of_week INT NOT NULL,
    month_of_year INT NOT NULL,
    quarter INT NOT NULL,
    year INT NOT NULL,
    is_weekend BIT NOT NULL,
    is_holiday BIT NOT NULL DEFAULT 0
);
```

Tabela `dim_date` jest obowiązkowym wymiarem daty w projekcie hurtowni danych. Klucz `date_key` jest zapisany w formacie `YYYYMMDD`, co ułatwia sortowanie i łączenie z tabelami faktów.

### 8.6. Tabela faktów zamówień

Fragment z pliku `2_dwh_schema.sql`:

```sql
CREATE TABLE fact_orders (
    order_key INT PRIMARY KEY IDENTITY(1,1),
    order_id VARCHAR(32) NOT NULL UNIQUE,
    customer_key INT NOT NULL,
    order_date_key INT NOT NULL,
    order_status_key INT NOT NULL,
    total_items INT,
    total_price DECIMAL(10, 2),
    total_freight_value DECIMAL(10, 2),
    total_order_value DECIMAL(10, 2),
    days_to_delivery INT,
    is_delayed BIT
);
```

Tabela `fact_orders` przechowuje dane na poziomie pojedynczego zamówienia. Zawiera miary takie jak liczba pozycji, wartość zamówienia, koszt frachtu oraz informację, czy dostawa była opóźniona.

### 8.7. Transformacja regionów klientów

Fragment logiki ETL z `main_dwh.py`:

```sql
CASE
    WHEN c.customer_state IN ('SP', 'RJ', 'MG', 'ES') THEN 'Southeast'
    WHEN c.customer_state IN ('PR', 'SC', 'RS') THEN 'South'
    WHEN c.customer_state IN ('BA', 'SE', 'AL', 'PE', 'PB', 'RN', 'CE', 'PI', 'MA') THEN 'Northeast'
    WHEN c.customer_state IN ('GO', 'MT', 'MS', 'DF') THEN 'Center-West'
    WHEN c.customer_state IN ('AM', 'RR', 'AP', 'PA', 'TO', 'RO', 'AC') THEN 'North'
    ELSE 'Unknown'
END AS region_name
```

Ten fragment przypisuje klienta do regionu Brazylii na podstawie kodu stanu. Jest to przykład transformacji danych źródłowych na atrybut analityczny używany później w raportach.

### 8.8. Transformacja produktu

Przykładowa logika w wymiarze produktu:

```sql
p.product_length_cm * p.product_height_cm * p.product_width_cm AS volume_cm3,
CASE WHEN p.product_weight_g > 10000 THEN 1 ELSE 0 END AS is_heavy,
CASE
    WHEN p.product_length_cm * p.product_height_cm * p.product_width_cm < 1000 THEN 'Small'
    WHEN p.product_length_cm * p.product_height_cm * p.product_width_cm < 10000 THEN 'Medium'
    ELSE 'Large'
END AS size_class
```

Na podstawie danych źródłowych wyliczana jest objętość produktu, flaga produktu ciężkiego oraz klasa rozmiaru produktu. Są to transformacje zwiększające wartość analityczną danych.

### 8.9. Obliczanie opóźnień dostawy

Fragment logiki faktu zamówień:

```sql
CASE
    WHEN o.order_delivered_customer_date IS NOT NULL
     AND o.order_estimated_delivery_date IS NOT NULL
     AND o.order_delivered_customer_date > o.order_estimated_delivery_date
    THEN 1 ELSE 0
END AS is_delayed
```

Ten fragment tworzy flagę `is_delayed`, która informuje, czy rzeczywista data dostawy była późniejsza od przewidywanej daty dostawy. Flaga ta jest wykorzystywana w raportach dotyczących jakości dostaw.

### 8.10. Podstawowa delta danych

Fragment z pliku `5_delta_demo.sql`:

```sql
WHERE NOT EXISTS (
    SELECT 1
    FROM fact_orders fo
    WHERE fo.order_id = o.order_id
);
```

Ten warunek zabezpiecza przed ponownym dodaniem zamówienia, które już istnieje w hurtowni danych. Dzięki temu skrypt demonstruje podstawową obsługę delty danych: dopisywane są tylko nowe rekordy.

### 8.11. Logowanie i walidacja ETL

Fragment z pliku `6_etl_audit_schema.sql`:

```sql
CREATE TABLE dbo.etl_run_log (
    etl_run_id INT IDENTITY(1,1) PRIMARY KEY,
    run_name VARCHAR(100) NOT NULL,
    started_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    finished_at DATETIME2 NULL,
    status VARCHAR(30) NOT NULL,
    fact_orders_rows INT NULL,
    fact_order_items_rows INT NULL,
    agg_monthly_sales_rows INT NULL
);
```

Tabela `etl_run_log` zapisuje informacje o przebiegu walidacji po ETL, w tym liczbę rekordów w najważniejszych tabelach oraz końcowy status sprawdzenia.

### 8.12. SCD Type 2 dla produktu

Fragment z pliku `7_scd_type2_product.sql`:

```sql
CREATE TABLE dbo.dim_product_scd2 (
    product_scd_key INT IDENTITY(1,1) PRIMARY KEY,
    product_id VARCHAR(32) NOT NULL,
    category_name_pt VARCHAR(200) NULL,
    category_name_en VARCHAR(200) NULL,
    valid_from DATETIME2 NOT NULL,
    valid_to DATETIME2 NULL,
    is_current BIT NOT NULL,
    scd_hash VARBINARY(32) NOT NULL
);
```

Tabela `dim_product_scd2` pokazuje podejście SCD Type 2. Zamiast nadpisywać dane produktu, tabela przechowuje wersje historyczne z zakresem obowiązywania `valid_from` i `valid_to` oraz flagą `is_current`.

### 8.13. Integracja z API NBP

Fragment z pliku `fetch_nbp_brl_rate.py`:

```python
NBP_URL = "https://api.nbp.pl/api/exchangerates/rates/a/brl/?format=json"

response = requests.get(NBP_URL, headers={"Accept": "application/json"}, timeout=30)
response.raise_for_status()
payload = response.json()
```

Skrypt pobiera aktualny kurs BRL/PLN z publicznego API NBP. Kurs jest zapisywany do tabeli `dim_exchange_rate`, a następnie używany do widoku `vw_fact_orders_pln`, który przelicza wartość zamówień z BRL na PLN.

---

## 9. Model hurtowni danych

Model hurtowni jest oparty o podejście wymiarowe. Zastosowano tabele faktów oraz wymiary współdzielone przez różne obszary analityczne.

### 9.1. Wymiary

| Tabela | Opis |
|---|---|
| `dim_date` | Wymiar daty, jedna linia na dzień. |
| `dim_customer` | Dane klientów oraz region klienta. |
| `dim_seller` | Dane sprzedawców oraz region sprzedawcy. |
| `dim_product` | Dane produktów, kategorie, wymiary, ciężar i klasa rozmiaru. |
| `dim_payment_type` | Typy płatności i ich klasyfikacja. |
| `dim_order_status` | Statusy zamówień i ich znaczenie biznesowe. |
| `dim_geolocation` | Dane geograficzne po kodach pocztowych. |
| `dim_exchange_rate` | Kurs BRL/PLN pobrany z zewnętrznego API NBP. |
| `dim_product_scd2` | Demonstracja wymiaru historycznego SCD Type 2. |

### 9.2. Fakty

| Tabela | Ziarno tabeli | Przykładowe miary |
|---|---|---|
| `fact_orders` | jedno zamówienie | wartość zamówienia, liczba pozycji, dni dostawy, opóźnienie |
| `fact_order_items` | jedna pozycja zamówienia | cena, fracht, wartość pozycji |
| `fact_payments` | jedna płatność/rata | wartość płatności, liczba rat |
| `fact_reviews` | jedna opinia | ocena, długość komentarza, czas odpowiedzi |

### 9.3. Agregacja

| Tabela | Opis |
|---|---|
| `agg_monthly_sales` | Agregacja miesięczna sprzedaży według kategorii, sprzedawcy i czasu. |

Tabela agregująca przyspiesza raportowanie i pokazuje wymaganą w projekcie agregację danych.

**Miejsce na screen:** schemat/model bazy albo widok tabel w `olist_dwh`.

```text
[SCREEN_05_DWH_SCHEMA]
```

---

## 10. Transformacje danych

W projekcie wykonano następujące transformacje:

| Transformacja | Opis | Gdzie |
|---|---|---|
| Region klienta | Przypisanie regionu Brazylii na podstawie `customer_state`. | `main_dwh.py`, `dim_customer` |
| Region sprzedawcy | Przypisanie regionu Brazylii na podstawie `seller_state`. | `main_dwh.py`, `dim_seller` |
| Tłumaczenie kategorii | Połączenie z tabelą `product_category_name_translation`. | `main_dwh.py`, `dim_product` |
| Objętość produktu | `length * height * width`. | `main_dwh.py`, `dim_product` |
| Klasa rozmiaru | Przypisanie `Small`, `Medium`, `Large` na podstawie objętości. | `main_dwh.py`, `dim_product` |
| Produkt ciężki | Flaga dla produktów o dużej wadze. | `main_dwh.py`, `dim_product` |
| Dni dostawy | Różnica między datą zakupu i datą dostawy. | `main_dwh.py`, `fact_orders` |
| Opóźnienie dostawy | Porównanie rzeczywistej i przewidywanej daty dostawy. | `main_dwh.py`, `fact_orders` |
| Pozytywna opinia | Flaga dla ocen większych lub równych 4. | `main_dwh.py`, `fact_reviews` |
| Agregacja miesięczna | Sprzedaż, liczba zamówień, opóźnienia i średnie oceny. | `agg_monthly_sales` |

---

## 11. Raporty podstawowe

Raporty podstawowe znajdują się w pliku:

```text
3_reports.sql
```

Przygotowano pięć raportów, mimo że wymagane były minimum trzy:

### 11.1. Raport 1 — miesięczna sprzedaż według kategorii produktu

Cel: identyfikacja kategorii generujących największy przychód w czasie.

Najważniejsze elementy zapytania:

```sql
SELECT TOP 50
    year,
    month,
    category_name_en,
    SUM(total_orders) AS total_orders,
    SUM(total_revenue) AS total_revenue
FROM agg_monthly_sales
GROUP BY year, month, category_name_en
ORDER BY year, month, total_revenue DESC;
```

**Miejsce na screen:** wynik raportu 1.

```text
[SCREEN_06_REPORT_MONTHLY_SALES]
```

### 11.2. Raport 2 — opóźnienia dostaw według regionu klienta

Cel: wskazanie regionów, w których logistyka działa najgorzej.

W raporcie wykorzystywane są `fact_orders` oraz `dim_customer`.

**Miejsce na screen:** wynik raportu 2.

```text
[SCREEN_07_REPORT_DELAYS_BY_REGION]
```

### 11.3. Raport 3 — ranking sprzedawców

Cel: identyfikacja najlepszych sprzedawców pod względem przychodu, liczby zamówień i średniej oceny.

Raport wykorzystuje CTE oraz funkcję okna `RANK()`.

**Miejsce na screen:** wynik raportu 3.

```text
[SCREEN_08_REPORT_SELLER_RANKING]
```

### 11.4. Raport 4 — analiza metod płatności

Cel: sprawdzenie, które metody płatności są najczęściej używane i które odpowiadają za największą wartość transakcji.

**Miejsce na screen opcjonalny:** wynik raportu 4.

```text
[SCREEN_09_REPORT_PAYMENT_METHODS]
```

### 11.5. Raport 5 — kategorie produktów, fracht i oceny

Cel: porównanie kategorii produktów pod względem przychodu, kosztów dostawy i satysfakcji klientów.

**Miejsce na screen opcjonalny:** wynik raportu 5.

```text
[SCREEN_10_REPORT_PRODUCT_CATEGORIES]
```

---

## 12. Raporty zaawansowane SQL

Raporty zaawansowane znajdują się w pliku:

```text
9_advanced_reports.sql
```

Wykorzystano w nich bardziej złożone techniki SQL:

- CTE,
- funkcje okna,
- `LAG()` do porównania miesiąc do miesiąca,
- `RANK()` do rankingów,
- `NTILE()` do segmentacji klientów,
- udziały procentowe,
- widok z danymi z zewnętrznego API.

Przykład raportu z dynamiką miesięczną:

```sql
LAG(monthly_revenue) OVER (ORDER BY year, month_of_year) AS previous_month_revenue
```

Ten fragment pozwala porównać sprzedaż danego miesiąca ze sprzedażą miesiąca poprzedniego.

**Miejsce na screen:** wynik wybranego raportu zaawansowanego, np. dynamika miesięczna lub ranking kategorii.

```text
[SCREEN_11_ADVANCED_REPORT]
```

---

## 13. Walidacje jakości danych

Walidacje znajdują się w pliku:

```text
4_validation_queries.sql
```

Zakres walidacji:

| Walidacja | Oczekiwany wynik |
|---|---|
| Liczba rekordów w tabelach DWH | tabele faktów i wymiary nie są puste |
| Status tabel faktów | `OK` |
| Duplikaty kluczy biznesowych w wymiarach | brak rekordów |
| Brakujące klucze wymiarów w `fact_order_items` | same zera |
| Spójność przychodu fakt vs agregacja | różnica równa 0 lub bardzo bliska 0 |

Przykład walidacji spójności przychodu:

```sql
SELECT
    (SELECT CAST(SUM(ISNULL(total_item_value, 0) + ISNULL(freight_value, 0)) AS DECIMAL(15, 2)) FROM fact_order_items) AS revenue_from_fact_items,
    (SELECT CAST(SUM(total_revenue) AS DECIMAL(15, 2)) FROM agg_monthly_sales) AS revenue_from_aggregate,
    ... AS difference;
```

Walidacja porównuje przychód obliczony z tabeli faktów z przychodem zapisanym w tabeli agregującej. Dzięki temu można sprawdzić, czy agregacja została wyliczona poprawnie.

**Miejsce na screen:** wynik liczby rekordów.

```text
[SCREEN_12_VALIDATION_ROW_COUNTS]
```

**Miejsce na screen:** status `OK` i brakujące klucze.

```text
[SCREEN_13_VALIDATION_STATUS_KEYS]
```

**Miejsce na screen:** zgodność przychodu fakt vs agregacja.

```text
[SCREEN_14_VALIDATION_REVENUE_CONSISTENCY]
```

---

## 14. Podstawowa obsługa delty danych

Podstawowa delta danych znajduje się w pliku:

```text
5_delta_demo.sql
```

Skrypt pokazuje sposób dopisywania wyłącznie nowych rekordów do tabel faktów. Najważniejszy mechanizm to warunek `NOT EXISTS`, który sprawdza, czy dany rekord już istnieje w hurtowni.

Przykład:

```sql
WHERE NOT EXISTS (
    SELECT 1
    FROM fact_orders fo
    WHERE fo.order_id = o.order_id
);
```

Interpretacja:

- jeżeli `order_id` nie istnieje jeszcze w `fact_orders`, rekord może zostać dodany,
- jeżeli `order_id` już istnieje, rekord nie zostanie załadowany drugi raz.

Podczas testu skrypt zwrócił komunikaty typu `0 rows affected` dla części dopisującej nowe rekordy, co było oczekiwane — dane były już wcześniej w pełni załadowane do hurtowni. Następnie odświeżona została tabela agregująca.

**Miejsce na screen:** wynik uruchomienia `5_delta_demo.sql`.

```text
[SCREEN_15_DELTA_DEMO]
```

---

## 15. Logowanie i audyt ETL

Element dodatkowy na plus został zrealizowany w pliku:

```text
6_etl_audit_schema.sql
```

Skrypt tworzy tabele:

| Tabela | Opis |
|---|---|
| `etl_run_log` | zapisuje informacje o przebiegu walidacji po ETL, status oraz liczby rekordów |
| `etl_validation_result` | zapisuje wyniki poszczególnych kontroli jakości danych |

Przykład tworzenia tabeli logów:

```sql
CREATE TABLE dbo.etl_run_log (
    etl_run_id INT IDENTITY(1,1) PRIMARY KEY,
    run_name VARCHAR(100) NOT NULL,
    started_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    finished_at DATETIME2 NULL,
    status VARCHAR(30) NOT NULL,
    fact_orders_rows INT NULL,
    fact_order_items_rows INT NULL,
    fact_payments_rows INT NULL,
    fact_reviews_rows INT NULL,
    agg_monthly_sales_rows INT NULL
);
```

Dzięki temu po uruchomieniu walidacji można sprawdzić, czy ETL zakończył się sukcesem oraz ile rekordów znajduje się w najważniejszych tabelach.

**Miejsce na screen:** wynik z `etl_run_log` oraz `etl_validation_result`.

```text
[SCREEN_16_ETL_AUDIT_LOG]
```

---

## 16. SCD Type 2

Element dodatkowy dotyczący SCD został zrealizowany w pliku:

```text
7_scd_type2_product.sql
```

Standardowe wymiary `dim_customer`, `dim_seller` i `dim_product` działają jak wymiary aktualnego stanu, czyli podejście zbliżone do **SCD Type 1**. Dodatkowo przygotowano tabelę:

```text
dim_product_scd2
```

Tabela ta demonstruje podejście **SCD Type 2**, czyli przechowywanie historii zmian. Każda wersja produktu ma pola:

- `valid_from` — początek obowiązywania wersji,
- `valid_to` — koniec obowiązywania wersji,
- `is_current` — informacja, czy rekord jest aktualny,
- `scd_hash` — hash atrybutów używany do wykrywania zmiany.

Przykład definicji:

```sql
valid_from DATETIME2 NOT NULL,
valid_to DATETIME2 NULL,
is_current BIT NOT NULL,
scd_hash VARBINARY(32) NOT NULL
```

Skrypt zawiera demonstrację zmiany produktu: aktualna wersja zostaje zamknięta przez ustawienie `valid_to` i `is_current = 0`, a następnie tworzona jest nowa wersja z `is_current = 1`.

**Miejsce na screen:** produkt z wersją historyczną i aktualną w `dim_product_scd2`.

```text
[SCREEN_17_SCD2_PRODUCT]
```

---

## 17. Integracja z zewnętrznym źródłem danych — API NBP

Element dodatkowy dotyczący integracji z innym źródłem danych został zrealizowany przy pomocy plików:

```text
fetch_nbp_brl_rate.py
8_external_nbp_rates.sql
```

Skrypt `fetch_nbp_brl_rate.py` pobiera aktualny kurs waluty BRL/PLN z publicznego API NBP:

```python
NBP_URL = "https://api.nbp.pl/api/exchangerates/rates/a/brl/?format=json"
```

Pobrany kurs jest zapisywany w tabeli:

```text
dim_exchange_rate
```

Następnie plik `8_external_nbp_rates.sql` tworzy widok:

```text
vw_fact_orders_pln
```

Widok przelicza wartość zamówień z BRL na PLN.

Przykładowy wynik widoku zawiera kolumny:

```text
order_id
customer_key
order_date_key
total_order_value_brl
brl_to_pln_rate
total_order_value_pln
exchange_rate_date
```

**Miejsce na screen:** wynik `SELECT TOP 10 * FROM dbo.vw_fact_orders_pln`.

```text
[SCREEN_18_NBP_PLN_VIEW]
```

---

## 18. Power BI / wizualizacja raportowa

Jeżeli do projektu zostanie dołączony plik Power BI, należy umieścić go w folderze:

```text
reports/olist_dwh_powerbi.pbix
```

W Power BI można pokazać:

- sprzedaż miesięczną,
- liczbę zamówień,
- opóźnienia dostaw,
- sprzedaż według kategorii,
- wartości zamówień przeliczone na PLN.

**Miejsce na screen:** strona raportu Power BI — sprzedaż.

```text
[SCREEN_19_POWERBI_SALES]
```

**Miejsce na screen:** strona raportu Power BI — dostawy lub PLN.

```text
[SCREEN_20_POWERBI_DELIVERY_OR_PLN]
```

Jeżeli Power BI nie zostanie finalnie dołączony, tę sekcję można usunąć z dokumentacji albo zostawić jako możliwe rozszerzenie projektu.

---

## 19. Wnioski

Projekt pokazuje pełny proces budowy hurtowni danych:

- dane źródłowe są najpierw ładowane do warstwy staging,
- następnie są transformowane i przenoszone do modelu wymiarowego,
- w hurtowni dostępne są tabele faktów i wymiary pozwalające analizować sprzedaż, logistykę, płatności oraz opinie klientów,
- przygotowano tabelę agregującą, która ułatwia raportowanie miesięczne,
- dodano walidacje jakości danych,
- przygotowano podstawową obsługę delty danych,
- rozszerzono projekt o logowanie walidacji, demonstrację SCD Type 2 i integrację z API NBP.

Największą wartością biznesową projektu jest możliwość analizowania wyników sprzedaży i jakości dostaw w wielu przekrojach: czas, kategoria produktu, sprzedawca, region klienta, metoda płatności i ocena klienta.

---

## 20. Możliwe dalsze rozszerzenia

Projekt można rozbudować o:

- pełną produkcyjną implementację SCD Type 2 dla większej liczby wymiarów,
- harmonogramowanie ETL przy pomocy SSIS, Airflow lub Pentaho,
- pełny model OLAP / SSAS,
- dashboard Power BI z większą liczbą stron i filtrów,
- automatyczne testy jakości danych,
- obsługę wielu kursów walut i danych historycznych,
- tabelę techniczną kontrolującą każdą paczkę danych ładowaną jako delta.

---

## 21. Instrukcja finalnego oddania projektu

Do paczki ZIP powinny trafić:

```text
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
main_staging.py
main_dwh.py
requirements.txt
docker-compose.yml
scripts/
data/DATA.md
DOCUMENTATION.md albo DOCUMENTATION.pdf
screenshots/
reports/olist_dwh_powerbi.pbix — jeżeli Power BI jest dołączany
```

Nie należy dodawać do ZIP-a:

```text
.venv/
__pycache__/
scripts/__pycache__/
logów tymczasowych, jeśli nie są potrzebne
dużych CSV, jeżeli prowadzący nie wymaga ich dołączenia
```

---

## 22. Lista screenów do wstawienia

| Kod w dokumentacji | Co ma przedstawiać | Plik proponowany |
|---|---|---|
| `[SCREEN_01_DATA_FOLDER]` | folder `data/` z CSV | `screenshots/01_data_folder.png` |
| `[SCREEN_02_DOCKER_PS]` | działający kontener `olist-mssql` | `screenshots/02_docker_ps.png` |
| `[SCREEN_03_STAGING_SUCCESS]` | poprawne zakończenie `main_staging.py` | `screenshots/03_staging_success.png` |
| `[SCREEN_04_DWH_ETL_SUCCESS]` | poprawne zakończenie `main_dwh.py --etl` | `screenshots/04_dwh_etl_success.png` |
| `[SCREEN_05_DWH_SCHEMA]` | tabele w bazie `olist_dwh` albo model | `screenshots/05_dwh_schema.png` |
| `[SCREEN_06_REPORT_MONTHLY_SALES]` | raport miesięcznej sprzedaży | `screenshots/06_report_monthly_sales.png` |
| `[SCREEN_07_REPORT_DELAYS_BY_REGION]` | raport opóźnień według regionu | `screenshots/07_report_delays_by_region.png` |
| `[SCREEN_08_REPORT_SELLER_RANKING]` | ranking sprzedawców | `screenshots/08_report_seller_ranking.png` |
| `[SCREEN_11_ADVANCED_REPORT]` | wybrany raport zaawansowany | `screenshots/11_advanced_report.png` |
| `[SCREEN_12_VALIDATION_ROW_COUNTS]` | liczba rekordów w tabelach DWH | `screenshots/12_validation_row_counts.png` |
| `[SCREEN_13_VALIDATION_STATUS_KEYS]` | status OK / brakujące klucze | `screenshots/13_validation_status_keys.png` |
| `[SCREEN_14_VALIDATION_REVENUE_CONSISTENCY]` | zgodność przychodu fakt vs agregacja | `screenshots/14_validation_revenue_consistency.png` |
| `[SCREEN_15_DELTA_DEMO]` | wynik delty | `screenshots/15_delta_demo.png` |
| `[SCREEN_16_ETL_AUDIT_LOG]` | `etl_run_log` i `etl_validation_result` | `screenshots/16_etl_audit_log.png` |
| `[SCREEN_17_SCD2_PRODUCT]` | wersje produktu w `dim_product_scd2` | `screenshots/17_scd2_product.png` |
| `[SCREEN_18_NBP_PLN_VIEW]` | widok `vw_fact_orders_pln` | `screenshots/18_nbp_pln_view.png` |
| `[SCREEN_19_POWERBI_SALES]` | Power BI sprzedaż, jeśli dołączasz | `screenshots/19_powerbi_sales.png` |
| `[SCREEN_20_POWERBI_DELIVERY_OR_PLN]` | Power BI dostawy/PLN, jeśli dołączasz | `screenshots/20_powerbi_delivery_or_pln.png` |


# StockWise AI

StockWise AI, Firebird kaynak verisini PostgreSQL'e ETL ile taşıyan, FastAPI backend ile servis eden ve React/Vite frontend ile görüntüleyen stok/planlama uygulamasıdır.

## Bilesenler

- `etl/`: Firebird -> PostgreSQL senkronizasyonu (`raw_sync.py`)
- `backend/`: FastAPI API katmani (`backend/main.py`)
- `frontend/`: React + Vite arayuzu
- `start_services.cmd`: backend + etl birlikte baslatma script'i

## Mimari Ozet

1. ETL Firebird'den raw veriyi alir (`raw.*`), core tablolari uretir/gunceller.
2. Backend PostgreSQL'den okuyup API sunar.
3. Frontend API'den veriyi cekerek ekranda gosterir.

## Gereksinimler

- Windows + WSL2 (Ubuntu-22.04)
- Python 3.11
- PostgreSQL (WSL icinde)
- Firebird ODBC driver + `fbclient.dll` erisimi
- Node.js (frontend icin)

## Ortam Degiskenleri

`.env` dosyasinda en az su alanlar dolu olmali:

- `PG_HOST`, `PG_PORT`, `PG_DB`, `PG_USER`, `PG_PASSWORD`
- `FB_ODBC_DSN_FULL`, `FB_ODBC_DSN_LIVE` (veya `FB_ODBC_DSN`)
- `FORECAST_COMMAND` (forecast calisacaksa)

Not: WSL2 + Windows uygulama baglantisinda genelde `PG_HOST=127.0.0.1` kullanilir.

## Hizli Kurulum

1. Python sanal ortam ve bagimliliklar:

```bat
python -m venv .venv
c:\tkis_stockwise\.venv\Scripts\python.exe -m pip install -r etl\requirements.txt
c:\tkis_stockwise\.venv\Scripts\python.exe -m pip install -r backend\requirements.txt
```

2. Baglanti testi:

```bat
python tools\tests\test_connections.py
```

3. Frontend:

```bat
cd frontend
npm install
npm run build
```

## ETL Calistirma Modlari

### 1) Ilk kurulum (onerilen)

```bat
c:\tkis_stockwise\.venv\Scripts\python.exe etl\raw_sync.py --bootstrap
```

Akis:
- full yukleme (test/full DSN)
- canli DSN ile tek incremental catch-up
- monthly seat refresh
- weekly pre + forecast + weekly post

### 2) Sadece full yukleme

```bat
c:\tkis_stockwise\.venv\Scripts\python.exe etl\raw_sync.py --full
```

### 3) Surekli incremental dongu

```bat
c:\tkis_stockwise\.venv\Scripts\python.exe etl\raw_sync.py
```

## Forecast Mimarisi (Kritik Akis)

Bu proje forecast tarafinda **hibrit model secimi** kullanir: tek bir model zorlanmaz, her malzeme icin backtest performansina gore en iyi model secilir.

### Dosya ve SQL yolu (kritik)

- Orkestrasyon: `etl/raw_sync.py`
- Forecast runner: `etl/run_forecast.py`
- Model/backtest motoru: `etl/forecast_backtest.py`
- Weekly pre SQL: `etl/sql/core_weekly_pre_forecast.sql`
- Weekly post SQL: `etl/sql/core_weekly_post_forecast.sql`
- Monthly seat SQL: `etl/sql/core_monthly_seat.sql`

### End-to-end sira

1. `core_weekly_pre_forecast.sql`
2. `FORECAST_COMMAND` ile `etl/run_forecast.py`
3. `core_weekly_post_forecast.sql`

`--bootstrap` akisinda bunun oncesinde:
- full yukleme (full DSN),
- canli DSN ile 1 tur incremental catch-up,
- monthly seat refresh
calisir.

### Hibrit model yaklasimi

`etl/forecast_backtest.py` icindeki model havuzu:
- `TSB` (intermittent demand icin)
- `ETS` (Exponential Smoothing, kosula bagli)
- `MA4`, `MA13`, `MA26` (moving average aileleri)

Secim mantigi:
- Son 52 hafta rolling backtest yapilir.
- Hedef metrik: `WAPE` (daha dusuk daha iyi).
- Her malzeme icin en iyi model secilir (`chosen_method`).
- Son 26 haftasi sifir olan malzemede `INACTIVE_ZERO` zorlanir (forecast=0).
- Her malzeme icin ileri 12 hafta (`forecast_12w`) uretilir.

Bu nedenle yaklasim "hibrit"tir: model secimi malzeme bazinda dinamiktir, global tek model yoktur.

### Forecast ciktilari

CSV ciktilari:
- `material_level_backtest.csv`
- `category_unit_backtest.csv`
- `overall_backtest.csv`

DB tablolari:
- `core.final_forecast_summary`
- `core.final_forecast`
- `core.final_forecast_material_metrics`
- `core.final_forecast_category_unit_metrics`
- `core.final_forecast_overall_metrics`

Dashboard/servis tarafi forecast verisini `core.final_forecast_summary` ve post SQL ile uretilen dashboard tablolari uzerinden kullanir.

## Servis Baslatma

```bat
c:\tkis_stockwise\start_services.cmd
```

Bu script:
- PostgreSQL erisilebilirligini kontrol eder
- backend'i `logs\uvicorn.log` altina
- ETL'i `logs\raw_sync.log` altina baslatir
- baslangic adimlarini `logs\start_services.log` dosyasina yazar

## Task Scheduler (Reboot Sonrasi Otomatik Baslatma)

Kullanilan tasklar:
- `\Start WSL Services`
- `\StockWise Services`

Onemli:
- `Run as` kullanicisi WSL distro'nun kurulu oldugu kullanici olmali.
- Task tetikleyicisi `At system startup` olmali.

## Sik Karsilasilan Sorunlar

### Firebird ODBC baglanmiyor (`FBCLIENT.DLL failed to load`)

- Firebird ODBC ve fbclient bitness uyumunu kontrol edin (64-bit Python -> 64-bit client).
- `FBCLIENT.DLL` erisimi dogrulayin.
- `python tools\tests\test_connections.py` ile tekrar test edin.

### Reboot sonrasi servis acilmiyor

- `start_services.cmd` icindeki Postgre hostunu (`127.0.0.1`) kontrol edin.
- Task `Run as` hesabini kontrol edin.
- `logs\start_services.log` ve Task History'yi inceleyin.

## Gelistirme Notlari

- ETL ana kodu: `etl/raw_sync.py`
- Backend API: `backend/main.py`
- SQL job dosyalari: `etl/sql/`

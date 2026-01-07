import pandas as pd
import io
import os
import sys

# Konfigurasi Database (Diambil dari notebook Anda)
DB_USER = "postgres"
DB_PASS = "12345"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "flight_weather"

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
except ImportError as exc:
    raise SystemExit(
        "psycopg2 is required. Install with: pip install psycopg2-binary"
    ) from exc

def get_conn():
    """Membuat koneksi ke database PostgreSQL"""
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        sslmode="disable",
        connect_timeout=10
    )

def normalize_col_names(df):
    """Membersihkan nama kolom agar sesuai standar database (lowercase, no space)"""
    df.columns = (
        df.columns.astype(str)
        .str.strip()
        .str.lower()
        .str.replace(" ", "_", regex=False)
        .str.replace("/", "_", regex=False)
        .str.replace("-", "_", regex=False)
        .str.replace("°", "deg", regex=False)
        .str.replace("(", "", regex=False)
        .str.replace(")", "", regex=False)
        .str.replace("%", "percent", regex=False)
        .str.replace("mm", "mm", regex=False)
        .str.replace("hpa", "hpa", regex=False)
        .str.replace("cm", "cm", regex=False)
        .str.replace("wmo_code", "wmo_code", regex=False)
        .str.replace("km_h", "kmh", regex=False)
    )
    return df

def load_data_to_postgres(df, table_name, conn_func, if_exists='replace', primary_key_cols=None, foreign_key_definitions=None):
    """
    Fungsi generik untuk memuat DataFrame ke PostgreSQL dengan performa tinggi (COPY command).
    Mendukung pembuatan Primary Key dan Foreign Key secara otomatis.
    """
    df_copy = df.copy()
    df_copy = normalize_col_names(df_copy)

    print(f"   -> Loading table '{table_name}'...")

    with conn_func() as conn:
        with conn.cursor() as cur:
            cur.execute("SET search_path TO public;")

            # 1. Infer PostgreSQL data types
            cols_ddl_list = []
            for c in df_copy.columns:
                dt = df_copy[c].dtype
                pg_type = ""
                if str(dt).startswith("int"): 
                    pg_type = "BIGINT"
                elif str(dt).startswith("float"): 
                    pg_type = "DOUBLE PRECISION"
                elif str(dt).startswith("bool"): 
                    pg_type = "BOOLEAN"
                elif "datetime" in str(dt):
                    pg_type = "TIMESTAMP"
                else:
                    pg_type = "TEXT"
                cols_ddl_list.append(f'"{c}" {pg_type}')

            # 2. Add Primary Key constraint
            if primary_key_cols:
                cols_ddl_list.append(f'PRIMARY KEY ({", ".join([f"{col}" for col in primary_key_cols])})')

            # 3. Add Foreign Key constraints
            if foreign_key_definitions:
                for fk_def in foreign_key_definitions:
                    local_col = fk_def['local_col']
                    ref_table = fk_def['ref_table']
                    ref_col = fk_def['ref_col']
                    cols_ddl_list.append(f'FOREIGN KEY ("{local_col}") REFERENCES public."{ref_table}" ("{ref_col}")')

            create_table_sql = f'CREATE TABLE public."{table_name}" (\n  {",\n  ".join(cols_ddl_list)}\n);'

            # 4. Drop and create table
            if if_exists == 'replace':
                cur.execute(f'DROP TABLE IF EXISTS public."{table_name}" CASCADE;')
            cur.execute(create_table_sql)

            # 5. Fast load via COPY (In-memory buffer)
            buf = io.StringIO()
            df_copy.to_csv(buf, index=False, header=False)
            buf.seek(0)

            cols_list = ", ".join([f'"{col}"' for col in df_copy.columns])
            cur.copy_expert(
                f'COPY public."{table_name}" ({cols_list}) FROM STDIN WITH (FORMAT CSV)',
                buf
            )
        conn.commit()

        with conn.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) FROM public."{table_name}";')
            n = cur.fetchone()[0]
        print(f"      ✅ Loaded {n:,} rows into public.{table_name}")

def load_star_schema_to_dw(df):
    """
    Fungsi utama (Orchestrator) untuk memecah df_final menjadi tabel Dimensi & Fakta.
    """
    print("\n==========================================")
    print("   STARTING STAR SCHEMA LOAD (COPY MODE)  ")
    print("==========================================\n")

    # 1. Rename encoded columns to represent keys
    # Sesuaikan mapping ini dengan output dari transformation.py Anda
    df_star = df.copy()
    
    # Mapping nama kolom dari transformation.py ke nama key database
    rename_mapping = {
        'airline_encode': 'airline_key',         # transformation: airline_encode
        'origin_cities_encode': 'origin_city_key',
        'dest_cities_encode': 'dest_city_key'
    }
    
    # Hanya rename kolom yang ada
    df_star = df_star.rename(columns={k: v for k, v in rename_mapping.items() if k in df_star.columns})

    # Normalize column names in df_star before selecting
    df_star = normalize_col_names(df_star)

    # ---------------------------------------------------------
    # 2. Create Dimension Tables
    # ---------------------------------------------------------
    print("\n[1/2] Creating Dimension Tables...")

    # A. Dim Airline
    if 'airline_key' in df_star.columns:
        dim_airline = df_star[['airline_key', 'airline', 'airline_code']].drop_duplicates(subset=['airline_key']).sort_values('airline_key').reset_index(drop=True)
        load_data_to_postgres(dim_airline, "dim_airline", get_conn, primary_key_cols=['airline_key'])
    else:
        print("   ⚠️ Skip Dim Airline: 'airline_key' not found.")

    # B. Dim Origin City
    if 'origin_city_key' in df_star.columns:
        dim_origin_city = df_star[['origin_city_key', 'origin_city', 'origin']].drop_duplicates(subset=['origin_city_key']).sort_values('origin_city_key').reset_index(drop=True)
        load_data_to_postgres(dim_origin_city, "dim_origin_city", get_conn, primary_key_cols=['origin_city_key'])
    else:
        print("   ⚠️ Skip Dim Origin City: 'origin_city_key' not found.")

    # C. Dim Destination City
    if 'dest_city_key' in df_star.columns:
        dim_dest_city = df_star[['dest_city_key', 'dest_city', 'dest']].drop_duplicates(subset=['dest_city_key']).sort_values('dest_city_key').reset_index(drop=True)
        load_data_to_postgres(dim_dest_city, "dim_dest_city", get_conn, primary_key_cols=['dest_city_key'])
    else:
        print("   ⚠️ Skip Dim Dest City: 'dest_city_key' not found.")

    # D. Dim Date
    if 'fl_date' in df_star.columns:
        dim_date = df_star[['fl_date']].drop_duplicates().copy()
        dim_date['date_key'] = dim_date['fl_date'] # Use fl_date as date_key (Int format YYYYMMDD)
        
        # Parsing tanggal (fl_date formatnya int YYYYMMDD, perlu di-cast ke string dulu untuk parsing)
        dim_date['temp_date_str'] = dim_date['fl_date'].astype(str)
        dim_date['year'] = dim_date['temp_date_str'].str[:4].astype(int)
        dim_date['month'] = dim_date['temp_date_str'].str[4:6].astype(int)
        dim_date['day'] = dim_date['temp_date_str'].str[6:8].astype(int)
        
        # Konversi ke datetime object untuk fitur advanced (day name, quarter)
        dim_date['dt_obj'] = pd.to_datetime(dim_date['temp_date_str'], format='%Y%m%d', errors='coerce')
        dim_date['day_of_week'] = dim_date['dt_obj'].dt.dayofweek # 0=Monday
        dim_date['day_name'] = dim_date['dt_obj'].dt.day_name()
        dim_date['quarter'] = dim_date['dt_obj'].dt.quarter
        
        # Select final columns
        dim_date = dim_date[['date_key', 'year', 'month', 'day', 'day_of_week', 'day_name', 'quarter']].sort_values('date_key').reset_index(drop=True)
        load_data_to_postgres(dim_date, "dim_date", get_conn, primary_key_cols=['date_key'])
    else:
        print("   ⚠️ Skip Dim Date: 'fl_date' not found.")

    # ---------------------------------------------------------
    # 3. Create Fact Table
    # ---------------------------------------------------------
    print("\n[2/2] Creating Fact Table...")
    
    # Kolom dimensi (text) yang tidak perlu ada di tabel fakta karena sudah ada key-nya
    dim_cols_to_exclude = [
        'airline', 'airline_code', 
        'origin', 'origin_city', 
        'dest', 'dest_city',
        'temp_date_str', 'dt_obj', # kolom temporary
        'crs_dep_time_rounded', 'crs_arr_time_rounded', 'origin_time', 'dest_time'
    ]
    
    # Ambil semua kolom kecuali kolom dimensi text
    fact_cols = [col for col in df_star.columns if col not in dim_cols_to_exclude]
    
    fact_flights = df_star[fact_cols].copy()
    
    # Rename fl_date to date_key for Foreign Key consistency
    if 'fl_date' in fact_flights.columns:
        fact_flights = fact_flights.rename(columns={'fl_date': 'date_key'})

    # Definisi Foreign Keys untuk DDL PostgreSQL
    foreign_keys_for_fact = [
        {'local_col': 'airline_key', 'ref_table': 'dim_airline', 'ref_col': 'airline_key'},
        {'local_col': 'origin_city_key', 'ref_table': 'dim_origin_city', 'ref_col': 'origin_city_key'},
        {'local_col': 'dest_city_key', 'ref_table': 'dim_dest_city', 'ref_col': 'dest_city_key'},
        {'local_col': 'date_key', 'ref_table': 'dim_date', 'ref_col': 'date_key'}
    ]

    # Filter FK definition jika tabel dimensi terkait tidak berhasil dibuat (opsional, tapi aman)
    # Disini kita asumsikan semua dimensi berhasil dibuat.
    
    load_data_to_postgres(fact_flights, "fact_flights", get_conn, foreign_key_definitions=foreign_keys_for_fact)

    print("\n==========================================")
    print("       WAREHOUSE LOAD COMPLETED           ")
    print("==========================================\n")
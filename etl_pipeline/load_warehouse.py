import os
import io
import psycopg2
from psycopg2.extras import RealDictCursor
from sqlalchemy import create_engine

# Konfigurasi Koneksi Database
# SANGAT DISARANKAN: Jangan hardcode password di file produksi. Gunakan Environment Variables (.env).
DB_HOST = os.getenv("PGHOST", "dpg-d5cb930gjchc73cik7b0-a.singapore-postgres.render.com")
DB_PORT = int(os.getenv("PGPORT", "5432"))
DB_NAME = os.getenv("PGDATABASE", "etl_dw_prod")
DB_USER = os.getenv("PGUSER", "etl_user")
DB_PASS = os.getenv("PGPASSWORD", "h7UOWHH8PAhFFdA2ApIqikiXlz6DqGik")

def get_conn():
    """Membuat koneksi ke PostgreSQL."""
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        sslmode="require",
        connect_timeout=10
    )

def test_connection():
    """Fungsi utilitas untuk mengetes koneksi database."""
    try:
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT now() AS server_time, current_database() AS db;")
                result = cur.fetchone()
                print(f"✅ CONNECTED TO PROD DB: {result}")
        return True
    except Exception as e:
        print("❌ CONNECTION FAILED")
        print(e)
        return False

def load_final_merged_df_to_public_gold(final_merged_df, table_name="gold"):
    """
    Memuat DataFrame pandas ke tabel PostgreSQL (Schema Public).
    Metode: Drop table -> Create table -> COPY (Fast Load).
    """
    if final_merged_df is None or final_merged_df.empty:
        print("⚠️ DataFrame kosong. Skipping load to DB.")
        return

    # 1) Normalize column names -> valid Postgres identifiers
    df = final_merged_df.copy()
    df.columns = (
        df.columns.astype(str)
        .str.strip()
        .str.lower()
        .str.replace(" ", "_", regex=False)
        .str.replace("/", "_", regex=False)
        .str.replace("-", "_", regex=False)
        .str.replace("°", "deg", regex=False)
    )

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            # 2) Ensure we are using public schema
            cur.execute("SET search_path TO public;")

            # 3) Recreate table using inferred types
            pg_type = {}
            for c in df.columns:
                dt = df[c].dtype
                if str(dt).startswith("int"):
                    pg_type[c] = "BIGINT"
                elif str(dt).startswith("float"):
                    pg_type[c] = "DOUBLE PRECISION"
                elif str(dt).startswith("bool"):
                    pg_type[c] = "BOOLEAN"
                elif "datetime" in str(dt):
                    pg_type[c] = "TIMESTAMP"
                else:
                    pg_type[c] = "TEXT"

            cols_ddl = ",\n  ".join([f'"{c}" {pg_type[c]}' for c in df.columns])

            # DROP + CREATE table in public
            cur.execute(f'DROP TABLE IF EXISTS public."{table_name}" CASCADE;')
            cur.execute(f'CREATE TABLE public."{table_name}" (\n  {cols_ddl}\n);')

            # 4) Fast load via COPY
            buf = io.StringIO()
            df.to_csv(buf, index=False, header=False)
            buf.seek(0)

            # Quote column names to handle reserved keywords if any
            cols_list = ", ".join([f'"{c}"' for c in df.columns])
            
            cur.copy_expert(
                f'COPY public."{table_name}" ({cols_list}) FROM STDIN WITH (FORMAT CSV)',
                buf
            )

        conn.commit()

        # 5) Validate rowcount
        with conn.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) FROM public."{table_name}";')
            n = cur.fetchone()[0]

        print(f"✅ [DB LOAD SUCCESS] Loaded {n:,} rows into public.{table_name}")

    except Exception as e:
        conn.rollback()
        print(f"❌ [DB LOAD FAILED] Error: {e}")
        raise e
    finally:
        conn.close()

# # Blok ini hanya berjalan jika file ini dijalankan langsung (bukan di-import)
# if __name__ == "__main__":
#     print("Testing connection...")
#     test_connection()
import pandas as pd
import gdown
import os
import time

def extract_etl_source1():
    """
    Mengunduh Flight.csv dari Google Drive menggunakan gdown 
    dan mengembalikannya sebagai DataFrame.
    """
    print("   [EXTRACT] Memulai proses unduh Data Flight (Source 1)...")

    # ---------------------------------------------------------
    # KONFIGURASI GOOGLE DRIVE
    # ---------------------------------------------------------
    file_id = '11aQ3Y7Nk44eZjUdlLkEP_UoXC7RgITLM' 
    
    # URL format gdown
    url = f'https://drive.google.com/uc?id={file_id}'
    output_file = 'Flight.csv'
    
    start_time = time.time()

    try:
        # 1. Cek apakah file perlu didownload
        if not os.path.exists(output_file):
            print(f"   [GDOWN] Mengunduh {output_file}...")
            gdown.download(url, output_file, quiet=False)
        else:
            print(f"   [INFO] File {output_file} sudah ada di lokal.")

        # 2. Baca file ke dalam DataFrame (Lakukan ini SEBELUM mengakses variabel df)
        if os.path.exists(output_file):
            df = pd.read_csv(output_file)
            
            # 3. Hitung Statistik & Waktu
            end_time = time.time()
            execution_time = end_time - start_time
            num_rows, num_cols = df.shape

            print(f"   [SUCCESS] Status: Berhasil Memuat Data ke Memori")
            print(f"   -> Number of Rows: {num_rows}")
            print(f"   -> Number of Columns: {num_cols}")
            print(f"   -> Extraction Time: {execution_time:.4f} seconds")
            print(f"   --- Extraction Completed ---")
            
            return df
        else:
            print("   [ERROR] File tidak ditemukan (gagal download).")
            return None

    except Exception as e:
        print(f"   [ERROR] Terjadi kesalahan saat ekstraksi Source 1: {e}")
        return None
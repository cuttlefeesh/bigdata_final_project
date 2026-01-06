import pandas as pd
import gdown
import os
import time

def extract_etl_source2():
    """
    Mengunduh Flight.csv dari Google Drive menggunakan gdown 
    dan mengembalikannya sebagai DataFrame.
    """
    print("   [EXTRACT] Memulai proses unduh Data Flight (Source 1)...")

    # ---------------------------------------------------------
    # KONFIGURASI GOOGLE DRIVE
    # ---------------------------------------------------------
    # Masukkan File ID dari link Google Drive Anda di sini
    # Contoh Link: https://drive.google.com/file/d/1A2b3C.../view
    # ID-nya adalah bagian: 1A2b3C...
    # ---------------------------------------------------------
    file_id = '1mvXcBMi_Lkb6TE7P8O0Xc1_AGyb1gOqL' 
    
    # URL format gdown
    url = f'https://drive.google.com/uc?id={file_id}'
    output_file = 'Weather.csv'
    start_time = time.time()

    try:
        # Cek apakah file sudah ada agar tidak download berulang (opsional)
        # Jika ingin selalu download ulang (fresh), hapus blok 'if' ini.
        if not os.path.exists(output_file):
            print(f"   [GDOWN] Mengunduh {output_file}...")
            gdown.download(url, output_file, quiet=False)
        else:
            print(f"   [INFO] File {output_file} sudah ada. Menggunakan file lokal.")

        # Membaca CSV ke Pandas DataFrame
        if os.path.exists(output_file):
            df = pd.read_csv(output_file, sep=';')
            print(f"   [SUCCESS] Data Flight berhasil dimuat: {df.shape[0]} baris, {df.shape[1]} kolom.")
            
            end_time = time.time()
            execution_time = end_time - start_time

            # Log Information
            num_rows, num_cols = df.shape
            

            print(f"Status: Berhasil Memuat Data ke Memori")
            print(f"Number of Rows: {num_rows}")
            print(f"Number of Columns: {num_cols}")
            print(f"Extraction Time: {execution_time:.4f} seconds")
            print(f"--- Extraction Completed ---")
            
            return df
        else:
            print("   [ERROR] File tidak ditemukan setelah proses download.")
            return None

    except Exception as e:
        print(f"   [ERROR] Terjadi kesalahan saat ekstraksi Source 1: {e}")
        return None


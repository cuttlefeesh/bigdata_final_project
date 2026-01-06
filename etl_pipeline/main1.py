import pandas as pd
import extraction_source1  # Modul untuk Flight.csv
import extraction_source2  # Modul untuk Weather.csv
import transformation   # Modul untuk Transformasi Data
import data_validation  # Modul untuk Validasi Data
import load_warehouse as db_connection   # [BARU] Modul untuk Koneksi Database

def main():
    print("==========================================")
    print("      STARTING BIG DATA ETL PIPELINE      ")
    print("==========================================\n")
    
    # ---------------------------------------------------------
    # TAHAP 1: EXTRACTION
    # ---------------------------------------------------------
    print(">>> PHASE 1: EXTRACTION")
    
    # 1. Extraction Source 1 (Flight Data)
    flight_df = extraction_source1.extract_etl_source1()
    if flight_df is not None:
        print("[SUCCESS] Data Flight berhasil dimuat.")
    else:
        print("[FAILED] Gagal memuat Data Flight.")
        return

    # 2. Extraction Source 2 (Weather Data)
    weather_df = extraction_source2.extract_etl_source2()
    if weather_df is not None:
        print("[SUCCESS] Data Weather berhasil dimuat.")
    else:
        print("[FAILED] Gagal memuat Data Weather.")
        return


    # ---------------------------------------------------------
    # TAHAP 2: TRANSFORMATION (CLEANING & FILTERING)
    # ---------------------------------------------------------
    print("\n>>> PHASE 2: TRANSFORMATION")
    
    # 1. Filter Flight Data (Top 10 Cities)
    flight_df_filtered = transformation.filter_data(flight_df)
    
    # 2. Clean Flight Data (Nulls & Inconsistencies)
    flight_df_cleaned = transformation.clean_data(flight_df_filtered)
    
    # 3. Check Duplicates & Outliers (Flight & Weather)
    transformation.check_duplicate_outliers(flight_df_cleaned, weather_df)


    # ---------------------------------------------------------
    # TAHAP 3: STANDARDIZATION
    # ---------------------------------------------------------
    print("\n>>> PHASE 3: STANDARDIZATION")
    
    # Standarisasi (Lowercase kolom, Encoding Kota/Airline, Format Tanggal)
    flight_df_std, weather_df_std = transformation.standarisasi(flight_df_cleaned, weather_df)


    # ---------------------------------------------------------
    # TAHAP 4: MERGING
    # ---------------------------------------------------------
    print("\n>>> PHASE 4: MERGING DATASETS")
    
    # Menggabungkan Flight dan Weather
    df_merged = transformation.merge_data(flight_df_std, weather_df_std)
    
    print(f"Hasil Merge: {df_merged.shape[0]} baris, {df_merged.shape[1]} kolom")


    # ---------------------------------------------------------
    # TAHAP 5: DATA ENRICHMENT
    # ---------------------------------------------------------
    print("\n>>> PHASE 5: FEATURE ENGINEERING")
    
    # Menambah kolom baru (selisih suhu, tekanan, dll)
    df_final = transformation.data_enrichment(df_merged)


    # ---------------------------------------------------------
    # TAHAP 6: DATA VALIDATION
    # ---------------------------------------------------------
    print("\n>>> PHASE 6: DATA VALIDATION")

    # Menggunakan 'df_final' agar kolom hasil enrichment ikut tervalidasi.
    data_validation.validate_data(flight_df_cleaned, df_final, weather_df_std)


    # # ---------------------------------------------------------
    # # TAHAP 7: LOADING TO DATABASE
    # # ---------------------------------------------------------
    # print("\n>>> PHASE 7: LOADING TO DATABASE")
    
    # # Memanggil fungsi loading dari db_connection.py
    # # Kita beri nama tabel tujuan 'gold_flight_weather'
    # try:
    #     db_connection.load_final_merged_df_to_public_gold(df_final, table_name="gold_flight_weather")
    # except Exception as e:
    #     print(f"[CRITICAL ERROR] Gagal menyimpan ke Database: {e}")
    #     # Opsional: return atau exit jika loading adalah langkah krusial


    # ---------------------------------------------------------
    # FINAL OUTPUT
    # ---------------------------------------------------------
    print("\n==========================================")
    print("           PIPELINE COMPLETED             ")
    print("==========================================")
    
    print("\n--- Final Data Preview (5 Baris Teratas) ---")
    pd.set_option('display.max_columns', None)
    print(df_final.head())
    
    print("\n--- Info Dataset Akhir ---")
    print(df_final.info())

if __name__ == "__main__":
    main()
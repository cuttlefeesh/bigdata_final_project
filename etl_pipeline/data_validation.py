import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

def validate_data(df1_filtered, final_merged_df, df_weather_std):
    """
    Fungsi untuk melakukan validasi data (Quality Assurance).
    Struktur pengecekan disamakan dengan referensi:
    1. Uniqueness Check
    2. Null Check
    3. Range Check
    4. Data Type Check
    5. Referential Integrity Check
    6. Distribusi Data
    """
    print("--- Memulai Proses Filtering ---")
    
    # ---------------------------------------------------------
    # 1. Uniqueness Check
    # ---------------------------------------------------------
    print("\n[1/6] Uniqueness Check")
    rows_before = len(df1_filtered)
    rows_after = len(final_merged_df)

    print(f"   -> Rows Before Merge: {rows_before}")
    print(f"   -> Rows After Merge : {rows_after}")

    if rows_after > rows_before:
        print(f"   ❌ WARNING: Terjadi duplikasi! Ada {rows_after - rows_before} baris tambahan.")
        print("      Saran: Cek duplikasi di dataset weather pada kolom kunci (date, location, time).")
        # Cek duplikasi di weather source (jika memungkinkan)
        if df_weather_std is not None:
             # Sesuaikan nama kolom dengan output transformation.py (date, origin_cities_encode/location_id, time_hour_minute)
             # Di sini kita pakai nama umum dari df_weather_std di main.py
             possible_keys = ['date', 'time_hour_minute', 'location_id'] # Sesuaikan dengan transformation.py standarisasi
             available_keys = [k for k in possible_keys if k in df_weather_std.columns]
             
             if available_keys:
                 dupes = df_weather_std[df_weather_std.duplicated(subset=available_keys, keep=False)]
                 if not dupes.empty:
                     print(f"      -> Ditemukan {len(dupes)} baris duplikat di data Weather!")
    elif rows_after < rows_before:
        print(f"   ❌ ERROR: Data berkurang! (Seharusnya tidak terjadi pada Left Join)")
    else:
        print("   ✅ PASS: Jumlah baris konsisten (One-to-One / Many-to-One relationship aman).")

    
    # ---------------------------------------------------------
    # 2. Null Check (Focus on Weather Data)
    # ---------------------------------------------------------
    print("\n[2/6] Null Check (Missing Weather Data)")
    # Fokus pada kolom cuaca baru (yang berawalan 'origin_' dan 'dest_')
    new_weather_cols = [col for col in final_merged_df.columns if col.startswith('origin_') or col.startswith('dest_')]
    
    # Hapus kolom non-cuaca yang mungkin kebetulan berawalan origin/dest (misal origin_city, dest_city)
    weather_cols_clean = [c for c in new_weather_cols if c not in ['origin', 'dest', 'origin_city', 'dest_city', 'origin_cities_encode', 'dest_cities_encode']]

    if weather_cols_clean:
        null_counts = final_merged_df[weather_cols_clean].isnull().sum()
        null_pct = (null_counts / len(final_merged_df)) * 100
        
        # Tampilkan jika ada missing value
        if null_counts.sum() > 0:
            print("   -> Detail Missing Values per Column:")
            # Tampilkan hanya yang > 0 agar terminal rapi
            missing_df = pd.DataFrame({'Missing Count': null_counts, 'Percentage (%)': null_pct})
            print(missing_df[missing_df['Missing Count'] > 0])
        
        # Threshold warning (misal 5%)
        if null_pct.max() > 5:
            print("   ⚠️ WARNING: Lebih dari 5% data penerbangan tidak memiliki data cuaca.")
        else:
            print("   ✅ PASS: Missing data weather masih dalam batas toleransi (< 5%).")
    else:
        print("   ⚠️ SKIP: Tidak ditemukan kolom cuaca hasil merge.")


    # ---------------------------------------------------------
    # 3. Range Check
    # ---------------------------------------------------------
    print("\n[3/6] Range Check (Business Logic)")

    # Definisikan batasan logis (Nama kolom disesuaikan dengan output transformation.py)
    constraints = {
        'origin_temperature_2m_c': (-50, 60),    # Suhu bumi ekstrem tapi valid
        'dest_temperature_2m_c': (-50, 60),
        'origin_precipitation_mm': (0, 2000),    # Hujan tidak negatif
        'dest_precipitation_mm': (0, 2000),
        'origin_wind_speed_10m_kmh': (0, 300),   # Angin (perhatikan suffix kmh)
        'dest_wind_speed_10m_kmh': (0, 300),
        'origin_cloud_cover_percent': (0, 100),
        'dest_cloud_cover_percent': (0, 100),
        'origin_surface_pressure_hpa': (800, 1100), # Tekanan udara (hpa lowercase)
        'dest_surface_pressure_hpa': (800, 1100)
    }

    range_issues = False
    for col, (min_val, max_val) in constraints.items():
        if col in final_merged_df.columns:
            outliers = final_merged_df[(final_merged_df[col] < min_val) | (final_merged_df[col] > max_val)]
            if not outliers.empty:
                print(f"   ❌ FAIL: Kolom '{col}' memiliki {len(outliers)} nilai di luar range ({min_val} - {max_val}).")
                range_issues = True
    
    if not range_issues:
        print("   ✅ PASS: Semua kolom parameter cuaca berada dalam range yang wajar.")

    
    # ---------------------------------------------------------
    # 4. Data Type Check
    # ---------------------------------------------------------
    print("\n[4/6] Data Type Check")
    
    # Mapping tipe data yang diharapkan
    # (Disesuaikan dengan nama kolom hasil standarisasi transformation.py)
    expected_dtypes = {
        'fl_date': 'int64',
        'airline': 'object',
        'airline_code': 'object',
        'dot_code': 'int64',
        'fl_number': 'int64',
        'origin': 'object',
        'origin_city': 'object',
        'dest': 'object',
        'dest_city': 'object',
        'crs_dep_time': 'int64',
        'crs_dep_time_rounded': 'int64',
        'dep_time': 'int64',
        'dep_delay': 'int64',
        'taxi_out': 'int64',
        'wheels_off': 'int64',
        'wheels_on': 'int64',
        'taxi_in': 'int64',
        'crs_arr_time': 'int64',
        'crs_arr_time_rounded': 'int64',
        'arr_time': 'int64',
        'arr_delay': 'int64',
        'cancelled': 'int64',
        'diverted': 'int64',
        'crs_elapsed_time': 'int64',
        'elapsed_time': 'int64',
        'air_time': 'int64',
        'distance': 'int64',
        'delay_due_carrier': 'int64',
        'delay_due_weather': 'int64',
        'delay_due_nas': 'int64',
        'delay_due_security': 'int64',
        'delay_due_late_aircraft': 'int64',
        'airlines_encode': 'int64',
        'airline_code_encode': 'int64',
        'origin_encode': 'int64',
        'origin_cities_encode': 'int64',
        'dest_encode': 'int64',
        'dest_cities_encode': 'int64',
        'origin_time': 'object',
        'origin_temperature_2m_c': 'float64',
        'origin_precipitation_mm': 'float64',
        'origin_rain_mm': 'float64',
        'origin_snowfall_cm': 'float64',
        'origin_weather_code_wmo_code': 'int64',
        'origin_surface_pressure_hPa': 'float64',
        'origin_cloud_cover_percent': 'int64',
        'origin_cloud_cover_low_percent': 'int64',
        'origin_wind_speed_10m_km/h': 'float64',
        'origin_wind_speed_100m_km/h': 'float64',
        'origin_wind_direction_10m_°': 'int64',
        'origin_wind_direction_100m_°': 'int64',
        'origin_wind_gusts_10m_km/h': 'float64',
        'dest_time': 'object',
        'dest_temperature_2m_c': 'float64',
        'dest_precipitation_mm': 'float64',
        'dest_rain_mm': 'float64',
        'dest_snowfall_cm': 'float64',
        'dest_weather_code_wmo_code': 'int64',
        'dest_surface_pressure_hPa': 'float64',
        'dest_cloud_cover_percent': 'int64',
        'dest_cloud_cover_low_percent': 'int64',
        'dest_wind_speed_10m_km/h': 'float64',
        'dest_wind_speed_100m_km/h': 'float64',
        'dest_wind_direction_10m_°': 'int64',
        'dest_wind_direction_100m_°': 'int64',
        'dest_wind_gusts_10m_km/h': 'float64',
        'temp_2m_c_diff': 'float64',
        'surface_pressure_hPa_diff': 'float64',
        'wind_speed_10m_km_h_diff': 'float64',
        'wind_speed_100m_km_h_diff': 'float64',
        'dest_cloud_cover_diff': 'int64'
    }


    inconsistencies = False
    for col, expected_str in expected_dtypes.items():
        if col in final_merged_df.columns:
            # Cek apakah tipe data mengandung string yang diharapkan (misal 'int' ada di 'int64')
            curr_type = str(final_merged_df[col].dtype)
            if expected_str not in curr_type:
                # Toleransi float vs int jika data bersih
                if not (expected_str == 'int' and 'float' in curr_type) and not (expected_str == 'float' and 'int' in curr_type):
                    print(f"   ❌ FAIL: Kolom '{col}' tipe datanya '{curr_type}', diharapkan mengandung '{expected_str}'.")
                    inconsistencies = True
        # Tidak print warning jika kolom tidak ada, agar tidak spam
    
    if not inconsistencies:
        print("   ✅ PASS: Tipe data kolom kunci konsisten.")

    # Cek Mixed Types pada Object Columns
    object_cols = final_merged_df.select_dtypes(include='object').columns
    if not object_cols.empty:
        mixed_found = False
        for col in object_cols:
            if col not in ['airline', 'origin', 'dest', 'origin_city', 'dest_city']:
                try:
                    # Sample checking
                    unique_types = final_merged_df[col].dropna().map(type).unique()
                    if len(unique_types) > 1:
                        print(f"   ⚠️ WARNING: Kolom '{col}' memiliki mixed types: {unique_types}")
                        mixed_found = True
                except:
                    pass
        if not mixed_found:
            print("   ✅ PASS: Tidak ada mixed types berbahaya pada kolom object.")


    # ---------------------------------------------------------
    # 5. Referential Integrity Check
    # ---------------------------------------------------------
    print("\n[5/6] Referential Integrity Check")
    # Cek apakah ada baris yang gagal mendapat data cuaca (semua kolom cuaca Null)
    weather_cols_check = [c for c in final_merged_df.columns if 'temperature' in c and 'origin' in c]
    
    if weather_cols_check:
        missing_integrity = final_merged_df[weather_cols_check[0]].isnull().sum()
        if missing_integrity == 0:
             print("   ✅ PASS: Integritas terjaga. Semua penerbangan sukses di-join dengan data cuaca.")
        else:
             print(f"   ❌ FAIL: Ada {missing_integrity} penerbangan yang tidak mendapatkan data cuaca (Join mismatch).")
    else:
        print("   ⚠️ SKIP: Kolom indikator cuaca tidak ditemukan.")

    
    # ---------------------------------------------------------
    # 6. Distribusi Data (Visualization)
    # ---------------------------------------------------------
    print("\n[6/6] Distribusi Data (Visualization)")
    
    columns_to_visualize = [
        'dep_delay',
        'arr_delay',
        'origin_temperature_2m_c',
        'origin_precipitation_mm',
        'origin_rain_mm',
        'origin_snowfall_cm',
        'origin_weather_code_wmo_code',
        'origin_surface_pressure_hPa',
        'origin_cloud_cover_percent',
        'origin_cloud_cover_low_percent',
        'origin_wind_speed_10m_km/h',
        'origin_wind_speed_100m_km/h',
        'origin_wind_direction_10m_degree',
        'origin_wind_direction_100m_degree',
        'origin_wind_gusts_10m_km/h',
        'dest_temperature_2m_c',
        'dest_precipitation_mm',
        'dest_rain_mm',
        'dest_snowfall_cm',
        'dest_weather_code_wmo_code',
        'dest_surface_pressure_hPa',
        'dest_cloud_cover_percent',
        'dest_cloud_cover_low_percent',
        'dest_wind_speed_10m_km/h',
        'dest_wind_speed_100m_km/h',
        'dest_wind_direction_10m_degree',
        'dest_wind_direction_100m_degree',
        'dest_wind_gusts_10m_km/h',
        'temp_2m_c_diff',
        'surface_pressure_hPa_diff',
        'wind_speed_10m_km_h_diff',
        'wind_speed_100m_km_h_diff',
        'dest_cloud_cover_diff'
    ]
    # Filter hanya yang ada
    existing_plot_cols = [c for c in columns_to_visualize if c in final_merged_df.columns]

    if existing_plot_cols:
        print(f"   -> Generating plots for: {existing_plot_cols}...")
        print("   -> (Jendela grafik akan muncul. Tutup untuk menyelesaikan program.)")
        
        try:
            n_cols = 3
            n_rows = (len(existing_plot_cols) + n_cols - 1) // n_cols
            plt.figure(figsize=(15, 4 * n_rows))
            
            for i, col in enumerate(existing_plot_cols):
                plt.subplot(n_rows, n_cols, i + 1)
                sns.histplot(final_merged_df[col], kde=True, bins=30)
                plt.title(col)
            
            plt.tight_layout()
            plt.show()
            print("   ✅ PASS: Visualisasi berhasil.")
        except Exception as e:
            print(f"   ❌ FAIL: Gagal visualisasi ({e})")
    else:
        print("   ⚠️ SKIP: Tidak ada kolom numerik untuk divisualisasikan.")

    print("--- Memulai Proses Filtering ---")
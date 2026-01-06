import pandas as pd
import numpy as np
from sklearn.preprocessing import LabelEncoder
from sklearn.preprocessing import OrdinalEncoder

def filter_data(df1):
    """
    Melakukan filtering untuk mengambil data penerbangan dari dan ke
    Top 10 kota (Origin & Destination).
    """
    print("\n--- Memulai Proses Filtering ---")
    initial_rows = len(df1)
    
    # Mendapatkan Top 10 Kota Asal
    top_10_origin_cities = df1['ORIGIN_CITY'].value_counts().head(10).index.tolist()
    print(f"Top 10 Origin Cities: {top_10_origin_cities}")
    
    # Mendapatkan Top 10 Kota Tujuan
    top_10_dest_cities = df1['DEST_CITY'].value_counts().head(10).index.tolist()
    print(f"Top 10 Destination Cities: {top_10_dest_cities}")
    
    # Filter dataset
    df1_filtered = df1[df1['ORIGIN_CITY'].isin(top_10_origin_cities) & 
                     df1['DEST_CITY'].isin(top_10_dest_cities)]

    print(f"\nData setelah filtering top 10 kota. Baris awal: {initial_rows}, Baris akhir: {len(df1_filtered)}\n\n")
    return df1_filtered


def clean_data(df1_filtered):
    """
    Melakukan transformasi dan pembersihan data (imputasi null, drop kolom, handling inkonsistensi)
    sesuai dengan logika di notebook.
    """
    print("--- Memulai Proses Data Cleaning ---")
    
    # 1. Hapus Kolom Tidak Perlu 
    print("\n===== Drop Kolom yang Tidak Diperlukan =====")
    if 'AIRLINE_DOT' in df1_filtered.columns:
        df1_filtered = df1_filtered.drop('AIRLINE_DOT', axis=1)
        print("\nColumn 'AIRLINE_DOT' has been dropped.\n\n")
    else:
        print("\nColumn 'AIRLINE_DOT' does not exist in the DataFrame.\n\n")

    if 'CANCELLATION_CODE' in df1_filtered.columns:
        df1_filtered = df1_filtered.drop('CANCELLATION_CODE', axis=1)
        print("\nColumn 'CANCELLATION_CODE' has been dropped.\n\n")
    else:
        print("\nColumn 'CANCELLATION_CODE' does not exist in the DataFrame.\n\n")



    # 2. Imputasi Missing Value pada Kolom Delay
    # Mengisi NaN dengan 0 karena jika tidak ada info, diasumsikan tidak ada delay spesifik
    print("\n===== Imputasi Missing Value pada Kolom Delay =====")
    delay_columns = [
        'DELAY_DUE_CARRIER',
        'DELAY_DUE_WEATHER',
        'DELAY_DUE_NAS',
        'DELAY_DUE_SECURITY',
        'DELAY_DUE_LATE_AIRCRAFT'
    ]
    for col in delay_columns:
        if col in df1_filtered.columns:
            df1_filtered[col] = df1_filtered[col].fillna(0)
    print("\nImputasi kolom DELAY (Carrier, Weather, NAS, Security, Late Aircraft) selesai.\n\n")

    # 3. Menghapus Baris Data yang Inkonsisten
    # Menghapus data yang statusnya TIDAK CANCELLED, tapi kolom waktunya kosong (NaN)
    print("\n===== Hapus Baris Data yang Inkonsisten =====")
    missing_time_columns = [
        'DEP_TIME', 'DEP_DELAY', 'TAXI_OUT', 'WHEELS_OFF', 
        'WHEELS_ON', 'TAXI_IN', 'ARR_TIME', 'ARR_DELAY', 
        'AIR_TIME', 'ELAPSED_TIME'
    ]
    
    # Cek kolom waktu yang ada di dataframe saat ini
    valid_time_cols = [col for col in missing_time_columns if col in df1_filtered.columns]
    
    rows_before = len(df1_filtered)
    
    # Ambil data yang tidak dibatalkan (CANCELLED == 0)
    not_cancelled_flights = df1_filtered[df1_filtered['CANCELLED'] == 0]
    
    # Cari baris yang tidak cancel tapi kolom waktunya ada yang null
    inconsistent_indices = not_cancelled_flights[not_cancelled_flights[valid_time_cols].isnull().any(axis=1)].index
    
    # Drop baris tersebut
    if not inconsistent_indices.empty:
        df1_filtered = df1_filtered.drop(inconsistent_indices)
        print(f"Dihapus {len(inconsistent_indices)} baris inkonsisten (Tidak cancel tapi waktu kosong).")
    
    print(f"Data Cleaning Selesai. Hasil: {df1_filtered.shape[0]} baris, {df1_filtered.shape[1]} kolom\n\n")
    
    return df1_filtered


def check_duplicate_outliers (df1_filtered, df2):
    print("\n--- Memulai Pengecekan Duplikat & Outlier ---")
    # 1. Cek Duplikat
    duplicate_count = df1_filtered.duplicated().sum()
    print("\n===== Cek Duplikat di Flight.csv =====")
    if duplicate_count > 0:
        print(f"Ditemukan {duplicate_count} data duplikat. Sedang menghapus...")
        df1_filtered = df1_filtered.drop_duplicates()
        print("Data duplikat berhasil dihapus.\n\n")
    else:
        print("Tidak ada data duplikat.\n\n")

    duplicate2_count = df2.duplicated().sum()
    print("\n===== Cek Duplikat di Weather.csv =====")
    if duplicate2_count > 0:
        print(f"Ditemukan {duplicate2_count} data duplikat. Sedang menghapus...")
        df2 = df2.drop_duplicates()
        print("Data duplikat berhasil dihapus.\n\n")
    else:
        print("Tidak ada data duplikat.\n\n")

    # 2. Cek Outlier pada Kolom Numerik di Flight.csv
    # Tidak dilakukan pembersihan outliers karena mungkin terdapat insight penting yang bisa diambil dari outliers tersebut
    print("\n===== Cek Outlier di Flight.csv =====")
    numeric_cols = df1_filtered.select_dtypes(include=['int64', 'float64']).columns
    columns_with_outliers = []

    for col in numeric_cols:
        Q1 = df1_filtered[col].quantile(0.25)
        Q3 = df1_filtered[col].quantile(0.75)
        IQR = Q3 - Q1

        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR

        outliers = df1_filtered[(df1_filtered[col] < lower_bound) | (df1_filtered[col] > upper_bound)]

        if not outliers.empty:
            print(f"\nColumn '{col}': {len(outliers)} outliers found.")
            columns_with_outliers.append(col)
        else:
            print(f"\nColumn '{col}': No outliers found.")


def standarisasi (df1_filtered, df2) :
    """
    Melakukan standarisasi data, meliputi lowercase nama kolom, encoding pada kolom kategorikal, dan standarisasi format datetime
    """

    print("\n--- Memulai Proses Standarisasi Data ---")
    
    # 1. Lowercase Nama Kolom
    print("\n===== Lowercase Nama Kolom =====")
    df1_filtered.columns = [col.lower() for col in df1_filtered.columns]
    print("Standarisasi nama kolom ke lowercase selesai.\n")
    print(f"{df1_filtered.head()}\n\n")
    
    # 2. Encoding Kolom Kategorikal di df1_filtered
    print("\n===== Encoding Kolom Kategorikal =====")
    # 2.1 Label Encoder untuk kolom yang memiliki kategori yang bukan tingkatan
    le = LabelEncoder()
    le_cols = ['airline', 'airline_code', 'origin', 'dest']
    
    for col in le_cols:
            if col in df1_filtered.columns:
                # Tentukan nama kolom baru (misal: airline -> airline_encode)
                new_col_name = f"{col}_encode"
                
                # Lakukan fit_transform dan simpan ke KOLOM BARU
                df1_filtered[new_col_name] = le.fit_transform(df1_filtered[col])

    # 2.2 Ordinal Encoder untuk kolom yang memiliki tingkatan (ORIGIN_CITY, DEST_CITY)
    # Ordinal Encoder diterapkan di kolom tersebut untuk menyesuaikan dengan id lokasi di dataset Weather.csv untuk memudahkan saat merge data
    oe = OrdinalEncoder(categories = [
        ['Chicago, IL',
        'Atlanta, GA',
        'Dallas/Fort Worth, TX',
        'Denver, CO',
        'New York, NY',
        'Charlotte, NC',
        'Houston, TX',
        'Los Angeles, CA',
        'Washington, DC',
        'Phoenix, AZ']
        ])
    
    df1_filtered['origin_cities_encode'] = oe.fit_transform(df1_filtered[['origin_city']])
    df1_filtered['dest_cities_encode'] = oe.fit_transform(df1_filtered[['dest_city']])
    df1_filtered['origin_cities_encode'] = df1_filtered['origin_cities_encode'].astype(int)
    df1_filtered['dest_cities_encode'] = df1_filtered['dest_cities_encode'].astype(int)

    print("Proses Encoding selesai\n")
    print(f"{df1_filtered.head()}\n\n")

    # 3. Standarisasi Format Datetime
    # Menyamakan format datetime di kedua dataframe dengan menghilangkan tanda -
    print("\n===== Memulai Proses Standarisasi Format Datetime =====")

    # 3.1 Flight.csv (df1_filtered)
    df1_filtered['fl_date'] = df1_filtered['fl_date'].str.replace('-', '').astype(int)

    print("Data type of 'fl_date' column after transformation:")
    print(df1_filtered['fl_date'].dtype)
    print("First 5 rows of 'fl_date' after transformation:")
    print(df1_filtered['fl_date'].head())


    # 3.2 Weather.csv (df2)
    # Extract date and time parts
    df2['date'] = df2['time'].apply(lambda x: x.split('T')[0].replace('-', '')).astype(int)
    df2['time_hour_minute'] = df2['time'].apply(lambda x: x.split('T')[1].replace(':', '')).astype(int)

    print("First 5 rows of df2 with new 'date' and 'time_hour_minute' columns:")
    print(df2[['time', 'date', 'time_hour_minute']].head())

    print("\nData types of new columns:")
    print(df2[['date', 'time_hour_minute']].dtypes)

    print("\nProses Standarisasi Datetime selesai\n")
    print(f"{df1_filtered.head()}\n\n")



    # 4. Konsistensi Tipe Data
    print("\n===== Memulai Proses Konsistensi Tipe Data =====")
    time_related_nullable_columns = [
    'dep_time', 'dep_delay', 'taxi_out', 'wheels_off', 'wheels_on',
    'taxi_in', 'arr_time', 'arr_delay', 'elapsed_time', 'air_time'
    ]

    # Filter for rows where CANCELLED is 1
    cancelled_flights_mask = df1_filtered['cancelled'] == 1

    # Fill NaN values in specified time columns with 0 for cancelled flights
    for col in time_related_nullable_columns:
        df1_filtered.loc[cancelled_flights_mask, col] = df1_filtered.loc[cancelled_flights_mask, col].fillna(0)

    float_cols_to_int = [
    'dep_time', 'dep_delay', 'taxi_out', 'wheels_off', 'wheels_on',
    'taxi_in', 'arr_time', 'arr_delay', 'cancelled', 'diverted',
    'crs_elapsed_time', 'elapsed_time', 'air_time', 'distance',
    'delay_due_carrier', 'delay_due_weather', 'delay_due_nas',
    'delay_due_security', 'delay_due_late_aircraft'
    ]

    for col in float_cols_to_int:
        df1_filtered[col] = df1_filtered[col].astype(int)

    print("\n--- Proses Standarisasi Data Selesai---")
    print(f"\n--- Hasil : {df1_filtered.shape[0]} baris, {df1_filtered.shape[1]} kolom---")
    print(f"{df1_filtered.head()}\n\n")
    

    return df1_filtered, df2


def merge_data (df1_filtered, df2):
    """
    Pada bagian ini akan dilakukan tahap penggabungan 2 df menjadi satu.
    """
    print("\n--- Memulai Proses Merge Flight & Weather ---")
    
    # 1. Menyamakan Time
    # Round down crs_dep_time to the nearest hundred (e.g., 1151 becomes 1100)
    # The columns are already integer type based on previous steps.
    df1_filtered['crs_dep_time_rounded'] = (df1_filtered['crs_dep_time'] // 100 * 100).astype(int)
    crs_dep_time_idx = df1_filtered.columns.get_loc('crs_dep_time')
    df1_filtered.insert(crs_dep_time_idx + 1, 'crs_dep_time_rounded', df1_filtered.pop('crs_dep_time_rounded'))

    # Round down crs_arr_time to the nearest hundred (e.g., 1151 becomes 1100)
    df1_filtered['crs_arr_time_rounded'] = (df1_filtered['crs_arr_time'] // 100 * 100).astype(int)
    crs_arr_time_idx = df1_filtered.columns.get_loc('crs_arr_time')
    df1_filtered.insert(crs_arr_time_idx + 1, 'crs_arr_time_rounded', df1_filtered.pop('crs_arr_time_rounded'))

    # 2. Merge Dataframe
    # Rename columns in df2 to match df1_filtered for merging
    df2_origin = df2.rename(columns={
        'date': 'fl_date',
        'location_id': 'origin_cities_encode',
        'time_hour_minute': 'crs_dep_time_rounded'
    })

    # Select and prefix columns from df2 for origin weather
    df2_origin_cols = {}
    for col in df2_origin.columns:
        if col not in ['fl_date', 'origin_cities_encode', 'crs_dep_time_rounded']:
            df2_origin_cols[col] = 'origin_' + col.replace(' ', '_').replace('(', '').replace(')', '').replace('°C', 'c').replace('%', 'percent').replace('(mm)', 'mm').replace('(hPa)', 'hpa').replace('(cm)', 'cm').replace('(wmo_code)', 'wmo_code').replace('(km/h)', 'kmh').replace('(_)', 'degree')

    df2_origin_processed = df2_origin.rename(columns=df2_origin_cols)

    # Ensure only relevant columns from df2_origin_processed are used for merging
    # Include merge keys and the new prefixed columns
    columns_to_keep_origin_weather = [
        'fl_date', 'origin_cities_encode', 'crs_dep_time_rounded'
    ] + list(df2_origin_cols.values())

    df2_origin_processed = df2_origin_processed[columns_to_keep_origin_weather]

    # Perform a left merge with df1_filtered for origin weather
    df_merged_full = pd.merge(
        df1_filtered,
        df2_origin_processed,
        on=['fl_date', 'origin_cities_encode', 'crs_dep_time_rounded'],
        how='left'
    )

    # Rename columns in df2 to match df1_filtered for destination merging
    df2_dest = df2.rename(columns={
        'date': 'fl_date',
        'location_id': 'dest_cities_encode',
        'time_hour_minute': 'crs_arr_time_rounded'
    })

    # Select and prefix columns from df2 for destination weather
    df2_dest_cols = {}
    for col in df2_dest.columns:
        if col not in ['fl_date', 'dest_cities_encode', 'crs_arr_time_rounded']:
            df2_dest_cols[col] = 'dest_' + col.replace(' ', '_').replace('(', '').replace(')', '').replace('°C', 'c').replace('%', 'percent').replace('(mm)', 'mm').replace('(hPa)', 'hpa').replace('(cm)', 'cm').replace('(wmo_code)', 'wmo_code').replace('(km/h)', 'kmh').replace('(_)', 'degree')

    df2_dest_processed = df2_dest.rename(columns=df2_dest_cols)

    # Ensure only relevant columns from df2_dest_processed are used for merging
    # Include merge keys and the new prefixed columns
    columns_to_keep_dest_df2 = [
        'fl_date', 'dest_cities_encode', 'crs_arr_time_rounded'
    ] + list(df2_dest_cols.values())

    df2_dest_processed = df2_dest_processed[columns_to_keep_dest_df2]

    # Perform a left merge with df_merged_full for destination df2
    final_merged_df = pd.merge(
        df_merged_full,
        df2_dest_processed,
        on=['fl_date', 'dest_cities_encode', 'crs_arr_time_rounded'],
        how='left'
    )

    print("Shape of final merged DataFrame:", final_merged_df.shape)
    print("First 5 rows of final merged DataFrame (showing relevant destination weather columns):")
    print(final_merged_df[[
        'fl_date', 'dest_city', 'dest_cities_encode', 'crs_arr_time_rounded',
        'dest_temperature_2m_c', 'dest_precipitation_mm', 'dest_rain_mm'
    ]].head())

    print("\n--- Proses Merge Selesai\n ---")
    print(f"{final_merged_df.head()}\n\n")

    return final_merged_df


def data_enrichment (final_merged_df):
    """
    Pada bagian ini akan dilakukan penambahan 5 kolom baru.
    """
    print("\n--- Memulai Proses Feature Engineering ---")

    # 1. Menambahkan Kolom 'temp_2m_c_diff'
    final_merged_df['temp_2m_c_diff'] = final_merged_df['dest_temperature_2m_c'] - final_merged_df['origin_temperature_2m_c']

    # 2. Menambahkan Kolom 'surface_pressure_hpa_diff'
    final_merged_df['surface_pressure_hPa_diff'] = final_merged_df['dest_surface_pressure_hPa'] - final_merged_df['origin_surface_pressure_hPa']

    # 3. Menambahkan Kolom 'wind_speed_10m_km_h_diff'
    final_merged_df['wind_speed_10m_km_h_diff'] = final_merged_df['dest_wind_speed_10m_km/h'] - final_merged_df['origin_wind_speed_10m_km/h']

    # 4. wind_speed_100m_km_h_diff
    final_merged_df['wind_speed_100m_km_h_diff'] = final_merged_df['dest_wind_speed_100m_km/h'] - final_merged_df['origin_wind_speed_100m_km/h']

    # 5. dest_cloud_cover_diff
    final_merged_df['dest_cloud_cover_diff'] = final_merged_df['dest_cloud_cover_percent'] - final_merged_df['dest_cloud_cover_low_percent']

    print("\n--- Proses Feature Engineering Selesai dengan penambahan Kolom temp_2m_c_diff, surface_pressure_hpa_diff, wind_speed_10m_km_h_diff, wind_speed_100m_km_h_diff, dest_cloud_cover_diff ---\n")
    print(f"{final_merged_df.head()}\n\n")

    return final_merged_df
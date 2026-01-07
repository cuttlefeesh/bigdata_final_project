-- Maskapai Paling Tepat Waktu saat Cuaca Cerah -- 

SELECT 
    da.airline, -- Diambil dari tabel dimensi
    AVG(ff.dep_delay) as baseline_delay_cerah,
    AVG(ff.taxi_out) as baseline_taxi_cerah,
    COUNT(*) as total_flight_sample
FROM fact_flights ff
JOIN dim_airline da ON ff.airline_key = da.airline_key -- Wajib join untuk mendapatkan nama airline
WHERE ff.origin_precipitation_mm = 0 
  AND ff.origin_wind_speed_10m_kmh < 10 -- Sesuaikan nama kolom dengan schema (kmh vs km_h)
  AND ff.origin_cloud_cover_percent < 20
  AND ff.origin_snowfall_cm = 0
GROUP BY da.airline
HAVING COUNT(*) > 100
ORDER BY baseline_delay_cerah ASC;
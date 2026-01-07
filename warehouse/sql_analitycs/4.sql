-- Analisis Delay dan Pembatalan berdasarkan Kode Cuaca --

SELECT 
    origin_weather_code_wmo_code,
    CASE 
        WHEN origin_weather_code_wmo_code = 0 THEN 'Cerah'
        WHEN origin_weather_code_wmo_code BETWEEN 1 AND 3 THEN 'Berawan'
        WHEN origin_weather_code_wmo_code BETWEEN 45 AND 48 THEN 'Kabut (Fog)'
        WHEN origin_weather_code_wmo_code BETWEEN 51 AND 67 THEN 'Gerimis/Hujan Ringan'
        WHEN origin_weather_code_wmo_code BETWEEN 71 AND 77 THEN 'Salju'
        WHEN origin_weather_code_wmo_code >= 95 THEN 'Badai Petir (Thunderstorm)'
        ELSE 'Lainnya'
    END AS deskripsi_cuaca,
    AVG(dep_delay) as avg_delay,
    SUM(cancelled) as total_batal
FROM fact_flights
GROUP BY 1, 2
ORDER BY avg_delay DESC;
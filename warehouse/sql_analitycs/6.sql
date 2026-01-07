-- Analisis Delay berdasarkan Suhu --

SELECT 
    CASE 
        WHEN origin_temperature_2m_c < 0 THEN 'Beku (< 0°C)'
        WHEN origin_temperature_2m_c BETWEEN 0 AND 30 THEN 'Normal (0-30°C)'
        WHEN origin_temperature_2m_c > 30 THEN 'Panas (> 30°C)'
    END AS kategori_suhu,
    AVG(dep_delay) as avg_departure_delay,
    AVG(arr_delay) as avg_arrival_delay
FROM fact_flights
GROUP BY 1
ORDER BY avg_departure_delay DESC;
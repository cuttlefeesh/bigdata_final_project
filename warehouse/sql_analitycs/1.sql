-- "Survival Rate" Penerbangan (On-Time Performance di Cuaca Ekstrem) --

SELECT 
    da.airline, -- Ambil nama maskapai dari tabel dimensi
    COUNT(*) as total_flights_saat_badai,
    
    -- Hitung jumlah penerbangan yang tetap On-Time (Delay <= 0) meski badai
    SUM(CASE WHEN ff.dep_delay <= 0 THEN 1 ELSE 0 END) as on_time_flights,
    
    -- Hitung Survival Rate (Persentase)
    ROUND((SUM(CASE WHEN ff.dep_delay <= 0 THEN 1 ELSE 0 END)::DECIMAL / COUNT(*)) * 100, 2) as survival_rate_persen

FROM fact_flights ff
JOIN dim_airline da ON ff.airline_key = da.airline_key -- Wajib Join
WHERE ff.origin_precipitation_mm > 3 
   OR ff.origin_wind_speed_10m_kmh > 35 -- Perhatikan akhiran '_kmh' (bukan '_km_h')
GROUP BY 1
HAVING COUNT(*) > 50 -- Filter sampel minimal
ORDER BY survival_rate_persen DESC;
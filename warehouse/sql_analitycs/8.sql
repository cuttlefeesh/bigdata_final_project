-- Rata-Rata Delay berdasarkan Kondisi Hujan --

SELECT 
    CASE 
        WHEN origin_precipitation_mm = 0 THEN '1. Cerah/Berawan (0 mm)'
        WHEN origin_precipitation_mm BETWEEN 0.1 AND 5 THEN '2. Hujan Ringan (0.1 - 5 mm)'
        WHEN origin_precipitation_mm > 5 THEN '3. Hujan Lebat (> 5 mm)'
    END AS kondisi_hujan,
    
    COUNT(*) as total_penerbangan,
    
    -- Rata-rata Total Delay (Semua penyebab)
    ROUND(AVG(dep_delay), 2) as rata_rata_total_delay,
    
    -- Rata-rata Delay SPESIFIK karena Cuaca
    ROUND(AVG(delay_due_weather), 2) as rata_rata_delay_karena_cuaca,
    
    -- Waktu Taxi-Out (Indikator landasan licin/antrian)
    ROUND(AVG(taxi_out), 2) as rata_rata_waktu_taxi,
    
    -- Persentase Pembatalan
    ROUND(AVG(cancelled) * 100, 2) as persentase_pembatalan

FROM fact_flights
GROUP BY 1
ORDER BY 1;
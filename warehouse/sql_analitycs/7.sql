-- Maskapai paling Terdampak Salju --

SELECT 
    da.airline, -- Mengambil nama maskapai dari tabel dimensi
    
    COUNT(*) as jumlah_penerbangan_salju,
    
    -- Rata-rata keterlambatan keberangkatan
    ROUND(AVG(ff.dep_delay), 2) as rata_rata_delay_menit,
    
    -- Waktu Taxi-Out (Indikator proses De-icing)
    ROUND(AVG(ff.taxi_out), 2) as rata_rata_waktu_taxi_menit, 
    
    -- Statistik Pembatalan
    SUM(ff.cancelled) as jumlah_batal,
    ROUND((SUM(ff.cancelled)::DECIMAL / COUNT(*)) * 100, 2) as persentase_pembatalan

FROM fact_flights ff
JOIN dim_airline da ON ff.airline_key = da.airline_key -- Join ke tabel dimensi
WHERE ff.origin_snowfall_cm > 0 -- Filter hanya saat bersalju
GROUP BY 1
HAVING COUNT(*) > 50 -- Mengabaikan maskapai dengan sampel data terlalu sedikit
ORDER BY rata_rata_delay_menit DESC;
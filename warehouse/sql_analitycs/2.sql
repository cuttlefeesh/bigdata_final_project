-- Ketahanan Bandara Besar (Hub) vs Kecil --

WITH AirportVolume AS (
    -- Kita perlu JOIN ke dimensi untuk mendapatkan nama kota
    SELECT doc.origin_city, COUNT(*) as total 
    FROM fact_flights ff
    JOIN dim_origin_city doc ON ff.origin_city_key = doc.origin_city_key
    GROUP BY 1
),
AirportType AS (
    -- Bagian ini tidak berubah logikanya
    SELECT origin_city, 
           CASE WHEN total > (SELECT PERCENTILE_CONT(0.9) WITHIN GROUP(ORDER BY total) FROM AirportVolume) 
                THEN 'Major Hub (Top 10%)' 
                ELSE 'Small/Medium Airport' 
           END as tipe_bandara
    FROM AirportVolume
)
SELECT 
    t.tipe_bandara,
    AVG(ff.dep_delay) as avg_delay_all,
    AVG(CASE WHEN ff.origin_precipitation_mm > 0 THEN ff.dep_delay END) as avg_delay_hujan,
    (AVG(CASE WHEN ff.origin_precipitation_mm > 0 THEN ff.dep_delay END) - AVG(ff.dep_delay)) as gap_dampak_cuaca
FROM fact_flights ff
-- Join ke dimensi kota dulu...
JOIN dim_origin_city doc ON ff.origin_city_key = doc.origin_city_key 
-- ...baru join ke hasil CTE kategori bandara
JOIN AirportType t ON doc.origin_city = t.origin_city
GROUP BY 1;
-- Dampak Cloud Cover terhadap Delay Kedatangan --

SELECT 
    CASE 
        WHEN dest_cloud_cover_percent < 25 THEN 'Langit Bersih (0-25%)'
        WHEN dest_cloud_cover_percent BETWEEN 25 AND 75 THEN 'Berawan Sebagian (25-75%)'
        WHEN dest_cloud_cover_percent > 75 THEN 'Mendung Total (>75%)'
    END AS kondisi_langit_tujuan,
    AVG(arr_delay) as rata_rata_delay_kedatangan
FROM fact_flights
GROUP BY 1
ORDER BY rata_rata_delay_kedatangan DESC;
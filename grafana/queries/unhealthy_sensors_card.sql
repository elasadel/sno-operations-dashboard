SELECT 
  COUNT(DISTINCT CASE WHEN t.tags NOT LIKE '%maintenance%' THEN o.sensor END) * 100.00 
  / COUNT(DISTINCT o.sensor) AS unhealthy_pct
FROM observatories o
LEFT JOIN sno_tasks t ON o.sensor = t.sensor AND t.status != 'done'
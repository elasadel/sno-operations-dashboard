SELECT 
  COUNT(DISTINCT CASE WHEN t.tags NOT LIKE '%maintenance%' THEN s.sensor END) * 100.00 
  / COUNT(DISTINCT s.sensor) AS unhealthy_pct
FROM sensors s
LEFT JOIN sno_tasks t ON s.sensor = t.sensor AND t.status != 'done'

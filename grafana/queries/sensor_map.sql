WITH tasks_enriched AS (
  SELECT 
    sensor,
    observatory_id,
    priority,
    CASE
      WHEN priority = 'urgent' THEN 1
      WHEN priority = 'high' THEN 2
      ELSE 3  
    END AS priority_number
  FROM sno_tasks
  WHERE status != 'done' AND tags NOT LIKE '%maintenance%'
)
SELECT 
  s.sensor,
  s.name,
  s.lat, 
  s.lon, 
  CASE 
    WHEN min(t.priority_number) = 1 THEN 'Urgent'
    WHEN min(t.priority_number) = 2 THEN 'High'
    WHEN min(t.priority_number) = 3 THEN 'Normal'
    ELSE 'Healthy'
  END as priority_status,
  CASE 
    WHEN s.sensor IN (${selected_sensors:sqlstring}) THEN 10
    ELSE 5
  END AS marker_size,
  CASE 
    WHEN s.sensor IN (${selected_sensors:sqlstring}) THEN s.sensor
    ELSE ''
  END AS label
FROM sensors s
LEFT JOIN tasks_enriched t ON s.sensor = t.sensor AND s.observatory_id = t.observatory_id
GROUP BY s.sensor, s.name, s.lat, s.lon

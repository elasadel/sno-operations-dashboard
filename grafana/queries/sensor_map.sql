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
  o.sensor,
  o.name,
  o.lat, 
  o.lon, 
  CASE 
    WHEN min(t.priority_number) = 1 THEN 'Urgent'
    WHEN min(t.priority_number) = 2 THEN 'High'
    WHEN min(t.priority_number) = 3 THEN 'Normal'
    ELSE 'Healthy'
  END as priority_status,
  CASE 
    WHEN o.sensor IN (${selected_sensors:sqlstring}) THEN 10
    ELSE 5
  END AS marker_size,
  CASE 
    WHEN o.sensor IN (${selected_sensors:sqlstring}) THEN o.sensor
    ELSE ''
  END AS label
FROM observatories o
LEFT JOIN tasks_enriched t ON o.sensor = t.sensor AND o.observatory_id = t.observatory_id
GROUP BY o.sensor, o.name, o.lat, o.lon;
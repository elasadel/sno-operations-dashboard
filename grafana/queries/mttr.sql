SELECT 
  AVG(actual_hours) AS mttr_actual_hours
FROM sno_tasks 
WHERE status = 'done'
  AND tags NOT LIKE '%maintenance%'
  AND actual_hours IS NOT NULL 
  AND actual_hours > 0  
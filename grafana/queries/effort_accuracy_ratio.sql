SELECT 
  ROUND(
    AVG(actual_hours) / NULLIF(AVG(estimated_hours), 0), 
    2
  ) AS effort_accuracy_ratio
FROM sno_tasks 
WHERE status = 'done'
  AND tags NOT LIKE '%maintenance%'
  AND actual_hours IS NOT NULL 
  AND estimated_hours IS NOT NULL
  AND estimated_hours > 0
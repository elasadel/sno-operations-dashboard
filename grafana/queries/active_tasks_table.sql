SELECT 
  task_name AS "Task",
  sensor AS "Sensor",
  status AS "Status",
  priority AS "Priority",
  assignee_name AS "Assignee",
  incident_type AS "Type",
  TO_CHAR(created_date, 'Mon DD, HH24:MI') AS "Opened"
FROM sno_tasks
WHERE 
  sensor IN (${selected_sensors:sqlstring})
  AND status != 'done'
  AND tags NOT LIKE '%maintenance%'
ORDER BY 
  CASE priority 
    WHEN 'urgent' THEN 1 
    WHEN 'high' THEN 2 
    ELSE 3 
  END ASC;
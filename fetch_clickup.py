import os
import requests
from pathlib import Path
from dotenv import load_dotenv
import psycopg2
from datetime import datetime
import logging

load_dotenv(Path.home() / "code/secrets" / "sno.env")

TOKEN = os.getenv('CLICKUP_API_TOKEN')
LIST_ID = os.getenv('CLICKUP_LIST_ID')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s - %(message)s',
)

logger = logging.getLogger('fetch_clickup')


def format_date(timestamp):    
    if timestamp is None or timestamp == "":
        return None
    try:
        return datetime.fromtimestamp(int(timestamp) / 1000)
    except (ValueError, TypeError):
        return None

def fetch_tasks():
    # NOTE: For this demo we assume a small ClickUp list.
    # In production, this should handle pagination (page/limit) to fetch all tasks.
    if not TOKEN or not LIST_ID:
        logger.error('Missing CLICKUP_API_TOKEN or CLICKUP_LIST_ID env vars.')
        return []

    url = f'https://api.clickup.com/api/v2/list/{LIST_ID}/task'
    headers = {'Authorization': TOKEN }
    params = {'include_closed': True}

    try:
        logger.info('Requesting tasks from ClickUp list %s', LIST_ID)
        response = requests.get(url, headers=headers, params=params, timeout=10)

        if response.status_code != 200:
            logger.error(
                'ClickUp API error. status=%s body=%s',
                response.status_code,
                response.text[:500],
            )
            return []

        tasks = response.json().get('tasks', [])
        logger.info('Fetched %d tasks from ClickUp', len(tasks))
        return tasks

    except requests.Timeout:
        logger.error('ClickUp API request timed out.')
        return []
    except requests.RequestException as e:
        logger.error('ClickUp API request failed: %s', e)
        return []    


def get_custom_field_value(task, field_name):
    fields = task.get("custom_fields", [])
    for f in fields:
        if f.get("name") == field_name:
            val = f.get("value")
            if f.get("type") == "drop_down" and val is not None:
                options = f.get("type_config", {}).get("options", [])
                for opt in options:
                    if str(opt.get("orderindex")) == str(val):
                        return opt.get("name")
            return val
    return None

def transform_task(task):    
    tags_list = [tag.get("name") for tag in task.get("tags", [])]
    tags_string = ", ".join(tags_list)

    return {
        "task_id": task.get("id"),
        "task_name": task.get("name"),
        "status": task.get("status", {}).get("status"),
        "priority": task.get("priority", {}).get("priority") if task.get("priority") else "normal",
        "assignee_name": get_custom_field_value(task, "Assignee"),        
        
        "created_date": format_date(task.get("date_created")),
        "due_date": format_date(task.get("due_date")),
        "closed_date": format_date(task.get("date_closed")),
        
        "tags": tags_string,
        "observatory_id": get_custom_field_value(task, "Observatory_ID"),
        "sensor": get_custom_field_value(task, "Sensor"),
        "incident_type": get_custom_field_value(task, "Incident_Type"),
        "estimated_hours": get_custom_field_value(task, "Estimated Hours"),
        "actual_hours": get_custom_field_value(task, "Actual Hours")
    }

def load_to_db(clean_tasks):
    conn = None
    cur = None
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            port=os.getenv("DB_PORT")
        )
        cur = conn.cursor()
        
        upsert_query = """
        INSERT INTO sno_tasks (
            task_id, task_name, status, priority, assignee_name, 
            created_date, due_date, closed_date, tags, 
            observatory_id, sensor, incident_type, estimated_hours, actual_hours
        ) VALUES (
            %(task_id)s, %(task_name)s, %(status)s, %(priority)s, %(assignee_name)s, 
            %(created_date)s, %(due_date)s, %(closed_date)s, %(tags)s, 
            %(observatory_id)s, %(sensor)s, %(incident_type)s, %(estimated_hours)s, %(actual_hours)s
        )
        ON CONFLICT (task_id) DO UPDATE SET
            status = EXCLUDED.status,
            priority = EXCLUDED.priority,
            assignee_name = EXCLUDED.assignee_name,
            due_date = EXCLUDED.due_date,
            closed_date = EXCLUDED.closed_date,
            tags = EXCLUDED.tags,
            sensor = EXCLUDED.sensor,
            incident_type = EXCLUDED.incident_type,
            estimated_hours = EXCLUDED.estimated_hours,
            actual_hours = EXCLUDED.actual_hours,
            updated_at = NOW();
        """

        logger.info('Loading %d tasks into the database...', len(clean_tasks))     
        for task in clean_tasks:
            cur.execute(upsert_query, task)
        
        conn.commit()
        logger.info('Database sync complete.')


    except Exception as e:
        if conn:
            conn.rollback()
        logger.error('Database load error: %s', e)
    

    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()


if __name__ == "__main__":
    # 1. EXTRACT
    raw_tasks = fetch_tasks()
    
    if raw_tasks:
        # 2. TRANSFORM
        cleaned_list = [transform_task(task) for task in raw_tasks]        
        # 3. LOAD
        load_to_db(cleaned_list)
    else:
        logger.info('No tasks found to process.')
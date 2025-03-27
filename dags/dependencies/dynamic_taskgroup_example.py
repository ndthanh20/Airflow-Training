from datetime import datetime

from airflow.models.dag import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy import DummyOperator

with DAG(
    dag_id="dynamic_task_group_example",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["session7"],
) as dag:
    groups = []
    for g_id in range(1, 4):
        tg_id = f"group{g_id}"
        with TaskGroup(group_id=tg_id) as tg:
            t1 = DummyOperator(task_id="task1")
            t2 = DummyOperator(task_id="task2")

            t1 >> t2

            if tg_id == "group1":
                t3 = DummyOperator(task_id="task3")
                t1 >> t3

            groups.append(tg)

    # Set dependencies between TaskGroups
    groups[0] >> groups[2]
    groups[1] >> groups[2]

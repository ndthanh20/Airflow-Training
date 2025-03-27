from datetime import datetime

from airflow.models.dag import DAG
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup

with DAG(
    dag_id="nested_task_group_example",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["session7"],    
) as dag:
    main_groups = []
    for main_group_id in range(1, 3):
        main_tg_id = f"group{main_group_id}"
        with TaskGroup(group_id=main_tg_id) as main_tg:
            task_before_subgroups = DummyOperator(task_id="start")
            task_after_subgroups = DummyOperator(task_id="end")

            sub_groups = []
            for sub_group_id in range(1, 3):
                sub_tg_id = f"sub_group{sub_group_id}"
                with TaskGroup(group_id=sub_tg_id) as sub_tg:
                    sub_task_1 = DummyOperator(task_id="task1")
                    sub_task_2 = DummyOperator(task_id="task2")

                    sub_task_1 >> sub_task_2
                    sub_groups.append(sub_tg)

            task_before_subgroups >> sub_groups >> task_after_subgroups
            main_groups.append(main_tg)

    main_groups[0] >> main_groups[1]

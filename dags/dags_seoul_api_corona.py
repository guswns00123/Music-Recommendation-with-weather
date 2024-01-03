from operators.seoul_api_to_csv_operator import SeoulApiToCsvOperator
from airflow import DAG
import pendulum

with DAG(
    dag_id='dags_seoul_api_corona',
    schedule='0 7 * * *',
    start_date=pendulum.datetime(2024,1,2, tz='Asia/Seoul'),
    catchup=False
) as dag:
    
    tb_corona19_count_status = SeoulApiToCsvOperator(
        task_id='tb_corona19_count_status',
        dataset_nm='TbCorona19CountStatus',
        path='/opt/airflow/files/Weather/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash }}',
        file_name='weather.csv'
    )
    
    

    tb_corona19_count_status
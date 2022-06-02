from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.sftp_operator import SFTPOperator
from datetime import datetime, timedelta
from airflow.operators


default_args = {
    "owner": "higor.souza",
    "depends_on_past": False,
    "start_date": datetime(2022, 3, 15),
    "email": ["email@exemplo.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "catchup": False,
}

dag = DAG("teste_SFTP", default_args=default_args,
          schedule_interval=timedelta(1))

# def list_zip_task(RAW_FILES_PATH):
#     files=os.listdir(RAW_FILES_PATH)
#     for file in files:
#         if files.endswith('.zip'):
#             filePath=path+'/'+file
#             zip_file = zipfile.ZipFile(filePath)
#             for names in zip_file.namelist():
#                 zip_file.extract(names,path)
#             zip_file.close()


def process_task(**kwargs):
    templates_dict = kwargs.get("templates_dict")
    input_file = templates_dict.get("input_file")
    output_file = templates_dict.get("output_file")
    output_rows = []
    with open(input_file, newline='') as csv_file:
        for row in csv.reader(csv_file):
            row.append("Processado")
            output_rows.append(row)
    with open(output_file, "w", newline='') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerows(output_rows)


download_new_file = SFTPOperator(
    task_id="get_file",
    ssh_conn_id="stfp_iti",
    remote_filepath="",
    local_filepath=raw_files_path,
    operation="get",
    create_intermediate_dirs=False,
    dag=dag
)

process_task = PythonOperator(task_id="process-file",
        templates_dict=['input_file', 'output_file']
        input_file= "#inputfilepath",
        output_file: "#outputfiledestination",
        python_callable=process_task,
        dag=dag
        )

upload_file = SFTPOperator(
    task_id="put-file",
    ssh_conn_id="my_sftp_server",
    remote_filepath="/{{ds}}/output.csv",
    local_filepath="/tmp/{{ run_id }}/output.csv",
    operation="put",
    dag=dag)

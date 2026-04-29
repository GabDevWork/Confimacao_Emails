from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "Gabriel Montalvão", 
    "start_date": datetime(2026, 3, 26),
    "retries": 0,
    "retry_delay": timedelta(minutes=5)
}

docs = """
    # DAG para Confirmação de E-mails
    Lê as planilhas do SFTP dos fornecedores, carrega no Snowflake, processa pendências e dispara e-mails.
"""

with DAG(
    "dbt_dag_confirmacao_emails",
    default_args=default_args,
    schedule_interval="3/5 9-12 * * *",
    catchup=False,
    tags=["<TAG_AREA>", "<TAG_PROJETO>", "<TAG_AUTOR"], 
    doc_md=docs,
    max_active_runs=1
) as dag:
    
    inicio = EmptyOperator(task_id="inicio")
    fim = EmptyOperator(task_id="fim")

    task_1 = KubernetesPodOperator(
        task_id="processamento_confirmacao_emails",
        name="pod_processamento",
        image="<link_AWS>:latest",
        cmds=["python3"],
        arguments=["/usr/datamart/confirmacaoAbastecimentoEmails.py"],
        namespace="processing",        
        is_delete_operator_pod=True,
        image_pull_policy="Always", 
        in_cluster=True
    )

    inicio >> task_1 >> fim
"""
Airflow DAG Factory para Pipelines Medalhão
============================================

Este repositório utiliza o padrão DAG Factory para gerar dinamicamente
pipelines ETL com base em configurações.

Autor: Lucas Antunes Reis
Data: 27/06/2025
"""

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import yaml
from jinja2 import Template
from airflow.models import Variable
import logging

# Importa a nova API simplificada
from log_analyzer.etl import run_pipeline

CONFIG_YAML_PATH = os.path.join(os.path.dirname(__file__), "jobs/medalhao.yaml")
with open(CONFIG_YAML_PATH, "r") as f:
    raw_yaml = f.read()

# Configuração do logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Renderização do template usando Jinja
try:
    jinja_context = {
        'var': {
            'json': {
                'log_analyzer_paths': Variable.get('log_analyzer_paths', deserialize_json=True)
            }
        }
    }
    template = Template(raw_yaml)
    rendered_yaml = template.render(**jinja_context)
    PIPELINE_CONFIG = yaml.safe_load(rendered_yaml)
    logging.info(f"Configuração YAML carregada com sucesso: {PIPELINE_CONFIG['dag_id']}")
except Exception as e:
    # Fallback para desenvolvimento local
    logging.warning(f"Erro ao carregar variáveis do Airflow: {e}")
    logging.warning("Usando valores padrão para desenvolvimento local")
    PIPELINE_CONFIG = yaml.safe_load(raw_yaml)
    # Substituir variáveis por valores padrão
    if 'pipeline_params' in PIPELINE_CONFIG:
        params = PIPELINE_CONFIG['pipeline_params']
        if '{{' in str(params):
            PIPELINE_CONFIG['pipeline_params'] = {
                'input_path': '/opt/airflow/data/logs.txt',
                'output_path': '/opt/airflow/data/processed',
                'log_format': 'common',
                'save_to_db': False
            }

def create_medalhao_dag(pipeline_config: dict) -> DAG:
    """
    Gera um DAG do Airflow dinamicamente a partir de uma configuração de pipeline.
    Versão atualizada para usar a API simplificada do log_analyzer.
    """
    with DAG(
        dag_id=pipeline_config['dag_id'],
        start_date=pipeline_config['start_date'],
        schedule_interval=pipeline_config['schedule_interval'],
        description=pipeline_config['description'],
        catchup=False,
        tags=pipeline_config['tags'],
        owner=pipeline_config['owner'],
    ) as dag:
        
        # Nova abordagem: um único task que executa o pipeline completo
        def execute_pipeline(**kwargs):
            """Executa o pipeline completo usando a nova API simplificada"""
            logging.info("Iniciando o pipeline completo de análise de logs...")
            
            # Extrai parâmetros do contexto
            input_path = kwargs.get("input_path")
            output_path = kwargs.get("output_path")
            log_format = kwargs.get("log_format")
            save_to_db = kwargs.get("save_to_db", False)
            
            # Executa o pipeline completo em um único passo
            result = run_pipeline(
                input_path=input_path, 
                output_path=output_path,
                log_format=log_format,
                save_to_db=save_to_db
            )
            
            if result["status"] != "success":
                raise Exception(f"Falha no pipeline: {result.get('error', 'Erro desconhecido')}")
                
            logging.info(f"Pipeline executado com sucesso!")
            logging.info(f"Registros processados: {result.get('processed_records', 0)}")
            logging.info(f"Output: {result.get('output_path', '-')}")
            
            # Log métricas-chave
            metrics = result.get('metrics', {})
            if metrics:
                logging.info(f"Métricas principais:")
                for key, value in metrics.items():
                    if not isinstance(value, (dict, list)):
                        logging.info(f"  {key}: {value}")
            
            return result
        
        # Cria a task para o pipeline completo
        run_pipeline_task = PythonOperator(
            task_id="run_pipeline",
            python_callable=execute_pipeline,
            op_kwargs=pipeline_config.get("pipeline_params", {}),
        )

    return dag

try:
    if isinstance(PIPELINE_CONFIG['start_date'], str):
        PIPELINE_CONFIG['start_date'] = datetime.fromisoformat(PIPELINE_CONFIG['start_date'])
except Exception as e:
    logging.warning(f"Erro ao converter data: {e}")
    PIPELINE_CONFIG['start_date'] = datetime.now()

# Registra o DAG no namespace global para o Airflow descobrir
try:
    dag_obj = create_medalhao_dag(PIPELINE_CONFIG)
    globals()[PIPELINE_CONFIG['dag_id']] = dag_obj
    logging.info(f"DAG '{PIPELINE_CONFIG['dag_id']}' criado com sucesso.")
except Exception as e:
    logging.error(f"Erro ao criar DAG: {e}")
    raise
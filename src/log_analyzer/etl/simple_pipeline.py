"""
Pipeline ETL - Execução simplificada do pipeline completo
=======================================================

Módulo para orquestrar os componentes de extração, transformação e análise de logs.

Author: Lucas Antunes Reis
Date: June 27, 2025
"""

import logging
import sys
from typing import Optional, Dict, Any, Union
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from log_analyzer.core.config import get_settings
from log_analyzer.core.spark import get_spark_session, stop_spark_session
from log_analyzer.etl.extractor import extract_logs
from log_analyzer.etl.transformer import transform_logs
from log_analyzer.etl.analyzer import analyze_logs
from log_analyzer.etl.load import load_to_parquet

logger = logging.getLogger(__name__)


def run_pipeline(
    input_path: Optional[str] = None,
    output_path: Optional[str] = None,
    save_to_db: bool = True,
    spark_session: Optional[SparkSession] = None,
    log_format: Optional[str] = None
) -> Dict[str, Any]:
    """
    Executa o pipeline ETL completo de forma simples e direta.
    
    Args:
        input_path: Caminho para o arquivo de logs de entrada
        output_path: Caminho base para salvar os resultados
        save_to_db: Se True, tenta salvar os resultados no banco de dados (ignora se driver não disponível)
        spark_session: Sessão Spark opcional
        log_format: Formato do log (apache_common, apache_combined, etc.)
        
    Returns:
        Dicionário com status e caminhos dos resultados
    """
    spark = spark_session or get_spark_session()
    settings = get_settings()
    
    try:
        logger.info("🚀 Iniciando pipeline ETL simplificado...")
        
        # Define caminhos padrão se não especificados
        if not input_path:
            paths = settings.get("paths", {})
            input_path = f"data/logs.txt"
        
        if not output_path:
            paths = settings.get("paths", {})
            output_path = paths.get("output_data")
            
        # Define caminhos derivados
        bronze_path = f"{output_path}/bronze"
        silver_path = f"{output_path}/silver"
        gold_path = f"{output_path}/gold"
        
        logger.info(f"📄 Arquivo de entrada: {input_path}")
        logger.info(f"📂 Diretório de saída: {output_path}")
        
        # Etapa 1: Extração (Bronze)
        logger.info("🔄 Executando etapa de extração (Bronze)...")
        df_bronze = extract_logs(input_path, log_format=log_format, spark_session=spark)
        
        # Salva os dados brutos extraídos
        bronze_output = f"{bronze_path}/logs.parquet"
        load_to_parquet(df_bronze, bronze_output)
        logger.info(f"💾 Dados Bronze salvos em: {bronze_output}")
        
        # Etapa 2: Transformação (Silver)
        logger.info("🔄 Executando etapa de transformação (Silver)...")
        df_silver = transform_logs(df_bronze, spark_session=spark)
        
        # Salva os dados transformados
        silver_output = f"{silver_path}/logs.parquet"
        load_to_parquet(df_silver, silver_output, partition_by=["date"])
        logger.info(f"💾 Dados Silver salvos em: {silver_output}")
        
        # Verificar se o driver PostgreSQL está disponível antes de tentar salvar no banco de dados
        db_available = False
        if save_to_db:
            try:
                # Verificar se o driver PostgreSQL está disponível
                driver_class = "org.postgresql.Driver"
                try:
                    spark._jvm.Class.forName(driver_class)
                    db_available = True
                    logger.info("✅ Driver PostgreSQL encontrado e disponível")
                except Exception:
                    logger.warning(f"⚠️ Driver PostgreSQL não está disponível: {driver_class}")
                    logger.info("ℹ️ Os dados serão salvos apenas em arquivos locais")
                    save_to_db = False
            except Exception as e:
                logger.warning(f"⚠️ Erro ao verificar disponibilidade do driver: {str(e)}")
                save_to_db = False
        
        # Etapa 3: Análise (Gold)
        logger.info("🔄 Executando etapa de análise (Gold)...")
        gold_dataframes = analyze_logs(
            df_silver, 
            output_path=gold_path,
            save_to_db=save_to_db,  # Agora será False se o driver não estiver disponível
            spark_session=spark
        )
        
        logger.info("✅ Pipeline ETL executado com sucesso!")
        
        return {
            "status": "success",
            "bronze_output": bronze_output,
            "silver_output": silver_output,
            "gold_output": gold_path,
            "metrics_count": len(gold_dataframes),
            "db_save_attempted": save_to_db,
            "db_available": db_available,
            "metrics": gold_dataframes
        }
        
    except Exception as e:
        logger.error(f"❌ Erro durante execução do pipeline: {str(e)}", exc_info=True)
        
        # Em caso de erro, paramos a sessão se foi criada aqui 
        if spark and spark_session is None:
            stop_spark_session()
            
        return {
            "status": "error",
            "error": str(e)
        }


# Alias simplificado para uso em scripts
run_medalhao_pipeline = run_pipeline


# Execução standalone
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    result = run_pipeline()
    print(f"Status: {result['status']}")
    
    if result['status'] == 'success':
        print(f"Bronze: {result.get('bronze_output')}")
        print(f"Silver: {result.get('silver_output')}")
        print(f"Gold: {result.get('gold_output')}")
    else:
        print(f"Erro: {result.get('error')}")

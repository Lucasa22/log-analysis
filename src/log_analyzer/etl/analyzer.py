"""
Log Analyzer - Análise de dados de logs
======================================

Módulo responsável por gerar métricas e análises a partir de logs transformados.
"""

import logging
from typing import Dict, List, Optional, Any, Union
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, count, desc, sum as spark_sum, max as spark_max,
    min as spark_min, avg, date_format, dayofweek, countDistinct
)

from log_analyzer.core.spark import get_spark_session
from log_analyzer.etl.load import load_to_database, load_to_parquet

logger = logging.getLogger(__name__)


def analyze_logs(
    df: DataFrame, 
    output_path: Optional[str] = None,
    save_to_db: bool = False,
    spark_session: Optional[SparkSession] = None
) -> Dict[str, DataFrame]:
    """
    Analisa os logs transformados e gera diversas métricas.
    
    Args:
        df: DataFrame com os logs transformados
        output_path: Caminho opcional para salvar resultados em parquet
        save_to_db: Se True, salva resultados no banco de dados configurado
        spark_session: Sessão Spark opcional
        
    Returns:
        Dicionário com DataFrames de métricas calculadas
    """
    spark = spark_session or get_spark_session()
    
    logger.info("🔍 Iniciando análise de logs...")
    start_time = datetime.now()
    
    # Registra o DataFrame como uma tabela temporária para consultas SQL
    df.createOrReplaceTempView("logs")
    
    # Calcula as várias métricas
    metrics = {}
    metrics['summary'] = create_summary_metrics(df)
    metrics['top_ips'] = create_top_ips(df)
    metrics['top_endpoints'] = create_top_endpoints(df)
    metrics['error_analysis'] = create_error_analysis(df)
    metrics['peak_traffic'] = create_peak_traffic_analysis(df)
    
    processing_time = (datetime.now() - start_time).total_seconds()
    logger.info(f"✅ Análise concluída em {processing_time:.2f} segundos")
    
    # Salva os resultados, se solicitado
    if output_path:
        logger.info(f"💾 Salvando resultados em {output_path}...")
        for name, metric_df in metrics.items():
            metric_path = f"{output_path}/{name}"
            load_to_parquet(metric_df, metric_path)
    
    # Salva no banco de dados, se solicitado
    if save_to_db:
        logger.info("📊 Salvando métricas no banco de dados...")
        from log_analyzer.core.config import get_settings
        
        settings = get_settings()
        db_config = settings.get("database", {})
        db_url = db_config.get("silver_db_url")
        db_properties = {
            "user": db_config.get("silver_db_user"),
            "password": db_config.get("silver_db_password"),
            "driver": db_config.get("silver_db_driver"),
        }
        
        for name, metric_df in metrics.items():
            try:
                table_name = db_config.get("gold_tables", {}).get(name, f"gold_{name}")
                load_to_database(metric_df, table_name, db_url, db_properties)
                logger.info(f"✅ Métrica '{name}' salva no banco de dados com sucesso")
            except Exception as e:
                logger.warning(f"⚠️ Erro ao salvar métrica '{name}' no banco de dados: {str(e)}")
                logger.info("ℹ️ Continuando com as próximas métricas...")
    
    return metrics


def create_summary_metrics(df: DataFrame) -> DataFrame:
    """Cria métricas de resumo do tráfego."""
    logger.info("📊 Gerando métricas de resumo...")
    
    return df.agg(
        count("*").alias("total_requests"),
        countDistinct("ip").alias("unique_visitors"),
        countDistinct("date").alias("days_span"),
        avg("bytes_sent_int").alias("avg_response_size"),
        spark_sum((col("status_int") >= 400).cast("int")).alias("total_errors"),
        (spark_sum((col("status_int") >= 400).cast("int")) / count("*") * 100).alias("error_rate")
    )


def create_top_ips(df: DataFrame, limit: int = 10) -> DataFrame:
    """Cria métricas dos IPs com mais requisições."""
    logger.info("🖥️ Identificando IPs com mais requisições...")
    
    return df.groupBy("ip").agg(
        count("*").alias("requests"),
        spark_sum("bytes_sent_int").alias("total_bytes"),
        spark_sum((col("status_int") >= 400).cast("int")).alias("errors")
    ).orderBy(desc("requests")).limit(limit)


def create_top_endpoints(df: DataFrame, limit: int = 10) -> DataFrame:
    """Cria métricas dos endpoints mais acessados."""
    logger.info("🌐 Identificando endpoints mais acessados...")
    
    return (
        df.filter(~col("is_static_asset"))
        .groupBy("url")
        .agg(
            count("*").alias("requests"),
            avg("bytes_sent_int").alias("avg_size"),
            spark_sum((col("status_int") >= 400).cast("int")).alias("errors")
        )
        .orderBy(desc("requests"))
        .limit(limit)
    )


def create_error_analysis(df: DataFrame) -> DataFrame:
    """Analisa os erros por código de status."""
    logger.info("❌ Analisando erros por código de status...")
    
    return (
        df.filter(col("status_int") >= 400)
        .groupBy("status", "status_int")
        .agg(
            count("*").alias("count"),
            countDistinct("ip").alias("affected_users"),
            countDistinct("date").alias("affected_days")
        )
        .orderBy(desc("count"))
    )


def create_peak_traffic_analysis(df: DataFrame) -> DataFrame:
    """Identifica períodos de pico de tráfego."""
    logger.info("📈 Identificando períodos de pico de tráfego...")
    
    return (
        df.groupBy("date", "hour")
        .agg(
            count("*").alias("requests"),
            spark_sum("bytes_sent_int").alias("total_bytes"),
            spark_sum((col("status_int") >= 400).cast("int")).alias("errors")
        )
        .orderBy(desc("requests"))
    )

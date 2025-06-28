"""
ETL Load module - Operações de carregamento para o pipeline ETL

Este módulo fornece funções simples e diretas para carregar dados processados
em diferentes formatos e destinos, como parquet e bancos de dados.
"""
import logging
from typing import Dict, List, Optional, Union
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from log_analyzer.core.spark import get_spark_session

logger = logging.getLogger(__name__)

def load_to_parquet(
    df: DataFrame, 
    output_path: Union[str, Path], 
    mode: str = "overwrite", 
    partition_by: Optional[List[str]] = None,
    spark_session: Optional[SparkSession] = None
) -> None:
    """
    Carrega um DataFrame Spark para arquivos Parquet.
    
    Args:
        df: DataFrame a ser salvo
        output_path: Caminho para salvar os arquivos
        mode: Modo de escrita (overwrite, append, etc.)
        partition_by: Colunas para particionamento
        spark_session: Sessão Spark opcional
    """
    logger.info(f"💾 Carregando dados para Parquet em: {output_path}")
    
    writer = df.write.mode(mode).format("parquet")
    
    if partition_by:
        writer = writer.partitionBy(*partition_by)
        
    writer.save(str(output_path))
    logger.info(f"✓ Dados salvos com sucesso em: {output_path}")

def load_to_database(
    df: DataFrame, 
    table_name: str, 
    url: str = None, 
    properties: Dict[str, str] = None,
    mode: str = "overwrite",
    spark_session: Optional[SparkSession] = None
) -> None:
    """
    Carrega um DataFrame para um banco de dados via JDBC.
    
    Args:
        df: DataFrame a ser salvo
        table_name: Nome da tabela de destino
        url: URL de conexão JDBC
        properties: Propriedades de conexão (user, password, driver)
        mode: Modo de escrita (overwrite, append, etc.)
        spark_session: Sessão Spark opcional
    """
    logger.info(f"📊 Carregando dados para tabela: {table_name}")
    
    # Se url e properties não foram fornecidos, tenta carregar das configurações
    if url is None or properties is None:
        from log_analyzer.core.config import get_settings
        settings = get_settings()
        db_config = settings.get("database", {})
        
        if url is None:
            url = db_config.get("silver_db_url")
        
        if properties is None:
            properties = {
                "user": db_config.get("silver_db_user"),
                "password": db_config.get("silver_db_password"),
                "driver": db_config.get("silver_db_driver"),
            }
    
    if url is None:
        raise ValueError("URL de conexão JDBC não foi fornecida e não está nas configurações")
    
    # Verificar se as propriedades necessárias estão presentes
    for prop in ["user", "driver"]:
        if prop not in properties or not properties[prop]:
            logger.warning(f"⚠️ Propriedade obrigatória '{prop}' não definida para conexão JDBC")
    
    try:
        df.write.jdbc(
            url=url,
            table=table_name,
            mode=mode,
            properties=properties
        )
        logger.info(f"✓ Dados salvos com sucesso na tabela: {table_name}")
    except Exception as e:
        logger.error(f"❌ Erro ao salvar na tabela {table_name}: {str(e)}")
        raise  # Re-lança a exceção para tratamento em nível superior

__all__ = ['load_to_parquet', 'load_to_database']
"""
Testes para a API Simplificada do Log Analyzer
=============================================

Este módulo contém testes para a API simplificada do log_analyzer,
focando especialmente na função run_pipeline.

Author: Data Team
Date: June 27, 2025
"""

import pytest
import tempfile
import os
from unittest.mock import patch, MagicMock, call
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from log_analyzer.etl import (
    run_pipeline,
    extract_logs,
    transform_logs,
    analyze_logs,
    load_to_parquet
)


@pytest.fixture
def mock_spark_session():
    """Cria uma sessão Spark mockada para testes."""
    mock_spark = MagicMock(spec=SparkSession)
    
    # Adiciona alguns atributos comumente usados
    mock_spark.sparkContext = MagicMock()
    mock_spark.read = MagicMock()
    mock_spark.read.text = MagicMock(return_value=MagicMock(spec=DataFrame))
    mock_spark.read.parquet = MagicMock(return_value=MagicMock(spec=DataFrame))
    
    return mock_spark


@pytest.fixture
def mock_settings():
    """Cria configurações mockadas para testes."""
    return {
        "paths": {
            "raw_data": "/tmp/raw",
            "processed_data": "/tmp/processed",
            "output_data": "/tmp/output",
        },
        "log_formats": {
            "apache_common": {
                "pattern": r'^(\S+) \S+ \S+ \[([^\]]+)\] "(\S+) (\S+) (\S+)" (\d+) (\S+)',
                "timestamp_format": "dd/MMM/yyyy:HH:mm:ss Z",
                "columns": ["ip", "timestamp_str", "method", "url", "protocol", "status", "size"]
            }
        }
    }


@pytest.fixture
def mock_dataframe():
    """Cria um DataFrame mockado para testes."""
    df = MagicMock(spec=DataFrame)
    
    # Configurar métodos comumente usados
    df.count = MagicMock(return_value=100)
    df.columns = ["ip", "timestamp_str", "method", "url", "status", "size"]
    df.schema = MagicMock(return_value=StructType([
        StructField("ip", StringType(), True),
        StructField("timestamp_str", StringType(), True),
        StructField("method", StringType(), True),
        StructField("url", StringType(), True),
        StructField("status", StringType(), True),
        StructField("size", StringType(), True),
    ]))
    df.limit = MagicMock(return_value=df)
    df.show = MagicMock()
    df.printSchema = MagicMock()
    df.withColumn = MagicMock(return_value=df)
    df.withColumnRenamed = MagicMock(return_value=df)
    df.write = MagicMock()
    df.write.parquet = MagicMock()
    df.write.format = MagicMock(return_value=df.write)
    df.write.mode = MagicMock(return_value=df.write)
    df.write.save = MagicMock()
    df.filter = MagicMock(return_value=df)
    df.groupBy = MagicMock(return_value=df)
    df.agg = MagicMock(return_value=df)
    
    return df


@pytest.mark.unit
def test_run_pipeline_basic(mock_spark_session, mock_dataframe, mock_settings):
    """Testa o funcionamento básico do run_pipeline."""
    # Configurando os mocks e patches
    with patch("log_analyzer.etl.simple_pipeline.get_spark_session", return_value=mock_spark_session):
        with patch("log_analyzer.etl.simple_pipeline.get_settings", return_value=mock_settings):
            with patch("log_analyzer.etl.simple_pipeline.extract_logs", return_value=mock_dataframe) as mock_extract:
                with patch("log_analyzer.etl.simple_pipeline.transform_logs", return_value=mock_dataframe) as mock_transform:
                    with patch("log_analyzer.etl.simple_pipeline.analyze_logs", return_value={"metrics": mock_dataframe}) as mock_analyze:
                        with patch("log_analyzer.etl.simple_pipeline.load_to_parquet") as mock_load:
                            with patch("log_analyzer.etl.simple_pipeline.stop_spark_session") as mock_stop:
                                
                                # Chamando a função com parâmetros básicos
                                result = run_pipeline(
                                    input_path="/tmp/input/logs.txt",
                                    output_path="/tmp/output"
                                )
                                
                                # Verificações
                                mock_extract.assert_called_once()
                                mock_transform.assert_called_once()
                                mock_analyze.assert_called_once()
                                assert mock_load.call_count >= 2  # Pelo menos duas chamadas (bronze e silver)
                                mock_stop.assert_not_called()  # Não deve parar a sessão que foi passada
                                
                                # Verificações do resultado
                                assert result["status"] == "success"
                                assert "bronze_output" in result
                                assert "silver_output" in result
                                assert "gold_output" in result
                                assert "metrics_count" in result


@pytest.mark.unit
def test_run_pipeline_with_custom_spark_session(mock_spark_session, mock_dataframe, mock_settings):
    """Testa o run_pipeline com uma sessão Spark customizada."""
    # Configurando os mocks e patches
    with patch("log_analyzer.etl.simple_pipeline.get_settings", return_value=mock_settings):
        with patch("log_analyzer.etl.simple_pipeline.extract_logs", return_value=mock_dataframe) as mock_extract:
            with patch("log_analyzer.etl.simple_pipeline.transform_logs", return_value=mock_dataframe) as mock_transform:
                with patch("log_analyzer.etl.simple_pipeline.analyze_logs", return_value={"metrics": mock_dataframe}) as mock_analyze:
                    with patch("log_analyzer.etl.simple_pipeline.load_to_parquet") as mock_load:
                        with patch("log_analyzer.etl.simple_pipeline.stop_spark_session") as mock_stop:
                            
                            # Chamando com sessão Spark personalizada
                            result = run_pipeline(
                                input_path="/tmp/input/logs.txt",
                                output_path="/tmp/output",
                                spark_session=mock_spark_session
                            )
                            
                            # A sessão personalizada deve ser usada e não encerrada
                            mock_extract.assert_called_once_with(
                                "/tmp/input/logs.txt", 
                                log_format=None, 
                                spark_session=mock_spark_session
                            )
                            mock_stop.assert_not_called()  # Não deve parar a sessão que foi passada
                            
                            assert result["status"] == "success"


@pytest.mark.unit
def test_run_pipeline_error_handling(mock_spark_session, mock_dataframe, mock_settings):
    """Testa o tratamento de erros do run_pipeline."""
    # Configurando os mocks para gerar um erro
    with patch("log_analyzer.etl.simple_pipeline.get_spark_session", return_value=mock_spark_session):
        with patch("log_analyzer.etl.simple_pipeline.get_settings", return_value=mock_settings):
            with patch("log_analyzer.etl.simple_pipeline.extract_logs", side_effect=Exception("Erro de extração")) as mock_extract:
                with patch("log_analyzer.etl.simple_pipeline.stop_spark_session") as mock_stop:
                    
                    # Chamando a função que deve capturar o erro
                    result = run_pipeline(
                        input_path="/tmp/input/logs.txt",
                        output_path="/tmp/output"
                    )
                    
                    # Verificações
                    mock_extract.assert_called_once()
                    mock_stop.assert_called_once()  # Deve parar a sessão em caso de erro
                    
                    # Verificações do resultado de erro
                    assert result["status"] == "error"
                    assert "error" in result
                    assert "Erro de extração" in result["error"]


@pytest.mark.unit
def test_run_pipeline_with_log_format(mock_spark_session, mock_dataframe, mock_settings):
    """Testa o run_pipeline especificando um formato de log."""
    # Configurando os mocks e patches
    with patch("log_analyzer.etl.simple_pipeline.get_spark_session", return_value=mock_spark_session):
        with patch("log_analyzer.etl.simple_pipeline.get_settings", return_value=mock_settings):
            with patch("log_analyzer.etl.simple_pipeline.extract_logs", return_value=mock_dataframe) as mock_extract:
                with patch("log_analyzer.etl.simple_pipeline.transform_logs", return_value=mock_dataframe) as mock_transform:
                    with patch("log_analyzer.etl.simple_pipeline.analyze_logs", return_value={"metrics": mock_dataframe}):
                        with patch("log_analyzer.etl.simple_pipeline.load_to_parquet"):
                            
                            # Chamando com formato de log específico
                            log_format = "apache_combined"
                            run_pipeline(
                                input_path="/tmp/input/logs.txt",
                                output_path="/tmp/output",
                                log_format=log_format
                            )
                            
                            # Verificar se o formato foi passado corretamente
                            mock_extract.assert_called_once_with(
                                "/tmp/input/logs.txt", 
                                log_format=log_format, 
                                spark_session=mock_spark_session
                            )


@pytest.mark.unit
def test_extract_logs_integration(mock_spark_session, mock_settings):
    """Testa a integração entre extract_logs e a nova API."""
    # Criando um arquivo de logs temporário
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp.write(b'192.168.1.1 - - [25/Jun/2025:00:00:00 +0000] "GET /home HTTP/1.1" 200 1234\n')
        tmp.write(b'192.168.1.2 - - [25/Jun/2025:00:01:00 +0000] "POST /api HTTP/1.1" 201 5678\n')
        tmp_path = tmp.name
    
    try:
        # Configurando mock do dataframe resultante
        mock_df = MagicMock(spec=DataFrame)
        mock_spark_session.read.text.return_value.limit.return_value = mock_df
        mock_df.collect.return_value = [
            MagicMock(value='192.168.1.1 - - [25/Jun/2025:00:00:00 +0000] "GET /home HTTP/1.1" 200 1234')
        ]
        
        # Mock do DataFrame final filtrado
        mock_filtered_df = MagicMock(spec=DataFrame)
        mock_filtered_df.count.return_value = 1
        mock_df.filter.return_value = mock_filtered_df
        
        # Mock do DataFrame resultado da extração
        extracted_df = MagicMock(spec=DataFrame)
        mock_df.withColumn.return_value = extracted_df
        extracted_df.withColumn.return_value = extracted_df
        
        with patch("log_analyzer.etl.extractor.get_settings", return_value=mock_settings):
            # Chamando a função
            result_df = extract_logs(tmp_path, spark_session=mock_spark_session)
            
            # Verificações
            mock_spark_session.read.text.assert_called_with(tmp_path)
            assert result_df is not None
            
    finally:
        # Limpeza
        os.unlink(tmp_path)


@pytest.mark.integration
def test_transform_logs_basic(mock_spark_session, mock_dataframe):
    """Testa o funcionamento básico da função transform_logs."""
    with patch("log_analyzer.etl.transformer.get_spark_session", return_value=mock_spark_session):
        # Chamando a função
        result_df = transform_logs(mock_dataframe)
        assert result_df is not None


@pytest.mark.integration
def test_end_to_end_pipeline_with_tempdir():
    """Teste de integração end-to-end usando diretórios temporários."""
    # Criar diretórios temporários
    with tempfile.TemporaryDirectory() as tmp_dir:
        input_dir = Path(tmp_dir) / "input"
        output_dir = Path(tmp_dir) / "output"
        
        input_dir.mkdir()
        
        # Criar arquivo de logs fictício
        log_file = input_dir / "test_logs.txt"
        with open(log_file, "w") as f:
            f.write('192.168.1.1 - - [25/Jun/2025:00:00:00 +0000] "GET /home HTTP/1.1" 200 1234\n')
            f.write('192.168.1.2 - - [25/Jun/2025:00:01:00 +0000] "POST /api HTTP/1.1" 201 5678\n')
        
        # Mock todo o pipeline para evitar dependências com Spark real
        with patch("log_analyzer.etl.simple_pipeline.extract_logs") as mock_extract:
            with patch("log_analyzer.etl.simple_pipeline.transform_logs") as mock_transform:
                with patch("log_analyzer.etl.simple_pipeline.analyze_logs") as mock_analyze:
                    with patch("log_analyzer.etl.simple_pipeline.load_to_parquet") as mock_load:
                        
                        # Configurar resultados dos mocks
                        mock_df = MagicMock(spec=DataFrame)
                        mock_extract.return_value = mock_df
                        mock_transform.return_value = mock_df
                        mock_analyze.return_value = {"metrics": mock_df}
                        
                        # Executar o pipeline
                        result = run_pipeline(
                            input_path=str(log_file),
                            output_path=str(output_dir)
                        )
                        
                        # Verificações
                        assert result["status"] == "success"
                        assert mock_extract.called
                        assert mock_transform.called
                        assert mock_analyze.called
                        assert mock_load.call_count >= 1


if __name__ == "__main__":
    pytest.main(["-xvs", __file__])

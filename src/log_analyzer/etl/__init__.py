"""
Log Analyzer - Módulo ETL
=========================

Este módulo para extração, transformação e análise de logs.

Estrutura simples e direta:
- extractor.py: Funções para extração de logs
- transformer.py: Funções para transformação de dados
- analyzer.py: Funções para análise e métricas
- load.py: Funções para carregamento de dados
- simple_pipeline.py: Orquestração de todo o pipeline

Usage:
    from log_analyzer.etl import run_pipeline
    result = run_pipeline(input_path="logs.txt", output_path="output")
"""

# Importações convenientes para uso direto
from log_analyzer.etl.extractor import extract_logs
from log_analyzer.etl.transformer import transform_logs
from log_analyzer.etl.analyzer import (
    analyze_logs, create_summary_metrics, create_top_ips, 
    create_top_endpoints, create_error_analysis
)
from log_analyzer.etl.load import load_to_parquet, load_to_database
from log_analyzer.etl.simple_pipeline import run_pipeline

__all__ = [
    # Pipeline principal
    'run_pipeline',
    
    # Funções individuais
    'extract_logs',
    'transform_logs',
    'analyze_logs',
    'load_to_parquet',
    'load_to_database',
    
    # Funções de análise específicas
    'create_summary_metrics',
    'create_top_ips',
    'create_top_endpoints',
    'create_error_analysis'
]

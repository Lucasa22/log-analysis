import os
from pathlib import Path
from typing import Dict, Any, Optional

PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
DATA_ROOT = PROJECT_ROOT / "data"
LOGS_ROOT = PROJECT_ROOT / "logs"

DEFAULT_CONFIG = {
    "spark": {
        "app_name": "LogAnalyzer",
        "master": "local[*]",
        "executor_memory": "2g",
        "driver_memory": "1g",
        "executor_cores": "2",
        "serializer": "org.apache.spark.serializer.KryoSerializer",
        "sql.adaptive.enabled": "true",
        "sql.adaptive.coalescePartitions.enabled": "true",
    },
    "paths": {
        "data_root": str(DATA_ROOT),
        "logs_root": str(LOGS_ROOT),
        "raw_data": str(DATA_ROOT / "raw"),
        "processed_data": str(DATA_ROOT / "processed"),
        "output_data": str(DATA_ROOT / "output"),
        "staging_data": str(DATA_ROOT / "staging"),
    },
    "log_formats": {
        "apache_common": {
            "pattern": r'^(\S+) \S+ \S+ \[([^\]]+)\] "(\S+) (\S+) (\S+)" (\d+) (\S+)',
            "timestamp_format": "dd/MMM/yyyy:HH:mm:ss Z",
            "columns": ["ip", "timestamp_str", "method", "url", "protocol", "status", "size"],
        },
        "apache_combined": {
            "pattern": r'^(\S+) \S+ \S+ \[([^\]]+)\] "(\S+) (\S+) (\S+)" (\d+) (\S+) "([^"]*)" "([^"]*)"' ,
            "timestamp_format": "dd/MMM/yyyy:HH:mm:ss Z",
            "columns": ["ip", "timestamp_str", "method", "url", "protocol", "status", "size", "referer", "user_agent"],
        },
    },
    "analysis": {
        "top_n_items": 10,
        "date_format": "%d/%b/%Y:%H:%M:%S %z",
        "time_window_minutes": 60,
        "error_threshold": 0.05,
    },
    "output": {
        "default_format": "parquet",
        "compression": "snappy",
        "overwrite_mode": "overwrite",
    },
    "database": {
        "silver_db_url": "jdbc:postgresql://postgres:5432/airflow",
        "silver_db_user": "airflow",
        "silver_db_password": "airflow",
        "silver_db_driver": "org.postgresql.Driver",
        "gold_tables": {
            "summary": "gold_summary_metrics",
            "top_ips": "gold_top_client_ips",
            "top_endpoints": "gold_top_endpoints",
            "peak_error": "gold_peak_error_day"
        }
    }
}


class Settings:
    """
    Gerencia as configurações do projeto, permitindo sobrescrita por variáveis de ambiente.
    """
    def __init__(self, config_dict: Optional[Dict[str, Any]] = None):
        """
        Inicializa as configurações com um dicionário customizado ou padrão.
        """
        self._config = config_dict or DEFAULT_CONFIG.copy()
        self._load_environment_overrides()

    def _load_environment_overrides(self):
        """
        Sobrescreve configurações com variáveis de ambiente, se existirem.
        """
        if os.getenv("SPARK_MASTER"):
            self._config["spark"]["master"] = os.getenv("SPARK_MASTER")
        if os.getenv("SPARK_EXECUTOR_MEMORY"):
            self._config["spark"]["executor_memory"] = os.getenv("SPARK_EXECUTOR_MEMORY")
        if os.getenv("SPARK_DRIVER_MEMORY"):
            self._config["spark"]["driver_memory"] = os.getenv("SPARK_DRIVER_MEMORY")
        if os.getenv("DATA_ROOT"):
            data_root = Path(os.getenv("DATA_ROOT"))
            self._config["paths"]["data_root"] = str(data_root)
            self._config["paths"]["raw_data"] = str(data_root / "raw")
            self._config["paths"]["processed_data"] = str(data_root / "processed")
            self._config["paths"]["output_data"] = str(data_root / "output")
            self._config["paths"]["staging_data"] = str(data_root / "staging")
        if os.getenv("SILVER_DB_PASSWORD"):
            self._config["database"]["silver_db_password"] = os.getenv("SILVER_DB_PASSWORD")

    def get(self, path: str, default: Any = None) -> Any:
        """
        Recupera um valor de configuração usando notação de pontos.
        """
        keys = path.split(".")
        value = self._config
        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return default
        return value

    def set(self, path: str, value: Any):
        """
        Define um valor de configuração usando notação de pontos.
        """
        keys = path.split(".")
        config = self._config
        for key in keys[:-1]:
            if key not in config:
                config[key] = {}
            config = config[key]
        config[keys[-1]] = value

    def get_spark_config(self) -> Dict[str, str]:
        """
        Retorna as configurações do Spark.
        """
        return self._config.get("spark", {})

    def get_paths(self) -> Dict[str, str]:
        """
        Retorna os caminhos de dados configurados.
        """
        return self._config.get("paths", {})

    def get_log_formats(self) -> Dict[str, str]:
        """
        Retorna os formatos de log configurados.
        """
        return self._config.get("log_formats", {})

    def to_dict(self) -> Dict[str, Any]:
        """
        Retorna uma cópia do dicionário de configurações.
        """
        return self._config.copy()


settings = Settings()


def get_settings() -> Settings:
    """
    Retorna a instância global de configurações.
    """
    return settings


def create_directories():
    """
    Cria os diretórios de dados definidos nas configurações, se não existirem.
    """
    paths = settings.get_paths()
    for path_key, path_value in paths.items():
        if path_key.endswith("_data") or path_key.endswith("_root"):
            Path(path_value).mkdir(parents=True, exist_ok=True)


create_directories()

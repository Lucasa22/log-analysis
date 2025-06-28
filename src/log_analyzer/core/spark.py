import os
from typing import Optional
from pyspark.sql import SparkSession
import logging

logger = logging.getLogger(__name__)


class SparkSessionFactory:
    """
    Singleton para criar e gerenciar uma SparkSession reutilizável.
    """

    _instance: Optional[SparkSession] = None

    @classmethod
    def get_spark_session(
        cls,
        app_name: str = "LogAnalyzer",
        master_url: Optional[str] = None,
        executor_memory: str = "2g",
        driver_memory: str = "1g",
        executor_cores: str = "2",
        force_new: bool = False,
    ) -> SparkSession:
        """
        Retorna uma SparkSession singleton configurada.
        """
        actual_master_url = master_url if master_url is not None else os.getenv("SPARK_MASTER_URL", "local[*]")

        if cls._instance is None or force_new:
            if cls._instance and force_new:
                cls._instance.stop()
            logger.info(f"Creating new Spark session: {app_name}")
            cls._instance = (
                SparkSession.builder.appName(app_name)
                .master(actual_master_url)
                .config("spark.executor.memory", executor_memory)
                .config("spark.driver.memory", driver_memory)
                .config("spark.executor.cores", executor_cores)
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                .config("spark.sql.adaptive.skewJoin.enabled", "true")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .getOrCreate()
            )
            cls._instance.sparkContext.setLogLevel("WARN")
            logger.info("✅ Spark session created successfully")
        return cls._instance

    @classmethod
    def stop_session(cls):
        """
        Encerra a SparkSession ativa.
        """
        if cls._instance:
            logger.info("Stopping Spark session")
            cls._instance.stop()
            cls._instance = None


def get_spark_session(**kwargs) -> SparkSession:
    """
    Wrapper para obter a SparkSession singleton.
    """
    return SparkSessionFactory.get_spark_session(**kwargs)


def stop_spark_session():
    """
    Encerra a SparkSession ativa.
    """
    SparkSessionFactory.stop_session()

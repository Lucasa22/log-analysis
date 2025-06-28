from pathlib import Path
from typing import Any, Optional
from pyspark.sql import SparkSession

class IOCommon:
    """
    Classe base para operações de entrada e saída com Spark.
    Fornece interface padrão para leitura e escrita de dados.
    """
    def __init__(self, spark_session: Optional[SparkSession] = None):
        """
        Inicializa com uma SparkSession opcional.
        """
        self.spark = spark_session

    def read(self, source: Any, **kwargs):
        """
        Interface para leitura de dados. Deve ser implementada nas subclasses.
        """
        raise NotImplementedError

    def write(self, data: Any, destination: Any, **kwargs):
        """
        Interface para escrita de dados. Deve ser implementada nas subclasses.
        """
        raise NotImplementedError

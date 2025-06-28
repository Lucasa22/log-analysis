from pathlib import Path
from typing import Union, Optional, List, Dict, Any
from pyspark.sql import DataFrame, SparkSession
import logging
from .io_common import IOCommon

logger = logging.getLogger(__name__)

class DataRepository(IOCommon):
    """
    Repositório de dados para operações de leitura e escrita com Spark.
    """
    def __init__(self, spark_session: Optional[SparkSession] = None):
        """
        Inicializa o DataRepository com uma SparkSession.
        """
        from .spark import get_spark_session
        super().__init__(spark_session or get_spark_session())

    def read(
        self,
        source: Union[str, Path],
        file_format: str,
        options: Optional[Dict[str, Any]] = None
    ) -> DataFrame:
        """
        Lê dados de um arquivo em um formato suportado pelo Spark.
        """
        path_str = str(source)
        options = options or {}
        logger.info(f"Reading data from '{path_str}' with format '{file_format}'")
        try:
            return self.spark.read.format(file_format).options(**options).load(path_str)
        except Exception as e:
            logger.error(f"Failed to read from {path_str}: {e}")
            raise

    def write(
        self,
        df: DataFrame,
        destination: Union[str, Path],
        file_format: str,
        mode: str = "overwrite",
        partition_by: Optional[List[str]] = None,
    ):
        """
        Escreve um DataFrame em um arquivo no formato especificado.
        """
        path_str = str(destination)
        logger.info(f"Writing data to '{path_str}' with format '{file_format}'")
        try:
            writer = df.write.format(file_format).mode(mode)
            if partition_by:
                writer = writer.partitionBy(*partition_by)
            writer.save(path_str)
        except Exception as e:
            logger.error(f"Failed to write to {path_str}: {e}")
            raise

    def read_text_file(
        self,
        file_path: Union[str, Path],
        streaming: bool = False
    ) -> DataFrame:
        """
        Lê um arquivo de texto como DataFrame Spark.
        """
        if streaming:
            return self.spark.readStream.text(str(file_path))
        return self.read(file_path, "text")

    def read_parquet(self, file_path: Union[str, Path]) -> DataFrame:
        """
        Lê um arquivo Parquet como DataFrame Spark.
        """
        return self.read(file_path, "parquet")

    def write_parquet(
        self,
        df: DataFrame,
        output_path: Union[str, Path],
        mode: str = "overwrite",
        partition_by: Optional[List[str]] = None,
    ):
        """
        Escreve um DataFrame em formato Parquet.
        """
        self.write(df, output_path, "parquet", mode=mode, partition_by=partition_by)

    def ensure_directory(self, directory_path: Union[str, Path]) -> Path:
        """
        Garante que o diretório existe, criando se necessário.
        """
        path = Path(directory_path)
        path.mkdir(parents=True, exist_ok=True)
        return path

    def file_exists(self, file_path: Union[str, Path]) -> bool:
        """
        Verifica se um arquivo existe.
        """
        return Path(file_path).exists()

    def write_to_jdbc(
        self,
        df: DataFrame,
        url: str,
        table_name: str,
        mode: str = "overwrite",
        properties: Optional[Dict[str, str]] = None
    ):
        """
        Escreve um DataFrame numa tabela de banco de dados via JDBC.
        """
        properties = properties or {}
        logger.info(f"Writing DataFrame to JDBC table '{table_name}'...")
        try:
            df.write.jdbc(
                url=url,
                table=table_name,
                mode=mode,
                properties=properties
            )
            logger.info(f"Successfully wrote data to table '{table_name}'.")
        except Exception as e:
            logger.error(f"❌ Failed to write to JDBC table '{table_name}': {e}", exc_info=True)
            raise

    def read_from_jdbc(
        self,
        url: str,
        table_name: str,
        properties: Optional[Dict[str, str]] = None
    ) -> DataFrame:
        """
        Lê dados de uma tabela de banco de dados via JDBC para um DataFrame.
        """
        properties = properties or {}
        logger.info(f"Reading data from JDBC table '{table_name}'...")
        try:
            df = self.spark.read.jdbc(
                url=url,
                table=f"({table_name}) as t",
                properties=properties
            )
            logger.info(f"Successfully read data from table '{table_name}'.")
            return df
        except Exception as e:
            logger.error(f"❌ Failed to read from JDBC table '{table_name}': {e}", exc_info=True)
            raise


def read_text_file(file_path: Union[str, Path], streaming: bool = False) -> DataFrame:
    """
    Lê um arquivo de texto como DataFrame Spark (atalho).
    """
    repo = DataRepository()
    return repo.read_text_file(file_path, streaming=streaming)


def write_parquet(df: DataFrame, output_path: Union[str, Path], **kwargs) -> None:
    """
    Escreve um DataFrame em formato Parquet (atalho).
    """
    repo = DataRepository()
    return repo.write_parquet(df, output_path, **kwargs)


def read_parquet(file_path: Union[str, Path]) -> DataFrame:
    """
    Lê um arquivo Parquet como DataFrame Spark (atalho).
    """
    repo = DataRepository()
    return repo.read_parquet(file_path)

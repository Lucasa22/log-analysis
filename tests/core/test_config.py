import pytest
from unittest.mock import patch, call
from pathlib import Path

# Importar o módulo config.py de forma que possamos recarregá-lo ou mockar suas dependências
from log_analyzer.core import config as config_module
from log_analyzer.core.config import Settings, DEFAULT_CONFIG, PROJECT_ROOT


@pytest.fixture
def default_settings():
    """Retorna uma instância de Settings com a configuração padrão."""
    return Settings()


def test_settings_initialization_default(default_settings):
    """Testa se Settings carrega DEFAULT_CONFIG por padrão."""
    assert default_settings.to_dict() == DEFAULT_CONFIG


def test_settings_initialization_custom_config():
    """Testa se Settings carrega uma configuração personalizada."""
    custom_config_dict = {"key": "value", "nested": {"subkey": "subvalue"}}
    settings = Settings(config_dict=custom_config_dict)
    # A inicialização também carrega variáveis de ambiente, então verificamos apenas as chaves personalizadas.
    assert settings.get("key") == "value"
    assert settings.get("nested.subkey") == "subvalue"


# Abordagem revisada para evitar KeyError no teardown
def test_settings_load_environment_overrides_spark(mocker): # Nome limpo
    """Testa se as variáveis de ambiente do Spark sobrescrevem a configuração."""

    original_environ_keys = {
        "SPARK_MASTER": config_module.os.environ.get("SPARK_MASTER"),
        "SPARK_EXECUTOR_MEMORY": config_module.os.environ.get("SPARK_EXECUTOR_MEMORY"),
        "SPARK_DRIVER_MEMORY": config_module.os.environ.get("SPARK_DRIVER_MEMORY"),
        "DATA_ROOT": config_module.os.environ.get("DATA_ROOT"), # para isolamento
        "SILVER_DB_PASSWORD": config_module.os.environ.get("SILVER_DB_PASSWORD") # para isolamento
    }

    # Define os mocks para este teste
    config_module.os.environ["SPARK_MASTER"] = "local[4]"
    config_module.os.environ["SPARK_EXECUTOR_MEMORY"] = "4g"
    config_module.os.environ["SPARK_DRIVER_MEMORY"] = "2g"

    # Garante que outras chaves de override estejam indefinidas para este teste
    if "DATA_ROOT" in config_module.os.environ:
        del config_module.os.environ["DATA_ROOT"]
    if "SILVER_DB_PASSWORD" in config_module.os.environ:
        del config_module.os.environ["SILVER_DB_PASSWORD"]

    try:
        settings = Settings()
        assert settings.get("spark.master") == "local[4]"
        assert settings.get("spark.executor_memory") == "4g"
        assert settings.get("spark.driver_memory") == "2g"
        assert settings.get("spark.app_name") == DEFAULT_CONFIG["spark"]["app_name"]
    finally:
        # Restaura todas as chaves modificadas/removidas
        for k, v in original_environ_keys.items():
            if v is None:
                if k in config_module.os.environ:
                    del config_module.os.environ[k]
            else:
                config_module.os.environ[k] = v


# Abordagem revisada
def test_settings_load_environment_overrides_data_root(mocker): # Nome limpo
    """Testa se a variável de ambiente DATA_ROOT sobrescreve os caminhos."""
    test_data_root_str = "/tmp/test_data_revised"

    original_environ_keys = {
        "DATA_ROOT": config_module.os.environ.get("DATA_ROOT"),
        "SPARK_MASTER": config_module.os.environ.get("SPARK_MASTER"), # para isolamento
        "SPARK_EXECUTOR_MEMORY": config_module.os.environ.get("SPARK_EXECUTOR_MEMORY"), # para isolamento
        "SPARK_DRIVER_MEMORY": config_module.os.environ.get("SPARK_DRIVER_MEMORY"), # para isolamento
        "SILVER_DB_PASSWORD": config_module.os.environ.get("SILVER_DB_PASSWORD") # para isolamento
    }

    config_module.os.environ["DATA_ROOT"] = test_data_root_str

    # Garante que outras chaves de override estejam indefinidas
    if "SPARK_MASTER" in config_module.os.environ: del config_module.os.environ["SPARK_MASTER"]
    if "SPARK_EXECUTOR_MEMORY" in config_module.os.environ: del config_module.os.environ["SPARK_EXECUTOR_MEMORY"]
    if "SPARK_DRIVER_MEMORY" in config_module.os.environ: del config_module.os.environ["SPARK_DRIVER_MEMORY"]
    if "SILVER_DB_PASSWORD" in config_module.os.environ: del config_module.os.environ["SILVER_DB_PASSWORD"]

    try:
        settings = Settings()
        test_data_root = Path(test_data_root_str)

        expected_paths = {
            "data_root": test_data_root_str,
            "logs_root": str(PROJECT_ROOT / "logs"),
            "raw_data": str(test_data_root / "raw"),
            "processed_data": str(test_data_root / "processed"),
            "output_data": str(test_data_root / "output"),
            "staging_data": str(test_data_root / "staging"),
        }
        assert settings.get_paths() == expected_paths
    finally:
        for k, v in original_environ_keys.items():
            if v is None:
                if k in config_module.os.environ: del config_module.os.environ[k]
            else:
                config_module.os.environ[k] = v


# Abordagem revisada
def test_settings_load_environment_overrides_db_password(mocker): # Nome limpo
    """Testa se a variável de ambiente SILVER_DB_PASSWORD sobrescreve a senha."""

    original_environ_keys = {
        "SILVER_DB_PASSWORD": config_module.os.environ.get("SILVER_DB_PASSWORD"),
        "SPARK_MASTER": config_module.os.environ.get("SPARK_MASTER"), # para isolamento
        "SPARK_EXECUTOR_MEMORY": config_module.os.environ.get("SPARK_EXECUTOR_MEMORY"), # para isolamento
        "SPARK_DRIVER_MEMORY": config_module.os.environ.get("SPARK_DRIVER_MEMORY"), # para isolamento
        "DATA_ROOT": config_module.os.environ.get("DATA_ROOT") # para isolamento
    }

    config_module.os.environ["SILVER_DB_PASSWORD"] = "new_secret_password_revised"

    # Garante que outras chaves de override estejam indefinidas
    if "SPARK_MASTER" in config_module.os.environ: del config_module.os.environ["SPARK_MASTER"]
    if "SPARK_EXECUTOR_MEMORY" in config_module.os.environ: del config_module.os.environ["SPARK_EXECUTOR_MEMORY"]
    if "SPARK_DRIVER_MEMORY" in config_module.os.environ: del config_module.os.environ["SPARK_DRIVER_MEMORY"]
    if "DATA_ROOT" in config_module.os.environ: del config_module.os.environ["DATA_ROOT"]

    try:
        settings = Settings()
        assert settings.get("database.silver_db_password") == "new_secret_password_revised"
        assert settings.get("database.silver_db_user") == DEFAULT_CONFIG["database"]["silver_db_user"]
    finally:
        for k, v in original_environ_keys.items():
            if v is None:
                if k in config_module.os.environ: del config_module.os.environ[k]
            else:
                config_module.os.environ[k] = v


def test_settings_get_existing_path(default_settings):
    """Testa o método get para um caminho existente."""
    assert default_settings.get("spark.app_name") == "LogAnalyzer"
    assert default_settings.get("log_formats.apache_common.pattern") == DEFAULT_CONFIG["log_formats"]["apache_common"]["pattern"]


def test_settings_get_non_existing_path_with_default(default_settings):
    """Testa o método get para um caminho não existente com valor padrão."""
    assert default_settings.get("non.existent.path", "default_val") == "default_val"


def test_settings_get_non_existing_path_without_default(default_settings):
    """Testa o método get para um caminho não existente sem valor padrão (deve retornar None)."""
    assert default_settings.get("non.existent.path") is None


def test_settings_set_new_path(default_settings):
    """Testa o método set para um novo caminho."""
    default_settings.set("new.path.to.value", "new_value")
    assert default_settings.get("new.path.to.value") == "new_value"


def test_settings_set_existing_path(default_settings):
    """Testa o método set para um caminho existente."""
    default_settings.set("spark.app_name", "NewAppName")
    assert default_settings.get("spark.app_name") == "NewAppName"


def test_get_spark_config(default_settings):
    """Testa o método get_spark_config."""
    assert default_settings.get_spark_config() == DEFAULT_CONFIG["spark"]


def test_get_paths(default_settings):
    """Testa o método get_paths."""
    assert default_settings.get_paths() == DEFAULT_CONFIG["paths"]


def test_get_log_formats(default_settings):
    """Testa o método get_log_formats."""
    assert default_settings.get_log_formats() == DEFAULT_CONFIG["log_formats"]


def test_to_dict(default_settings):
    """Testa o método to_dict."""
    assert default_settings.to_dict() == DEFAULT_CONFIG


@patch("log_analyzer.core.config.Path.mkdir")
# @patch.dict(config_module.os.environ, {}, clear=True) # Comentado para nova abordagem
def test_create_directories_called_correctly(mock_mkdir, mocker): # Nome limpo
    """
    Testa se create_directories chama Path.mkdir com os caminhos corretos.
    Este teste precisa recarregar o módulo config_module para re-executar create_directories.
    """
    # Simula os caminhos que create_directories deve usar
    # É importante que estes caminhos correspondam ao DEFAULT_CONFIG
    # já que limpamos as variáveis de ambiente.

    # Precisamos recarregar o módulo para que create_directories seja chamado novamente
    # com o mock_mkdir ativo.

    # Para garantir que estamos usando os caminhos padrão, precisamos de uma instância de Settings
    # que não seja afetada por variáveis de ambiente de override de outros testes.
    # A maneira mais segura é garantir que essas variáveis não estejam definidas.

    original_environ_keys = {
        "DATA_ROOT": config_module.os.environ.get("DATA_ROOT"),
        # Adicione outras vars se Settings._load_environment_overrides as usar para caminhos
    }

    # Garante que as variáveis de override de caminho não estejam definidas
    if "DATA_ROOT" in config_module.os.environ:
        del config_module.os.environ["DATA_ROOT"]

    try:
        # Criamos uma instância fresca de Settings para garantir que os caminhos são os default
        # Esta instância será usada por config_module.settings globalmente se recarregarmos o módulo,
        # ou podemos mockar config_module.settings para usar esta instância.
        # No entanto, create_directories usa a instância global config_module.settings.
        # Então, precisamos garantir que config_module.settings seja "resetado" para este teste.
        # Uma forma é recarregar o módulo config_module, mas isso pode ser complexo.
        # Alternativamente, podemos alterar temporariamente config_module.settings.

        original_global_settings_config = config_module.settings._config.copy()

        # Força a instância global de settings a usar a configuração padrão (sem overrides de env)
        # para este teste específico.
        config_module.settings._config = DEFAULT_CONFIG.copy() # Reset para o padrão
        config_module.settings._load_environment_overrides() # Recarrega overrides (que devem ser nulos agora)

        paths_from_default_config = []
        for path_key, path_value in DEFAULT_CONFIG["paths"].items():
             if path_key.endswith("_data") or path_key.endswith("_root"):
                paths_from_default_config.append(Path(path_value))

        # Chamamos explicitamente create_directories, que usa o config_module.settings global
        config_module.create_directories()

        # A verificação mais simples é a contagem de chamadas.
        assert mock_mkdir.call_count == len(paths_from_default_config)

        # Para verificar os caminhos específicos, precisaríamos de um mock mais sofisticado em Path
        # ou inspecionar os `self` das chamadas a mkdir, o que é complicado com o mock atual.
        # No entanto, podemos verificar se os diretórios esperados foram "tentados".
        # Se mock_mkdir.call_args_list[i][0][0] fosse o objeto Path, poderíamos verificar.
        # Mas como mock_mkdir é o próprio método, não temos acesso fácil ao `self` da instância Path.

        # Uma verificação possível é garantir que Path(expected_path).mkdir foi chamado.
        # Isso exigiria mockar o construtor de Path para retornar mocks.
        # Ex: with patch('log_analyzer.core.config.Path') as MockPathClass:
        #         mock_path_instance = MockPathClass.return_value
        #         mock_path_instance.mkdir = MagicMock()
        #         ...
        #         calls = [call(Path(p)) for p in paths_from_default_config]
        #         MockPathClass.assert_has_calls(calls, any_order=True)
        #         assert mock_path_instance.mkdir.call_count == len(paths_from_default_config)

    finally:
        # Restaura a configuração global de settings
        config_module.settings._config = original_global_settings_config
        config_module.settings._load_environment_overrides() # Recarrega os overrides originais

        # Restaura as variáveis de ambiente
        for k, v in original_environ_keys.items():
            if v is None:
                if k in config_module.os.environ:
                    del config_module.os.environ[k]
            else:
                config_module.os.environ[k] = v


# Testes adicionais podem ser necessários se houver mais lógica condicional
# ou interações complexas com o ambiente ou outras partes do sistema.

# Por exemplo, testar o comportamento se uma variável de ambiente DATA_ROOT
# for um caminho inválido, embora o código atual não tenha tratamento de erro explícito para isso.
# A classe Settings é relativamente simples e focada na leitura de configurações.
# A função create_directories tem um efeito colateral (criação de diretórios),
# que tentamos isolar com mocks.
# A instância global `settings` e a chamada `create_directories()` no final do módulo
# são padrões que podem tornar o teste um pouco mais complicado, pois
# o estado pode vazar entre os testes ou a importação do módulo já pode ter efeitos.
# Usar fixtures para fornecer instâncias limpas de Settings e, quando necessário,
# recarregar módulos ou mockar em um escopo mais amplo pode ajudar.
# O uso de `@patch.dict(config_module.os.environ, {}, clear=True)` é importante
# para isolar os testes de variáveis de ambiente externas.

# Testando o `get_settings()`
def test_get_settings_returns_singleton_instance():
    s1 = config_module.get_settings()
    s2 = config_module.get_settings()
    assert s1 is s2
    assert s1 is config_module.settings # Verifica se é a instância global

    # Modificando através de uma referência e verificando através de outra
    s1.set("test.newkey", "testvalue_singleton")
    assert s2.get("test.newkey") == "testvalue_singleton"
    # Limpando para não afetar outros testes, embora o ideal seja que cada teste use sua própria instância ou mock.
    # Como `settings` é global, precisamos ter cuidado.
    # Uma melhoria seria `get_settings` talvez retornar uma cópia ou ter um gerenciador de contexto para testes.
    # Por enquanto, este teste verifica o comportamento atual.
    # Para limpar, podemos recarregar o módulo ou remover a chave.
    # Vamos remover a chave para simplicidade aqui.
    del config_module.settings._config["test"] # Acesso direto para limpeza

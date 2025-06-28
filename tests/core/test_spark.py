import pytest
from unittest.mock import patch, MagicMock, call
import os

# Importar o módulo e classes a serem testadas
from log_analyzer.core import spark as spark_module
from log_analyzer.core.spark import SparkSessionFactory, get_spark_session, stop_spark_session

# Variável global para manter o estado do mock da SparkSession entre chamadas dentro de um teste
# Isso é necessário porque SparkSessionFactory é um singleton.
mock_spark_session_instance = None

@pytest.fixture(autouse=True)
def reset_spark_session_factory_singleton():
    """
    Fixture para resetar o estado da SparkSessionFactory antes de cada teste.
    Isso é crucial porque a SparkSessionFactory é um singleton e seu estado
    (cls._instance) persistiria entre os testes, afetando os resultados.
    Tamb{e}m reseta a variável global mock_spark_session_instance.
    """
    global mock_spark_session_instance
    if SparkSessionFactory._instance is not None:
        # Tenta parar a sessão real se ela existir e não for um mock
        if hasattr(SparkSessionFactory._instance, 'stop') and not isinstance(SparkSessionFactory._instance, MagicMock):
            SparkSessionFactory._instance.stop()
        SparkSessionFactory._instance = None
    mock_spark_session_instance = None
    yield # Permite que o teste execute
    # Limpeza após o teste, se necessário, mas o reset no início é mais importante.
    if SparkSessionFactory._instance is not None:
        if hasattr(SparkSessionFactory._instance, 'stop') and not isinstance(SparkSessionFactory._instance, MagicMock):
            SparkSessionFactory._instance.stop()
        SparkSessionFactory._instance = None
    mock_spark_session_instance = None


@patch('log_analyzer.core.spark.SparkSession', autospec=True)
def test_get_spark_session_creates_new_session_first_call(MockSparkSessionClass):
    global mock_spark_session_instance

    mock_builder = MockSparkSessionClass.builder
    mock_builder.appName.return_value = mock_builder
    mock_builder.master.return_value = mock_builder
    mock_builder.config.return_value = mock_builder

    first_session_mock = MagicMock()
    first_session_mock.sparkContext = MagicMock()
    mock_builder.getOrCreate.return_value = first_session_mock
    mock_spark_session_instance = first_session_mock # Store for global access if needed by other parts of test

    # Chama a função pela primeira vez
    session = SparkSessionFactory.get_spark_session(app_name="TestApp1")

    # Verifica se o builder foi chamado com os parâmetros corretos
    MockSparkSessionClass.builder.appName.assert_called_with("TestApp1")
    # Default master is 'local[*]' if SPARK_MASTER_URL is not set
    # Check the default call to master without specific env var first
    with patch.dict(os.environ, {}, clear=True): # Ensure SPARK_MASTER_URL is not set
         # Reset mocks if asserting calls for this specific section
        mock_builder.master.reset_mock()
        mock_builder.getOrCreate.reset_mock() # Reset to ensure we're checking this call
        mock_builder.getOrCreate.return_value = first_session_mock # Re-assign

        # Need to reset SparkSessionFactory._instance for a clean get_spark_session call
        SparkSessionFactory._instance = None
        session_default_master = SparkSessionFactory.get_spark_session(app_name="TestAppDefaultMaster")
        mock_builder.master.assert_called_with("local[*]")
        assert session_default_master is first_session_mock # ensure this is the one we configured

    # Verifica se getOrCreate foi chamado for the original "TestApp1" call
    # The getOrCreate mock would have been called multiple times by now.
    # We need to be careful about asserting call_count or assert_called_once without resets.
    # For the very first session ("TestApp1"):
    assert session is first_session_mock # Check instance
    first_session_mock.sparkContext.setLogLevel.assert_called_with("WARN") # Check setLogLevel

    # Test with SPARK_MASTER_URL set
    mock_builder.appName.reset_mock()
    mock_builder.master.reset_mock()
    mock_builder.config.reset_mock()
    mock_builder.getOrCreate.reset_mock()

    env_session_mock = MagicMock()
    env_session_mock.sparkContext = MagicMock()
    mock_builder.getOrCreate.return_value = env_session_mock
    mock_spark_session_instance = env_session_mock

    with patch.dict(os.environ, {"SPARK_MASTER_URL": "test_master_url"}, clear=True):
        # Ensure SparkSessionFactory creates a new session for this check
        SparkSessionFactory._instance = None
        session_with_env_master = SparkSessionFactory.get_spark_session(app_name="TestAppEnv", force_new=False) # force_new=False to use existing logic path initially
        mock_builder.master.assert_called_with("test_master_url")
        assert session_with_env_master is env_session_mock
        env_session_mock.sparkContext.setLogLevel.assert_called_with("WARN")
        mock_builder.getOrCreate.assert_called_once() # Check getOrCreate for this specific call


@patch('log_analyzer.core.spark.SparkSession', autospec=True)
def test_get_spark_session_returns_existing_session(MockSparkSessionClass):
    global mock_spark_session_instance

    mock_builder = MockSparkSessionClass.builder
    mock_builder.appName.return_value = mock_builder
    mock_builder.master.return_value = mock_builder
    mock_builder.config.return_value = mock_builder

    # Configure for the first call
    initial_session_mock = MagicMock()
    initial_session_mock.sparkContext = MagicMock()
    mock_builder.getOrCreate.return_value = initial_session_mock
    mock_spark_session_instance = initial_session_mock

    # Primeira chamada para criar a sessão
    session1 = SparkSessionFactory.get_spark_session(app_name="TestAppSingleton")

    # Segunda chamada - getOrCreate should not be called again
    session2 = SparkSessionFactory.get_spark_session(app_name="TestAppSingleton")

    # Verifica se a mesma instância é retornada
    assert session1 is session2
    assert session1 is initial_session_mock # Ensure it's the one we set up
    # Verifica se getOrCreate foi chamado apenas uma vez (na primeira chamada)
    mock_builder.getOrCreate.assert_called_once()


@patch('log_analyzer.core.spark.SparkSession', autospec=True)
def test_get_spark_session_with_custom_params(MockSparkSessionClass):
    global mock_spark_session_instance

    mock_builder = MockSparkSessionClass.builder
    mock_builder.appName.return_value = mock_builder
    mock_builder.master.return_value = mock_builder
    mock_builder.config.return_value = mock_builder # All .config calls return the builder

    custom_session_mock = MagicMock()
    custom_session_mock.sparkContext = MagicMock()
    mock_builder.getOrCreate.return_value = custom_session_mock
    mock_spark_session_instance = custom_session_mock

    SparkSessionFactory.get_spark_session(
        app_name="CustomApp",
        master_url="custom_master",
        executor_memory="8g",
        driver_memory="4g",
        executor_cores="4"
    )

    mock_builder.appName.assert_called_with("CustomApp")
    mock_builder.master.assert_called_with("custom_master")

    expected_config_calls = [
        call("spark.executor.memory", "8g"),
        call("spark.driver.memory", "4g"),
        call("spark.executor.cores", "4"),
        call("spark.sql.adaptive.enabled", "true"),
        call("spark.sql.adaptive.coalescePartitions.enabled", "true"),
        call("spark.sql.adaptive.skewJoin.enabled", "true"),
        call("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    ]
    # mock_builder.config is the mock for the config method. We check its call_args_list.
    mock_builder.config.assert_has_calls(expected_config_calls, any_order=False)
    mock_builder.getOrCreate.assert_called_once()
    custom_session_mock.sparkContext.setLogLevel.assert_called_with("WARN")


@patch.dict(os.environ, {"SPARK_MASTER_URL": "env_master_url_test"}, clear=True)
@patch('log_analyzer.core.spark.SparkSession', autospec=True)
def test_get_spark_session_uses_env_variable_for_master_url(MockSparkSessionClass):
    global mock_spark_session_instance

    mock_builder = MockSparkSessionClass.builder
    mock_builder.appName.return_value = mock_builder
    mock_builder.master.return_value = mock_builder
    mock_builder.config.return_value = mock_builder

    env_var_session_mock = MagicMock()
    env_var_session_mock.sparkContext = MagicMock()
    mock_builder.getOrCreate.return_value = env_var_session_mock
    mock_spark_session_instance = env_var_session_mock

    SparkSessionFactory.get_spark_session(app_name="TestAppEnvMaster")

    mock_builder.master.assert_called_with("env_master_url_test")
    mock_builder.getOrCreate.assert_called_once()
    env_var_session_mock.sparkContext.setLogLevel.assert_called_with("WARN")


@patch('log_analyzer.core.spark.SparkSession', autospec=True)
def test_get_spark_session_force_new_stops_existing_and_creates_new(MockSparkSessionClass):
    global mock_spark_session_instance

    mock_builder = MockSparkSessionClass.builder
    mock_builder.appName.return_value = mock_builder
    mock_builder.master.return_value = mock_builder
    mock_builder.config.return_value = mock_builder

    # Primeira sessão
    first_session_mock = MagicMock()
    first_session_mock.sparkContext = MagicMock()
    mock_builder.getOrCreate.return_value = first_session_mock
    mock_spark_session_instance = first_session_mock

    session1 = SparkSessionFactory.get_spark_session(app_name="AppForForceNew1")
    assert SparkSessionFactory._instance is first_session_mock
    mock_builder.getOrCreate.assert_called_once() # Called once for session1

    # Segunda sessão com force_new=True
    second_session_mock = MagicMock()
    second_session_mock.sparkContext = MagicMock()
    mock_builder.getOrCreate.return_value = second_session_mock # Next call to getOrCreate returns this
    mock_spark_session_instance = second_session_mock # Update global

    session2 = SparkSessionFactory.get_spark_session(app_name="AppForForceNew2", force_new=True)

    # Verifica se a primeira sessão foi parada
    first_session_mock.stop.assert_called_once()
    # Verifica se getOrCreate foi chamado novamente (total de duas vezes)
    assert mock_builder.getOrCreate.call_count == 2
    # Verifica se a nova sessão é a segunda mockada
    assert session2 is second_session_mock
    assert SparkSessionFactory._instance is second_session_mock
    second_session_mock.sparkContext.setLogLevel.assert_called_with("WARN")


@patch('log_analyzer.core.spark.SparkSession', autospec=True)
def test_get_spark_session_force_new_creates_new_if_none_exists(MockSparkSessionClass):
    global mock_spark_session_instance
    SparkSessionFactory._instance = None # Ensure no session exists
    mock_spark_session_instance = None

    mock_builder = MockSparkSessionClass.builder
    mock_builder.appName.return_value = mock_builder
    mock_builder.master.return_value = mock_builder
    mock_builder.config.return_value = mock_builder

    new_session_mock = MagicMock()
    new_session_mock.sparkContext = MagicMock()
    mock_builder.getOrCreate.return_value = new_session_mock
    mock_spark_session_instance = new_session_mock

    session = SparkSessionFactory.get_spark_session(app_name="AppForceNewNoExisting", force_new=True)

    mock_builder.getOrCreate.assert_called_once()
    assert session is new_session_mock
    assert SparkSessionFactory._instance is new_session_mock
    new_session_mock.stop.assert_not_called() # Nenhuma sessão anterior para parar
    new_session_mock.sparkContext.setLogLevel.assert_called_with("WARN")


def test_stop_session_stops_active_session():
    global mock_spark_session_instance
    # Cria uma sessão mockada e a define como ativa
    mock_session = MagicMock()
    SparkSessionFactory._instance = mock_session
    mock_spark_session_instance = mock_session

    SparkSessionFactory.stop_session()

    mock_session.stop.assert_called_once()
    assert SparkSessionFactory._instance is None
    # mock_spark_session_instance would still refer to mock_session here.
    # The important part is that SparkSessionFactory._instance is None.
    # The global mock_spark_session_instance is reset by the fixture for the next test.


def test_stop_session_does_nothing_if_no_active_session():
    SparkSessionFactory._instance = None
    global mock_spark_session_instance
    mock_spark_session_instance = None

    try:
        SparkSessionFactory.stop_session()
    except Exception as e:
        pytest.fail(f"stop_session raised an exception when no session was active: {e}")
    assert SparkSessionFactory._instance is None


@patch.object(SparkSessionFactory, 'get_spark_session', wraps=SparkSessionFactory.get_spark_session)
def test_convenience_get_spark_session_calls_factory_method(mock_factory_get_session_wrapper):
    # For this test, we don't need to mock the internal SparkSession builder.
    # We just need to ensure our convenience function calls the factory method.
    # The `reset_spark_session_factory_singleton` fixture will clear _instance.
    # We are wrapping the actual method, so it will try to create a SparkSession.
    # To prevent actual Spark logic, we can patch SparkSession itself here too.

    # Simplest: mock the target method directly on the class.
    with patch.object(SparkSessionFactory, 'get_spark_session') as mock_method:
        mock_method.return_value = MagicMock() # return a dummy session
        get_spark_session(app_name="ConvenienceApp")
        mock_method.assert_called_once_with(app_name="ConvenienceApp")


@patch.object(SparkSessionFactory, 'stop_session', wraps=SparkSessionFactory.stop_session)
def test_convenience_stop_spark_session_calls_factory_method(mock_factory_stop_session_wrapper):
    # Similar to above, mock the target method directly.
    with patch.object(SparkSessionFactory, 'stop_session') as mock_method:
        stop_spark_session()
        mock_method.assert_called_once()

# Nota sobre a fixture `reset_spark_session_factory_singleton`:
# É crucial para a confiabilidade desses testes, dado o padrão Singleton da SparkSessionFactory.
# Sem ele, o estado da sessão (cls._instance) de um teste poderia vazar para o próximo,
# levando a resultados de teste não determinísticos ou incorretos.
# O `autouse=True` garante que ele seja executado para cada teste neste arquivo.
# A variável global `mock_spark_session_instance` é uma tentativa de ajudar a coordenar
# o mock da sessão entre as chamadas dentro de um mesmo teste, quando `get_spark_session`
# é chamado múltiplas vezes e esperamos que a mesma instância mockada seja retornada.

# Ajuste no mock da cadeia de builder para ser mais robusto
# Pode ser melhor mockar cada chamada na cadeia para retornar o mock anterior, e.g.,
# mock_builder = MagicMock()
# MockSparkSessionClass.builder = mock_builder # This is incorrect, builder is a property returning a new mock.
                                            # MockSparkSessionClass.builder should be the mock builder itself.

# A estratégia correta é:
# mock_builder = MockSparkSessionClass.builder (este é o mock do objeto builder)
# mock_builder.appName.return_value = mock_builder (configura o método appName no mock_builder)
# mock_builder.master.return_value = mock_builder (configura o método master no mock_builder)
# etc.

# Considerar testar o log level setado no sparkContext (já está em alguns testes).
# Considerar testar as configurações padrão se nenhuma for passada para get_spark_session.

# Teste para verificar as configurações padrão quando nenhuma é explicitamente passada
@patch('log_analyzer.core.spark.SparkSession', autospec=True)
def test_get_spark_session_default_parameters(MockSparkSessionClass):
    global mock_spark_session_instance

    mock_builder = MockSparkSessionClass.builder
    mock_builder.appName.return_value = mock_builder
    mock_builder.master.return_value = mock_builder
    mock_builder.config.return_value = mock_builder

    default_session_mock = MagicMock()
    default_session_mock.sparkContext = MagicMock()
    mock_builder.getOrCreate.return_value = default_session_mock
    mock_spark_session_instance = default_session_mock

    with patch.dict(os.environ, {}, clear=True): # Garante que SPARK_MASTER_URL não está setado
        SparkSessionFactory.get_spark_session() # Chama com todos os padrões

    mock_builder.appName.assert_called_with("LogAnalyzer") # app_name padrão
    mock_builder.master.assert_called_with("local[*]") # master_url padrão

    expected_config_calls = [
        call("spark.executor.memory", "2g"),
        call("spark.driver.memory", "1g"),
        call("spark.executor.cores", "2"),
        call("spark.sql.adaptive.enabled", "true"),
        call("spark.sql.adaptive.coalescePartitions.enabled", "true"),
        call("spark.sql.adaptive.skewJoin.enabled", "true"),
        call("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    ]
    mock_builder.config.assert_has_calls(expected_config_calls, any_order=False)
    mock_builder.getOrCreate.assert_called_once()
    default_session_mock.sparkContext.setLogLevel.assert_called_with("WARN")

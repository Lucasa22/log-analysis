# ==============================================================================
# Dockerfile Final para o Desafio de Análise de Logs
# Baseado na imagem do Airflow, com todas as correções de permissão
# ==============================================================================

# Use a imagem base do Airflow que corresponde à sua versão do Python e Airflow
FROM apache/airflow:2.8.2-python3.9

# Mude para o usuário root para instalar dependências do sistema
USER root

# Instale as dependências do sistema necessárias.
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    default-jdk \
    && rm -rf /var/lib/apt/lists/*

# Define JAVA_HOME explicitamente para garantir que o Spark o encontre corretamente.
# Na imagem base Debian/Ubuntu, este é o caminho padrão.
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$JAVA_HOME/bin:$PATH

# Copie todo o contexto do projeto para o diretório de trabalho no container
# Fazemos isso como root para garantir que a cópia seja bem-sucedida
COPY . /opt/airflow/log-analysis

# --- CORREÇÃO DE PERMISSÃO FINAL ---
# Mude a propriedade dos arquivos para o usuário 'airflow' e grupo 'root' (GID 0)
# Esta é a configuração correta para a imagem oficial do Airflow.
RUN chown -R airflow:root /opt/airflow/log-analysis

# Mude de volta para o usuário padrão do Airflow para segurança
USER airflow

# Defina o diretório de trabalho
WORKDIR /opt/airflow/log-analysis

# --- BOA PRÁTICA ADICIONADA ---
# Definir o PYTHONPATH torna explícito para todas as ferramentas
# que nosso código-fonte está no caminho de importação.
ENV PYTHONPATH=/opt/airflow/log-analysis:$PYTHONPATH

# Copie o arquivo de requisitos e instale as dependências Python
# O WORKDIR já está definido, então ele copia para o lugar certo.
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Instale o projeto 'log_analyzer' em modo editável.
# O usuário 'airflow' agora tem permissão para escrever o diretório .egg-info.
RUN pip install -e .
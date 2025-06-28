#!/bin/bash

# Script de inicialização do projeto log-analysis
# Autor: Equipe de Dados
# Data: 27/06/2025

# Exit on error
set -e

# Cores para formatação
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}=======================================================${NC}"
echo -e "${BLUE}           INICIALIZAÇÃO DO LOG ANALYSIS CHALLENGE           ${NC}"
echo -e "${BLUE}=======================================================${NC}"

# Verificar se o Docker está instalado
echo -e "\n${YELLOW}[1/5] Verificando requisitos...${NC}"
if ! command -v docker &> /dev/null; then
    echo -e "${RED}Docker não encontrado. Por favor, instale o Docker primeiro.${NC}"
    exit 1
fi

if ! command -v docker-compose &> /dev/null; then
    echo -e "${RED}Docker Compose não encontrado. Por favor, instale o Docker Compose primeiro.${NC}"
    exit 1
fi

echo -e "${GREEN}✓ Docker e Docker Compose instalados${NC}"

# Criar diretórios necessários
echo -e "\n${YELLOW}[2/5] Criando estrutura de diretórios...${NC}"
mkdir -p data/bronze
mkdir -p data/silver
mkdir -p data/gold
mkdir -p data/test
mkdir -p jars

echo -e "${GREEN}✓ Estrutura de diretórios criada${NC}"

# Download do driver JDBC do PostgreSQL (se não existir)
echo -e "\n${YELLOW}[3/5] Verificando driver JDBC do PostgreSQL...${NC}"
if [ ! -f "jars/postgresql-42.7.3.jar" ]; then
    echo "Baixando driver JDBC do PostgreSQL..."
    curl -L "https://jdbc.postgresql.org/download/postgresql-42.7.3.jar" -o jars/postgresql-42.7.3.jar
fi

echo -e "${GREEN}✓ Driver JDBC encontrado ou baixado${NC}"

# Criar um arquivo de log de teste se não existir
echo -e "\n${YELLOW}[4/5] Verificando arquivo de logs de teste...${NC}"
if [ ! -f "data/logs.txt" ] && [ ! -L "data/logs.txt" ]; then
    echo "Criando arquivo de logs de teste..."
    cat > data/test/dummy_logs.txt << EOF
192.168.1.1 - - [27/Jun/2025:10:00:00 +0000] "GET /home HTTP/1.1" 200 1024 "https://example.com" "Mozilla/5.0"
192.168.1.2 - - [27/Jun/2025:10:01:00 +0000] "GET /about HTTP/1.1" 200 2048 "https://example.com" "Mozilla/5.0"
192.168.1.1 - - [27/Jun/2025:10:02:00 +0000] "POST /login HTTP/1.1" 302 0 "https://example.com" "Mozilla/5.0"
192.168.1.3 - - [27/Jun/2025:10:03:00 +0000] "GET /dashboard HTTP/1.1" 200 4096 "https://example.com" "Mozilla/5.0"
192.168.1.4 - - [27/Jun/2025:10:04:00 +0000] "GET /static/css/main.css HTTP/1.1" 200 1024 "https://example.com" "Mozilla/5.0"
192.168.1.5 - - [27/Jun/2025:10:05:00 +0000] "GET /api/data HTTP/1.1" 404 0 "https://example.com" "Mozilla/5.0"
EOF

    # Criar um link simbólico para facilitar o acesso
    cp "data/test/dummy_logs.txt" "data/logs.txt"
fi

echo -e "${GREEN}✓ Arquivo de logs de teste pronto${NC}"

# Iniciar o ambiente Docker
echo -e "\n${YELLOW}[5/5] Iniciando o ambiente com Docker Compose...${NC}"
echo -e "${BLUE}(Este processo pode levar alguns minutos na primeira execução)${NC}"

# Criar apenas os volumes e a rede primeiro
docker-compose up --no-start

echo -e "${GREEN}✓ Ambiente preparado com sucesso!${NC}"

echo -e "\n${BLUE}=======================================================${NC}"
echo -e "${GREEN}           INICIALIZAÇÃO CONCLUÍDA!                      ${NC}"
echo -e "${BLUE}=======================================================${NC}"

echo -e "\nPara iniciar os serviços, execute:"
echo -e "${YELLOW}  docker-compose up -d${NC}"
echo -e "\nPara acessar os serviços:"
echo -e "${YELLOW}  Jupyter: http://localhost:8888    (token: easy)${NC}"
echo -e "${YELLOW}  Airflow: http://localhost:8080    (usuário: admin / senha: admin)${NC}"
echo -e "${YELLOW}  Spark:   http://localhost:8081${NC}"
echo -e "\n${BLUE}=======================================================${NC}"
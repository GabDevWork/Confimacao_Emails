# Base da imagem DBT Snowflake
FROM ghcr.io/dbt-labs/dbt-snowflake:1.8.latest

# Diretório de trabalho
WORKDIR /usr/datamart

# Variáveis de ambiente para DBT
ENV \
  DBT_PROFILES_DIR=/usr/datamart/.dbt/profiles \
  DBT_MODULES_DIR=/usr/datamart/.dbt/modules

# Copiar arquivos de configuração e credenciais abstratas
COPY ./<ARQUIVO_PROFILES>.yml ${DBT_PROFILES_DIR}/profiles.yml
COPY ./<ARQUIVO_CREDENCIAIS_EMAIL>.json ./
COPY ./ ./

# Garantir permissão de administrador para instalar dependências no Debian
USER root

# Atualizar e instalar dependências do sistema
RUN apt-get update && apt-get install -y \
    curl \
    unzip \
    python3 \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

# Instalar AWS CLI
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" \
    && unzip awscliv2.zip \
    && ./aws/install \
    && rm -rf awscliv2.zip ./aws

# Instalar pacotes do Python necessários para processamento e conexão
RUN pip install --upgrade pip && \
    pip install pandas sqlalchemy pymysql snowflake-snowpark-python snowflake-connector-python[pandas] google-api-python-client google-auth-httplib2 google-auth-oauthlib pyyaml openpyxl requests urllib3
    
# Garantir permissões de execução nos scripts Python
RUN find /usr/datamart -maxdepth 1 -name "*.py" -exec chmod +x {} \;

# Comando padrão
CMD ["dbt", "run"]
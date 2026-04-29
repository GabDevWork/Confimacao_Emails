import yaml
import json
import sys
import pandas as pd
import requests
import urllib3
from datetime import datetime
from snowflake.snowpark.session import Session

# ==============================================================================
# 1. CONEXÃO SNOWFLAKE E SMTP
# ==============================================================================
def conectar_snowflake():
    print("🔌 Carregando credenciais do arquivo YAML...")
    try:
        with open("<ARQUIVO_PROFILES>.yml", "r") as file:
            profile_data = yaml.safe_load(file)

        snowflake_params = profile_data["credentials"]["outputs"]["prod"]

        snowflake_config = {
            "account": snowflake_params.get("account", ""),
            "user": snowflake_params.get("user", ""),
            "password": snowflake_params.get("password", ""),
            "warehouse": snowflake_params.get("warehouse", ""),
            "database": snowflake_params.get("database", "<DATABASE>"),
            "schema": "<SCHEMA_BRONZE>",
            "role": snowflake_params.get("role", "<ROLE_NAME>"), 
            "client_session_keep_alive": True,
            "network_timeout": 300,
            "retry_attempts": 10
        }
        
        session = Session.builder.configs(snowflake_config).create()
        print("✅ Conexão Snowflake estabelecida com sucesso!")
        return session
    except Exception as e:
        print(f"❌ Erro ao conectar no Snowflake: {e}")
        raise

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def carregar_credenciais_email():
    print("🔐 Carregando credenciais do email...")
    try:
        with open("<ARQUIVO_CREDENCIAIS_EMAIL>.json", "r") as file:
            email_config = json.load(file)
        print("✅ Credenciais do email carregadas com sucesso!")
        return email_config
    except Exception as e:
        print(f"❌ Erro ao carregar arquivo de credenciais de e-mail: {e}")
        raise

def enviar_email(assunto, corpo, destinatarios, credenciais_email, is_html=True):
    print(f"📧 Autenticando na API de e-mail...")
    
    TOKEN_URL = "https://<API_DOMAIN>/auth/oauth/v2/token"
    payload = {
        "client_id": credenciais_email["client_id"],
        "client_secret": credenciais_email["client_secret"],
        "scope": "all",
        "grant_type": "client_credentials"
    }

    try:
        token_response = requests.post(TOKEN_URL, data=payload, verify=False)
        token_response.raise_for_status()
        access_token = token_response.json().get("access_token")
        
        API_URL = "https://<API_DOMAIN>/v1/emails"
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {access_token}"
        }
        
        destinatario_principal = destinatarios[0]
        destinatarios_cc = ", ".join(destinatarios[1:]) if len(destinatarios) > 1 else ""

        email_payload = {
            "emailRemetente": "<EMAIL_REMETENTE>",
            "emailDestinatario": destinatario_principal,
            "emailDestinatarioCopia": destinatarios_cc,
            "assunto": assunto,
            "formatoHTML": is_html,
            "corpoEmail": corpo,
        }

        print(f"📤 Disparando e-mail: {assunto}")
        response = requests.post(API_URL, headers=headers, json=email_payload, verify=False)
        
        if response.status_code == 200:
            print("✔️ E-mail enviado com sucesso!")
        else:
            print(f"❌ Erro da API ao enviar o e-mail: {response.status_code} - {response.text}")
            
    except Exception as e:
        print(f"❌ Falha crítica no módulo de envio de e-mail: {e}")

# ==============================================================================
# 2. VALIDAÇÃO DE EXECUÇÃO
# ==============================================================================
def decide_se_roda_hoje(session):
    print("📅 Verificando se o processo deve rodar hoje...")
    
    try:
        query_calendario = """
            SELECT DIA_UTIL, DIA_SEMANA 
            FROM <DATABASE>.<SCHEMA_SILVER>.<TABLE_CALENDARIO> 
            WHERE DATA_MOVIMENTO = CURRENT_DATE();
        """
        
        resultado_cal = session.sql(query_calendario).collect()
        
        if not resultado_cal:
            print("⚠️ Calendário não retornou dados para a data de hoje. Abortando por segurança.")
            sys.exit(0)
            
        dia_util = str(resultado_cal[0][0]).strip().upper()
        dia_semana = str(resultado_cal[0][1]).strip().upper()
        
        if dia_util == 'SIM':
            print("✅ Dia útil detectado. O processo deve rodar hoje. Continuando...")
            return

        if dia_semana == 'SAB':
            print("📅 Sábado detectado. Verificando se há programação de abastecimento...")
            query_abastec = """
                SELECT COUNT(1)
                FROM <DATABASE>.<SCHEMA_SILVER>.<TABLE_PROGRAMACAO> A
                INNER JOIN <DATABASE>.<SCHEMA_SILVER>.<TABLE_AGENCIA> B 
                    ON B.IDT_TML_DND = A.IDT_TML_DND
                WHERE CAST(A.DTA_HOR_PRG AS DATE) = CURRENT_DATE();
            """
            
            resultado_abastec = session.sql(query_abastec).collect()
            qtde_prg = int(resultado_abastec[0][0]) if resultado_abastec else 0
            
            if qtde_prg > 0:
                print(f"✅ Sábado com programação ({qtde_prg} terminais). O processo deve rodar hoje.")
                return

        print("🛑 Dia não útil e sem programação para sábado. O processo não precisa rodar hoje.")
        sys.exit(0) 
            
    except Exception as e:
        print(f"❌ Erro ao verificar calendário/programação: {e}")
        raise

# ==============================================================================
# 3. INGESTÃO DE ARQUIVOS (STAGE ---> GCP)
# ==============================================================================
def carregar_previsoes_sftp(session, credenciais_email):
    print("🔄 Atualizando os metadados do Stage Externo...")
    try:
        session.sql("ALTER STAGE <DATABASE>.<SCHEMA_BRONZE>.<EXTERNAL_STAGE> REFRESH").collect()
    except Exception as e:
        print(f"❌ Erro crítico ao atualizar o Stage: {e}")
        raise

    print("🏗️ Criando Stage Interno Temporário para leitura dos arquivos...")
    stage_interno = "<DATABASE>.<SCHEMA_BRONZE>.<INTERNAL_TEMP_STAGE>"
    try:
        session.sql(f"CREATE OR REPLACE TEMPORARY STAGE {stage_interno}").collect()
    except Exception as e:
        print(f"❌ Erro ao criar Stage temporário: {e}")
        raise

    print("📂 Lendo planilhas em Excel via Stage Interno...")
    hoje = datetime.now()
    hoje_date = hoje.date()
    hoje_str = hoje.strftime("%d/%m/%Y")
    
    arquivos = {
        "FORNECEDOR_A": {
            "caminho_externo": "@<DATABASE>.<SCHEMA_BRONZE>.<EXTERNAL_STAGE>/caminho/A/arquivo_A.xlsx",
            "nome_arquivo": "arquivo_A.xlsx",
            "aba": "CONFIRMAÇÃO"
        },
        "FORNECEDOR_B": {
            "caminho_externo": "@<DATABASE>.<SCHEMA_BRONZE>.<EXTERNAL_STAGE>/caminho/B/arquivo_B.xlsx",
            "nome_arquivo": "arquivo_B.xlsx",
            "aba": "CONFIRMACOES"
        }
    }

    df_consolidado = pd.DataFrame()

    for transp, info in arquivos.items():
        try:
            print(f"Copiando arquivo de {transp}...")
            query_copy = f"COPY FILES INTO @{stage_interno} FROM '{info['caminho_externo']}'"
            session.sql(query_copy).collect()
            
            caminho_interno = f"@{stage_interno}/{info['nome_arquivo']}"
            file_stream = session.file.get_stream(caminho_interno)
            
            df = pd.read_excel(file_stream, sheet_name=info["aba"], header=0) 
            df = df.iloc[:, 0:5] 
            df.columns = ['DATA', 'FILIAL', 'NOME_PA', 'ATM', 'HORA']
            
            df = df.dropna(subset=['ATM'])
            
            datas_arquivo = pd.to_datetime(df['DATA'], errors='coerce', dayfirst=True)
            if datas_arquivo.dropna().empty:
                raise ValueError("A coluna DATA não contém datas válidas.")
            
            data_maxima_arquivo = datas_arquivo.max().date()
            if data_maxima_arquivo != hoje_date:
                raise ValueError(f"Arquivo desatualizado. Data esperada: {hoje_str}.")
            
            df['DATA'] = datas_arquivo.dt.strftime('%Y-%m-%d')
            df['HORA'] = pd.to_datetime(df['HORA'].astype(str), errors='coerce').dt.strftime('%H:%M:%S')
            df['TRANSPORTADORA'] = transp
            
            for col in df.columns:
                df[col] = df[col].astype(str).replace(['nan', 'NaT', 'None'], None)

            df_consolidado = pd.concat([df_consolidado, df], ignore_index=True)
            
        except Exception as e: 
            print(f"⚠️ Alerta para {transp}: {e}")
            enviar_email(
                assunto=f"Alerta Arquivo - {transp}",
                corpo=f"Erro ao processar: {e}",
                destinatarios=["<EMAIL_ALERTA_OPERACAO>"], 
                credenciais_email=credenciais_email, 
                is_html=False
            )
            
    print("💾 Gravando estrutura consolidada na tabela Stage...")
    session.sql("DROP TABLE IF EXISTS <DATABASE>.<SCHEMA_BRONZE>.<TABLE_STG_PREVISOES>").collect()

    if df_consolidado.empty:
        session.sql("""
            CREATE TABLE <DATABASE>.<SCHEMA_BRONZE>.<TABLE_STG_PREVISOES> (
                "DATA" VARCHAR, "FILIAL" VARCHAR, "NOME_PA" VARCHAR,
                "ATM" VARCHAR, "HORA" VARCHAR, "TRANSPORTADORA" VARCHAR
            )
        """).collect()
    else:
        df_consolidado = df_consolidado.astype(str)
        df_consolidado.columns = [str(col).upper().strip() for col in df_consolidado.columns]
        session.write_pandas(df_consolidado, "<TABLE_STG_PREVISOES>", auto_create_table=True, overwrite=True)

# ==============================================================================
# 4. TRANSFORMAÇÕES SQL
# ==============================================================================
def processar_dados_banco(session):
    print("⚙️ Executando transformações no Snowflake...")
    
    queries = {
        "1. Limpeza": """
            DELETE FROM <DATABASE>.<SCHEMA_BRONZE>.<TABLE_TEMP_1>;
        """,
        "2. Processamento": """
            INSERT INTO <DATABASE>.<SCHEMA_BRONZE>.<TABLE_TEMP_1> (...)
            WITH Excel_Deduplicado AS (
                SELECT ... FROM <DATABASE>.<SCHEMA_BRONZE>.<TABLE_STG_PREVISOES>
            ),
            Terminais_BD AS (
                SELECT ... FROM <DATABASE>.<SCHEMA_BRONZE>.<TABLE_DIM_TERMINAIS>
            )
            SELECT ... FROM Terminais_BD T LEFT JOIN Excel_Deduplicado E ON T.ID = E.ID;
        """
    }

    for etapa, query in queries.items():
        print(f"Executando: {etapa}")
        session.sql(query).collect()

# ==============================================================================
# 5. NOTIFICAÇÕES
# ==============================================================================
def notificar_pendencias_transportadoras(session, credenciais_email):
    print("🔍 Buscando pendências para notificar...")
    
    df_pendencias = pd.DataFrame()
    
    if df_pendencias.empty:
        return
        
    contatos = {
        "FORNECEDOR_A": ["contato_a1@fornecedor.com", "contato_a2@fornecedor.com"],
        "FORNECEDOR_B": ["contato_b1@fornecedor.com"]
    }
    
    for transp in df_pendencias['NOM_TRANSP'].unique():
        destinatarios = contatos.get(transp.upper(), ["<EMAIL_BACKUP>"]) 
        enviar_email(
            assunto=f"Previsões - {transp.capitalize()}", 
            corpo="<HTML_CORPO_EMAIL>", 
            destinatarios=destinatarios, 
            credenciais_email=credenciais_email, 
            is_html=True
        )

# ==============================================================================
# 6. MAIN
# ==============================================================================
def main():
    session = None
    credenciais_email = None
    try:
        session = conectar_snowflake()
        credenciais_email = carregar_credenciais_email()
        decide_se_roda_hoje(session)
        carregar_previsoes_sftp(session, credenciais_email)
        processar_dados_banco(session)
        notificar_pendencias_transportadoras(session, credenciais_email)

        enviar_email("Sucesso", "Processo finalizado.", ["<EMAIL_SUCESSO>"], credenciais_email, is_html=False)
    except SystemExit:
        pass
    except Exception as e:
        if credenciais_email:
            enviar_email("Erro", str(e), ["<EMAIL_ERRO>"], credenciais_email, is_html=False)
        raise
    finally:
        if session is not None: 
            session.close()

if __name__ == "__main__":
    main()
import pyodbc
import requests
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

# Carregar variáveis de ambiente do arquivo .env
load_dotenv()

# Configurações do banco de dados e API obtidas do .env
DB_CONFIG = {
    'server': os.getenv('DB_SERVER'),
    'database': os.getenv('DB_DATABASE'),
    'username': os.getenv('DB_USERNAME'),
    'password': os.getenv('DB_PASSWORD'),
}
API_BASE_URL = os.getenv('API_BASE_URL')

# Função para conectar ao banco de dados
def conectar_bd():
    connection_string = (
        f"DRIVER={{SQL Server}};"
        f"SERVER={DB_CONFIG['server']};"
        f"DATABASE={DB_CONFIG['database']};"
        f"UID={DB_CONFIG['username']};"
        f"PWD={DB_CONFIG['password']}"
    )
    return pyodbc.connect(connection_string)

# Função para obter os códigos de estação
def obter_codigos_estacao(conn):
    query = "SELECT codigo_hidro FROM estacoes"
    with conn.cursor() as cursor:
        cursor.execute(query)
        resultados = [row[0] for row in cursor.fetchall()]
    return resultados

# Função para chamar a API
def chamar_api(codigo_estacao, data):
    endpoints = [
        f"{API_BASE_URL}/api/v1/estacao-monitoramentos/telemetrica?codigo_estacao={codigo_estacao}&data={data}",
        f"{API_BASE_URL}/api/v1/estacao-monitoramentos/qualidade?codigo_estacao={codigo_estacao}&data={data}",
    ]

    for endpoint in endpoints:
        try:
            response = requests.get(endpoint)
            if response.status_code == 200:
                print(f"Sucesso: {endpoint}")
            else:
                print(f"Falha ({response.status_code}): {endpoint}")
        except Exception as e:
            print(f"Erro ao chamar {endpoint}: {e}")

# Script principal
def main():
    try:
        conn = conectar_bd()
        print("Conectado ao banco de dados com sucesso.")

        codigos_estacao = obter_codigos_estacao(conn)
        if not codigos_estacao:
            print("Nenhum código encontrado na tabela 'estacoes'.")
            return

        # Data de hoje menos um dia
        data = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

        for codigo in codigos_estacao:
            chamar_api(codigo, data)

    except Exception as e:
        print(f"Erro: {e}")

    finally:
        if 'conn' in locals() and conn:
            conn.close()
            print("Conexão com o banco de dados encerrada.")

if __name__ == "__main__":
    main()
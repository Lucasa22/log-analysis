import json
import pprint
import os
from log_analyzer.etl import run_pipeline

def run_and_show_results(save_to_db=True):
    """
    Ponto de entrada programático para executar o pipeline ETL simplificado
    e exibir os resultados. Ideal para uso em notebooks.

    Args:
        save_to_db: Se True, tenta salvar os resultados no banco de dados.
                    Se False, apenas salva em arquivos locais.
                    
    Returns:
        dict: Um dicionário contendo as métricas geradas.
    """
    print("Iniciando a análise de logs através do pipeline simplificado (execução local)...")

    # Executa o pipeline completo com os caminhos padrão
    result = run_pipeline(save_to_db=save_to_db)

    if result['status'] == 'success':
        print("✅ Pipeline executado com sucesso!")
        print(f"📄 Métricas geradas: {result.get('metrics_count', 0)}")
        
        # Informação sobre salvamento no banco de dados
        if save_to_db:
            if result.get('db_available', False):
                print("✅ Dados foram salvos em arquivos e no banco de dados")
            else:
                print("⚠️ Driver do banco de dados não disponível - dados salvos apenas em arquivos locais")
        else:
            print("ℹ️ Dados salvos apenas em arquivos locais (salvamento em BD desativado)")
            
        print(f"\nArquivos de saída:")
        print(f"- Bronze: {result.get('bronze_output')}")
        print(f"- Silver: {result.get('silver_output')}")
        print(f"- Gold: {result.get('gold_output')}")
        
        return result
    else:
        print(f"❌ Erro durante a execução do pipeline: {result.get('error', 'Erro desconhecido')}")
        return None

if __name__ == "__main__":
    import sys
    # Verifica se há um argumento --no-db para desativar o salvamento no banco de dados
    save_to_db = "--no-db" not in sys.argv
    run_and_show_results(save_to_db=save_to_db)
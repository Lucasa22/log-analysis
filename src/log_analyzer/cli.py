import sys
import click
from log_analyzer.etl import run_pipeline
import logging


@click.group()
def cli():
    """
    Log Analyzer - Ferramenta de linha de comando para análise de logs.
    Use os comandos disponíveis para processar e analisar arquivos de log.
    """
    pass


@cli.command()
@click.option(
    "--input-path",
    default="data/logs.txt",
    show_default=True,
    help="Caminho do arquivo de log bruto",
)
@click.option(
    "--output-path",
    default="data/processed",
    show_default=True,
    help="Diretório de saída para os resultados",
)
@click.option(
    "--log-format",
    default="common",
    type=click.Choice(["common", "combined", "nginx", "custom", "auto"]),
    help="Formato do log",
)
@click.option("--verbose", "-v", is_flag=True, help="Verbose output")
def run(input_path, output_path, log_format, verbose):
    """
    Executa o pipeline completo de análise de logs.
    Processa o arquivo de log informado, gera métricas e salva os resultados.
    """
    try:
        click.echo("🚀 Executando pipeline de análise de logs...")
        
        if verbose:
            logging.basicConfig(level=logging.INFO)
            
        result = run_pipeline(
            input_path=input_path,
            output_path=output_path,
            log_format=log_format
        )
        
        if result["status"] == "success":
            click.echo("✅ Pipeline finalizado com sucesso!")
            click.echo(f"Registros processados: {result['processed_records']}")
            click.echo(f"Dados salvos em: {output_path}")
            
            # Exibir resumo das principais métricas
            metrics = result.get("metrics", {})
            if metrics:
                click.echo("\n📊 Resumo das métricas:")
                for key, value in metrics.items():
                    if key == "top_endpoints" and isinstance(value, list):
                        click.echo("\n🔝 Top 5 endpoints:")
                        for i, endpoint in enumerate(value[:5], 1):
                            click.echo(f"  {i}. {endpoint['endpoint']} ({endpoint['count']} acessos)")
                    elif key == "status_counts" and isinstance(value, dict):
                        click.echo("\n📈 Códigos de status:")
                        for status, count in value.items():
                            click.echo(f"  {status}: {count}")
                    else:
                        click.echo(f"  {key}: {value}")
        else:
            click.echo(f"❌ Falha no pipeline: {result.get('error', 'Erro desconhecido')}")
            sys.exit(1)
    except Exception as e:
        click.echo(f"💥 Erro inesperado: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    cli()

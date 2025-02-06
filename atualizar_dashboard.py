"""
Script para automatizar a atualização do dashboard.
Monitora a pasta de saída por novos arquivos e atualiza o dashboard automaticamente.
"""
import time
import os
import sys
import logging
import schedule
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from analise_comparativa import AnalisadorComparativo

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('dashboard_update.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class ExcelHandler(FileSystemEventHandler):
    """Handler para monitorar arquivos Excel."""
    
    def __init__(self):
        self.analisador = AnalisadorComparativo()
        
        # Criar diretório de saída se não existir
        os.makedirs(self.analisador.diretorio_output, exist_ok=True)
        
    def on_created(self, event):
        """Chamado quando um novo arquivo é criado."""
        if not event.is_directory and event.src_path.endswith('.xlsx'):
            self.processar_arquivo(event.src_path)
    
    def on_modified(self, event):
        """Chamado quando um arquivo é modificado."""
        if not event.is_directory and event.src_path.endswith('.xlsx'):
            self.processar_arquivo(event.src_path)
    
    def processar_arquivo(self, arquivo):
        """Processa o arquivo e atualiza o dashboard."""
        try:
            logger.info(f"Novo arquivo detectado: {arquivo}")
            
            # Aguardar um momento para garantir que o arquivo está completamente escrito
            time.sleep(2)
            
            # Configurar arquivo de dados
            self.analisador.arquivo_dados = arquivo
            
            # Gerar relatório
            if self.analisador.carregar_dados() and self.analisador.gerar_relatorio():
                logger.info("Dashboard atualizado com sucesso!")
            else:
                logger.error("Falha ao atualizar dashboard")
                
        except Exception as e:
            logger.error(f"Erro ao processar arquivo: {str(e)}")

def atualizar_programado():
    """Função para atualização programada do dashboard."""
    logger.info("Iniciando atualização programada...")
    try:
        analisador = AnalisadorComparativo()
        arquivo = analisador.encontrar_arquivo_mais_recente()
        
        if arquivo:
            analisador.arquivo_dados = arquivo
            if analisador.carregar_dados() and analisador.gerar_relatorio():
                logger.info("Atualização programada concluída com sucesso!")
            else:
                logger.error("Falha na atualização programada")
        else:
            logger.warning("Nenhum arquivo encontrado para atualização programada")
            
    except Exception as e:
        logger.error(f"Erro na atualização programada: {str(e)}")

def main():
    """Função principal."""
    try:
        # Criar diretório de saída se não existir
        os.makedirs("output_graficos", exist_ok=True)
        
        # Configurar monitoramento de arquivos
        path = "output_20250131_0218"  # Diretório a ser monitorado
        if not os.path.exists(path):
            os.makedirs(path)
            
        event_handler = ExcelHandler()
        observer = Observer()
        observer.schedule(event_handler, path, recursive=True)
        observer.start()
        
        # Configurar atualizações programadas
        schedule.every().day.at("00:00").do(atualizar_programado)  # Atualização diária à meia-noite
        schedule.every().day.at("12:00").do(atualizar_programado)  # Atualização ao meio-dia
        
        logger.info(f"Monitorando diretório: {path}")
        logger.info("Pressione Ctrl+C para encerrar")
        
        # Executar primeira atualização
        atualizar_programado()
        
        # Loop principal
        while True:
            schedule.run_pending()
            time.sleep(1)
            
    except KeyboardInterrupt:
        observer.stop()
        logger.info("Monitoramento encerrado pelo usuário")
    except Exception as e:
        logger.error(f"Erro no monitoramento: {str(e)}")
    finally:
        observer.join()

if __name__ == "__main__":
    main()

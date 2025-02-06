"""
Analisador de Performance de Equipes - Versão 2.0
Melhorias:
- Configuração única do logger com verificação de handlers
- Parsing de datas otimizado
- Uso de argparse para parâmetros via CLI
- Tratamento de erros mais robusto
- Cache de dados para otimização
- Validação de dados aprimorada
"""

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os
import logging
import sys
import argparse
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Union
import numpy as np
from functools import lru_cache

class ConfiguracaoLogger:
    """Classe para configuração centralizada do logger."""
    
    @staticmethod
    def configurar(nome: str, nivel: int = logging.INFO) -> logging.Logger:
        """
        Configura e retorna um logger.
        
        Args:
            nome: Nome do logger
            nivel: Nível de logging (default: INFO)
            
        Returns:
            Logger configurado
        """
        logger = logging.getLogger(nome)
        
        # Evita handlers duplicados
        if not logger.handlers:
            # Handler para console
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            ))
            logger.addHandler(console_handler)
            
            # Handler para arquivo
            file_handler = logging.FileHandler('analise_equipes.log', encoding='utf-8')
            file_handler.setFormatter(logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            ))
            logger.addHandler(file_handler)
        
        logger.setLevel(nivel)
        return logger

class ValidadorDados:
    """Classe para validação de dados."""
    
    @staticmethod
    def validar_dataframe(df: pd.DataFrame, colunas_requeridas: List[str]) -> bool:
        """
        Valida se o DataFrame possui as colunas necessárias.
        
        Args:
            df: DataFrame a ser validado
            colunas_requeridas: Lista de colunas que devem existir
            
        Returns:
            bool: True se válido, False caso contrário
        """
        return all(coluna in df.columns for coluna in colunas_requeridas)

class AnalisadorEquipes:
    """Classe principal para análise de performance das equipes."""
    
    COLUNAS_REQUERIDAS = [
        'DATA_CRIACAO', 'DATA_RESOLUCAO', 'EQUIPE', 'OPERADOR',
        'STATUS', 'MOTIVO_PENDENCIA', 'BANCO'
    ]
    
    def __init__(self, caminho_arquivo: str, diretorio_saida: str = 'output_graficos_v2'):
        """
        Inicializa o analisador.
        
        Args:
            caminho_arquivo: Caminho para o arquivo de dados
            diretorio_saida: Diretório para salvar os resultados
        """
        self.logger = ConfiguracaoLogger.configurar(self.__class__.__name__)
        self.caminho_arquivo = Path(caminho_arquivo)
        self.diretorio_saida = Path(diretorio_saida)
        self.diretorio_saida.mkdir(parents=True, exist_ok=True)
        
        self.logger.info(f"Iniciando análise com arquivo: {self.caminho_arquivo}")
        self.carregar_dados()
        
    def carregar_dados(self) -> None:
        """Carrega e prepara os dados para análise."""
        try:
            self.logger.info("Carregando dados...")
            
            # Primeiro carrega sem converter datas
            self.df = pd.read_excel(self.caminho_arquivo)
            
            # Validação de dados
            if not ValidadorDados.validar_dataframe(self.df, self.COLUNAS_REQUERIDAS):
                raise ValueError(f"Arquivo não contém todas as colunas necessárias: {self.COLUNAS_REQUERIDAS}")
            
            # Converte datas manualmente
            for col in ['DATA_CRIACAO', 'DATA_RESOLUCAO']:
                self.logger.info(f"Processando coluna {col}")
                if col in self.df.columns:
                    try:
                        # Primeiro tenta converter assumindo formato brasileiro
                        self.df[col] = pd.to_datetime(self.df[col].astype(str).str.replace('20025', '2025'), format='%d/%m/%Y')
                        self.logger.info(f"Coluna {col} convertida com formato %d/%m/%Y")
                    except Exception as e1:
                        try:
                            # Tenta formato ISO
                            self.df[col] = pd.to_datetime(self.df[col].astype(str).str.replace('20025', '2025'), format='%Y-%m-%d')
                            self.logger.info(f"Coluna {col} convertida com formato %Y-%m-%d")
                        except Exception as e2:
                            try:
                                # Tenta formato flexível
                                self.df[col] = pd.to_datetime(self.df[col].astype(str).str.replace('20025', '2025'))
                                self.logger.info(f"Coluna {col} convertida com formato flexível")
                            except Exception as e3:
                                self.logger.warning(f"Erro ao converter coluna {col}, usando parse genérico")
                                self.df[col] = pd.to_datetime(self.df[col].astype(str).str.replace('20025', '2025'), errors='coerce')
                    
                    # Verificar valores nulos após conversão
                    nulos = self.df[col].isnull().sum()
                    if nulos > 0:
                        self.logger.warning(f"Coluna {col} contém {nulos} valores nulos após conversão")
                else:
                    self.logger.warning(f"Coluna {col} não encontrada no DataFrame")
            
            # Limpeza de dados
            self.df = self.limpar_dados(self.df)
            
            # Cálculo de métricas derivadas
            self.calcular_metricas_derivadas()
            
            self.logger.info(f"Dados carregados com sucesso. Total de registros: {len(self.df)}")
            
        except Exception as e:
            self.logger.error(f"Erro ao carregar dados: {str(e)}", exc_info=True)
            raise
    
    @staticmethod
    def limpar_dados(df: pd.DataFrame) -> pd.DataFrame:
        """
        Limpa e padroniza os dados.
        
        Args:
            df: DataFrame original
            
        Returns:
            DataFrame limpo
        """
        df = df.copy()
        
        # Remove linhas completamente vazias
        df = df.dropna(how='all')
        
        # Padroniza strings
        for col in df.select_dtypes(include=['object']):
            df[col] = df[col].str.strip().str.upper()
        
        return df
    
    def calcular_metricas_derivadas(self) -> None:
        """Calcula métricas derivadas dos dados."""
        # Tempo de resolução
        mask_resolvido = self.df['STATUS'].isin(['QUITADO', 'APROVADO'])
        self.df.loc[mask_resolvido, 'TEMPO_RESOLUCAO'] = (
            self.df.loc[mask_resolvido, 'DATA_RESOLUCAO'] - 
            self.df.loc[mask_resolvido, 'DATA_CRIACAO']
        ).dt.total_seconds() / (24 * 60 * 60)  # Converter para dias
        
        # Taxa de sucesso
        self.df['SUCESSO'] = self.df['STATUS'].isin(['QUITADO', 'APROVADO', 'RESOLVIDO'])
    
    @lru_cache(maxsize=None)
    def obter_equipes(self) -> List[str]:
        """Retorna lista de equipes única."""
        return sorted(self.df['EQUIPE'].unique())
    
    def criar_grafico(
        self,
        dados: Union[pd.Series, pd.DataFrame],
        tipo: str,
        titulo: str,
        nome_arquivo: str,
        layout_vertical: bool = False
    ) -> None:
        """
        Cria e salva um gráfico.
        
        Args:
            dados: Dados para o gráfico
            tipo: Tipo do gráfico (pizza, barras, linhas)
            titulo: Título do gráfico
            nome_arquivo: Nome do arquivo para salvar
            layout_vertical: Se True, usa layout vertical para gráfico de barras
        """
        try:
            if tipo == 'pizza':
                fig = px.pie(values=dados.values, names=dados.index, title=titulo)
            elif tipo == 'barras':
                if layout_vertical:
                    fig = px.bar(dados, title=titulo)
                else:
                    fig = px.bar(dados, orientation='h', title=titulo)
            elif tipo == 'linhas':
                fig = px.line(dados, title=titulo)
            else:
                raise ValueError(f"Tipo de gráfico não suportado: {tipo}")
            
            # Salvar gráficos
            caminho_html = self.diretorio_saida / f'{nome_arquivo}.html'
            caminho_png = self.diretorio_saida / f'{nome_arquivo}.png'
            
            fig.write_html(str(caminho_html))
            fig.write_image(str(caminho_png))
            
            self.logger.info(f"Gráfico criado: {nome_arquivo}")
            
        except Exception as e:
            self.logger.error(f"Erro ao criar gráfico {nome_arquivo}: {str(e)}")
            raise
    
    def analisar_distribuicao_status(self, equipe: Optional[str] = None) -> None:
        """Analisa distribuição de status por equipe."""
        try:
            if equipe:
                dados = self.df[self.df['EQUIPE'] == equipe]['STATUS'].value_counts()
                nome_arquivo = f'status_distribuicao_{equipe}'.replace('/', '_')
                titulo = f'Distribuição de Status - {equipe}'
            else:
                dados = self.df['STATUS'].value_counts()
                nome_arquivo = 'status_distribuicao_geral'
                titulo = 'Distribuição de Status - Todas as Equipes'
            
            self.criar_grafico(dados, 'pizza', titulo, nome_arquivo)
            
        except Exception as e:
            self.logger.error(f"Erro ao analisar distribuição de status: {str(e)}")
            raise
    
    def analisar_tempo_resolucao(self, equipe: Optional[str] = None) -> None:
        """Analisa tempo médio de resolução."""
        try:
            if equipe:
                dados = self.df[self.df['EQUIPE'] == equipe].groupby('OPERADOR')['TEMPO_RESOLUCAO'].mean()
                nome_arquivo = f'tempo_resolucao_{equipe}'.replace('/', '_')
                titulo = f'Tempo Médio de Resolução (dias) - {equipe}'
            else:
                dados = self.df.groupby('OPERADOR')['TEMPO_RESOLUCAO'].mean()
                nome_arquivo = 'tempo_resolucao_geral'
                titulo = 'Tempo Médio de Resolução (dias) - Todas as Equipes'
            
            self.criar_grafico(dados, 'barras', titulo, nome_arquivo)
            
        except Exception as e:
            self.logger.error(f"Erro ao analisar tempo de resolução: {str(e)}")
            raise
    
    def gerar_relatorio_html(self) -> None:
        """Gera relatório HTML com todos os gráficos."""
        try:
            self.logger.info("Gerando relatório HTML...")
            
            equipes = self.obter_equipes()
            template = """
            <html>
            <head>
                <title>Relatório de Análise de Equipes</title>
                <style>
                    body { font-family: Arial, sans-serif; margin: 40px; }
                    .container { max-width: 1200px; margin: 0 auto; }
                    h1 { color: #333; text-align: center; }
                    h2 { color: #666; margin-top: 30px; }
                    .graph-container { margin: 20px 0; }
                    iframe { width: 100%; height: 600px; border: none; }
                    .metrics { 
                        display: grid;
                        grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
                        gap: 20px;
                        margin: 20px 0;
                    }
                    .metric-card {
                        background: #f5f5f5;
                        padding: 20px;
                        border-radius: 8px;
                        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                    }
                    .metric-value {
                        font-size: 24px;
                        font-weight: bold;
                        color: #2196F3;
                    }
                </style>
            </head>
            <body>
                <div class="container">
                    <h1>Relatório de Análise de Equipes</h1>
            """
            
            # Adicionar métricas gerais
            metricas_gerais = {
                'Total de Demandas': len(self.df),
                'Demandas Resolvidas': len(self.df[self.df['SUCESSO']]),
                'Taxa de Sucesso': f"{(self.df['SUCESSO'].mean() * 100):.1f}%",
                'Tempo Médio de Resolução': f"{self.df['TEMPO_RESOLUCAO'].mean():.1f} dias"
            }
            
            template += """
                    <h2>Métricas Gerais</h2>
                    <div class="metrics">
            """
            
            for nome, valor in metricas_gerais.items():
                template += f"""
                        <div class="metric-card">
                            <div>{nome}</div>
                            <div class="metric-value">{valor}</div>
                        </div>
                """
            
            template += "</div>"
            
            # Adicionar seções para cada equipe
            for equipe in equipes:
                equipe_safe = equipe.replace('/', '_')
                template += f"""
                    <h2>Análise da Equipe: {equipe}</h2>
                    <div class="graph-container">
                        <h3>Distribuição de Status</h3>
                        <iframe src="status_distribuicao_{equipe_safe}.html"></iframe>
                    </div>
                    <div class="graph-container">
                        <h3>Tempo de Resolução</h3>
                        <iframe src="tempo_resolucao_{equipe_safe}.html"></iframe>
                    </div>
                """
            
            template += """
                </div>
            </body>
            </html>
            """
            
            # Salvar relatório
            caminho_relatorio = self.diretorio_saida / 'relatorio_equipes_v2.html'
            with open(caminho_relatorio, 'w', encoding='utf-8') as f:
                f.write(template)
            
            self.logger.info(f"Relatório HTML gerado com sucesso: {caminho_relatorio}")
            
        except Exception as e:
            self.logger.error(f"Erro ao gerar relatório HTML: {str(e)}")
            raise

def main():
    """Função principal."""
    # Configurar argumentos da linha de comando
    parser = argparse.ArgumentParser(description='Analisador de Performance de Equipes')
    parser.add_argument('--arquivo', required=True, help='Caminho para o arquivo de dados')
    parser.add_argument('--saida', default='output_graficos_v2', help='Diretório de saída')
    parser.add_argument('--debug', action='store_true', help='Ativar modo debug')
    args = parser.parse_args()
    
    # Configurar nível de log
    nivel_log = logging.DEBUG if args.debug else logging.INFO
    logging.basicConfig(level=nivel_log)
    
    try:
        # Criar analisador
        analisador = AnalisadorEquipes(args.arquivo, args.saida)
        
        # Executar análises por equipe
        for equipe in analisador.obter_equipes():
            analisador.analisar_distribuicao_status(equipe)
            analisador.analisar_tempo_resolucao(equipe)
        
        # Gerar relatório HTML
        analisador.gerar_relatorio_html()
        
        logging.info("Análise concluída com sucesso!")
        
    except Exception as e:
        logging.critical(f"Erro crítico: {str(e)}", exc_info=True)
        sys.exit(1)

if __name__ == '__main__':
    main()

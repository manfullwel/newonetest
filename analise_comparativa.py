#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Análise Comparativa de Demandas
==============================

Análise detalhada comparando as demandas entre JULIO e LEANDRO/ADRIANO,
com foco em quitações e performance por banco em Janeiro/2025.
"""

import os
import glob
import logging
from datetime import datetime
import pandas as pd
import plotly.graph_objects as go
from typing import Tuple, Dict, List, Optional

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('analise_comparativa.log'),
        logging.StreamHandler()
    ]
)

class AnalisadorComparativo:
    """Classe para análise comparativa de demandas."""
    
    def __init__(self):
        """Inicializa o analisador."""
        self.arquivo_dados = None
        self.df_quitados_jan = None
        self.df_julio_jan = None
        self.df_leandro_jan = None
        self.diretorio_output = "output_graficos"
        
        # Configurar logging
        self.logger = logging.getLogger(__name__)
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.INFO)
    
    def encontrar_arquivo_mais_recente(self) -> Optional[str]:
        """
        Encontra o arquivo de dados processado mais recente.
        
        Returns:
            str: Caminho do arquivo mais recente ou None se não encontrado
        """
        try:
            padrao = os.path.join('.', 'output_*', '*PROCESSADO*.xlsx')
            arquivos = glob.glob(padrao)
            
            if not arquivos:
                raise FileNotFoundError(f"Nenhum arquivo encontrado com o padrão: {padrao}")
            
            arquivo_recente = max(arquivos, key=os.path.getctime)
            self.logger.info(f"Arquivo mais recente encontrado: {arquivo_recente}")
            return arquivo_recente
            
        except Exception as e:
            self.logger.error(f"Erro ao buscar arquivo mais recente: {str(e)}")
            return None
    
    def carregar_dados(self):
        """
        Carrega os dados do arquivo Excel.
        
        Raises:
            ValueError: Se o arquivo não existir ou se faltar alguma coluna obrigatória
        """
        try:
            # Verificar se o arquivo existe
            if not os.path.exists(self.arquivo_dados):
                raise ValueError(f"Arquivo não encontrado: {self.arquivo_dados}")
            
            # Carregar dados
            df = pd.read_excel(self.arquivo_dados)
            
            # Verificar colunas obrigatórias
            colunas_obrigatorias = ['DATA', 'RESPONSAVEL', 'BANCO', 'STATUS']
            colunas_faltantes = [col for col in colunas_obrigatorias if col not in df.columns]
            
            if colunas_faltantes:
                raise ValueError(f"Colunas faltantes no arquivo: {colunas_faltantes}")
            
            # Filtrar dados
            self.df_quitados_jan = df[df['STATUS'] == 'QUITADO']
            self.df_julio_jan = df[df['RESPONSAVEL'] == 'JULIO']
            self.df_leandro_jan = df[df['RESPONSAVEL'].isin(['LEANDRO', 'ADRIANO'])]
            
            return True
            
        except Exception as e:
            self.logger.error(f"Erro ao carregar dados: {str(e)}")
            raise ValueError(f"Erro ao carregar dados: {str(e)}")
    
    def validar_responsaveis(self) -> bool:
        """
        Valida se os responsáveis necessários estão presentes nos dados.
        
        Returns:
            bool: True se todos os responsáveis necessários estão presentes
        """
        responsaveis_necessarios = {'JULIO', 'LEANDRO', 'ADRIANO'}
        responsaveis_presentes = set(self.df_quitados_jan['RESPONSAVEL'].unique())
        faltantes = responsaveis_necessarios - responsaveis_presentes
        
        if faltantes:
            self.logger.warning(f"Responsáveis faltantes nos dados: {faltantes}")
            return False
        return True
    
    def analisar_performance_geral(self):
        """
        Análise geral de performance.
        
        Returns:
            pd.DataFrame: DataFrame com métricas de performance por grupo
        """
        # Agrupar quitações por grupo
        quitacoes = self.df_quitados_jan.copy()
        quitacoes['GRUPO'] = quitacoes['RESPONSAVEL'].map(
            lambda x: 'LEANDRO/ADRIANO' if x in ['LEANDRO', 'ADRIANO'] else 'JULIO'
        )
        
        # Calcular métricas por grupo
        metricas = {
            'GRUPO': ['JULIO', 'LEANDRO/ADRIANO'],
            'Total de Quitações': [
                len(quitacoes[quitacoes['GRUPO'] == 'JULIO']),
                len(quitacoes[quitacoes['GRUPO'] == 'LEANDRO/ADRIANO'])
            ],
            'Média Diária': [
                len(quitacoes[quitacoes['GRUPO'] == 'JULIO']) / quitacoes['DATA'].dt.day.nunique(),
                len(quitacoes[quitacoes['GRUPO'] == 'LEANDRO/ADRIANO']) / quitacoes['DATA'].dt.day.nunique()
            ],
            'Bancos Atendidos': [
                quitacoes[quitacoes['GRUPO'] == 'JULIO']['BANCO'].nunique(),
                quitacoes[quitacoes['GRUPO'] == 'LEANDRO/ADRIANO']['BANCO'].nunique()
            ]
        }
        
        return pd.DataFrame(metricas).set_index('GRUPO')
    
    def analisar_bancos_principais(self):
        """Análise dos principais bancos por responsável."""
        julio_bancos = self.df_julio_jan['BANCO'].value_counts().head(5)
        leandro_bancos = self.df_leandro_jan['BANCO'].value_counts().head(5)
        
        fig = go.Figure()
        
        # Barras para JULIO
        fig.add_trace(go.Bar(
            name='JULIO',
            x=julio_bancos.index,
            y=julio_bancos.values,
            marker_color='rgba(0,0,0,0)'
        ))
        
        # Barras para LEANDRO/ADRIANO
        fig.add_trace(go.Bar(
            name='LEANDRO/ADRIANO',
            x=leandro_bancos.index,
            y=leandro_bancos.values,
            marker_color='rgba(0,0,0,0)'
        ))
        
        fig.update_layout(
            title='Top 5 Bancos por Responsável - Janeiro 2025',
            barmode='group',
            xaxis_title='Banco',
            yaxis_title='Quantidade de Demandas',
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='white',
            height=500
        )
        
        return fig
    
    def analisar_evolucao_diaria(self):
        """Análise da evolução diária de demandas."""
        # Contagem diária
        julio_daily = self.df_julio_jan.groupby(self.df_julio_jan['DATA'].dt.date).size()
        leandro_daily = self.df_leandro_jan.groupby(self.df_leandro_jan['DATA'].dt.date).size()
        
        # Média móvel de 3 dias
        julio_ma = julio_daily.rolling(window=3, min_periods=1).mean()
        leandro_ma = leandro_daily.rolling(window=3, min_periods=1).mean()
        
        fig = go.Figure()
        
        # Dados diários e média móvel para JULIO
        fig.add_trace(go.Scatter(
            x=julio_daily.index,
            y=julio_daily.values,
            name='JULIO - Diário',
            mode='markers',
            marker=dict(color='rgba(0,0,0,0)', size=8)
        ))
        
        fig.add_trace(go.Scatter(
            x=julio_ma.index,
            y=julio_ma.values,
            name='JULIO - Média Móvel',
            line=dict(color='rgba(0,0,0,0)', width=2)
        ))
        
        # Dados diários e média móvel para LEANDRO/ADRIANO
        fig.add_trace(go.Scatter(
            x=leandro_daily.index,
            y=leandro_daily.values,
            name='LEANDRO/ADRIANO - Diário',
            mode='markers',
            marker=dict(color='rgba(0,0,0,0)', size=8)
        ))
        
        fig.add_trace(go.Scatter(
            x=leandro_ma.index,
            y=leandro_ma.values,
            name='LEANDRO/ADRIANO - Média Móvel',
            line=dict(color='rgba(0,0,0,0)', width=2)
        ))
        
        fig.update_layout(
            title='Evolução Diária de Demandas com Média Móvel (3 dias)',
            xaxis_title='Data',
            yaxis_title='Quantidade de Demandas',
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='white',
            height=500
        )
        
        return fig
    
    def analisar_quitacoes_detalhado(self):
        """Análise detalhada das quitações por responsável e banco."""
        # Preparar dados de quitações
        quitacoes = self.df_quitados_jan.copy()
        
        # Filtrar apenas os responsáveis que queremos
        responsaveis_alvo = ['JULIO', 'LEANDRO', 'ADRIANO']
        quitacoes = quitacoes[quitacoes['RESPONSAVEL'].isin(responsaveis_alvo)]
        
        # Mapear responsáveis para seus grupos
        quitacoes['GRUPO'] = quitacoes['RESPONSAVEL'].map(
            lambda x: 'LEANDRO/ADRIANO' if x in ['LEANDRO', 'ADRIANO'] else 'JULIO'
        )
        
        # Análise por banco e grupo
        quit_banco = pd.crosstab(
            quitacoes['BANCO'],
            quitacoes['GRUPO'],
            margins=True
        ).fillna(0)
        
        # Calcular taxas de sucesso
        def calcular_taxa_sucesso(grupo, banco):
            if grupo == 'JULIO':
                demandas = len(self.df_julio_jan[self.df_julio_jan['BANCO'] == banco])
            elif grupo == 'LEANDRO/ADRIANO':
                demandas = len(self.df_leandro_jan[self.df_leandro_jan['BANCO'] == banco])
            else:
                return 0
                
            if banco in quit_banco.index and grupo in quit_banco.columns:
                quitacoes = quit_banco.loc[banco, grupo]
            else:
                quitacoes = 0
                
            return (quitacoes / demandas * 100) if demandas > 0 else 0
        
        # Criar DataFrame com taxas de sucesso
        bancos_unicos = pd.concat([
            self.df_julio_jan['BANCO'],
            self.df_leandro_jan['BANCO']
        ]).unique()
        
        dados_taxa = {
            'JULIO': [],
            'LEANDRO/ADRIANO': []
        }
        
        for banco in bancos_unicos:
            dados_taxa['JULIO'].append(calcular_taxa_sucesso('JULIO', banco))
            dados_taxa['LEANDRO/ADRIANO'].append(calcular_taxa_sucesso('LEANDRO/ADRIANO', banco))
        
        taxas_sucesso = pd.DataFrame(
            dados_taxa,
            index=bancos_unicos
        )
        
        # Cores para os grupos
        cores = {
            'JULIO': 'rgba(0,0,0,0)',          # Azul escuro
            'LEANDRO/ADRIANO': 'rgba(0,0,0,0)'  # Vermelho
        }
        
        # Criar gráfico de barras agrupadas para taxas
        fig = go.Figure()
        
        for grupo in ['JULIO', 'LEANDRO/ADRIANO']:
            fig.add_trace(go.Bar(
                name=grupo,
                x=taxas_sucesso.index,
                y=taxas_sucesso[grupo],
                marker_color=cores[grupo]
            ))
        
        fig.update_layout(
            title='Taxa de Sucesso em Quitações por Banco e Grupo (%)',
            barmode='group',
            xaxis_title='Banco',
            yaxis_title='Taxa de Sucesso (%)',
            xaxis_tickangle=-45,
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='white',
            height=600,
            showlegend=True,
            legend=dict(
                yanchor="top",
                y=0.99,
                xanchor="right",
                x=0.99
            )
        )
        
        # Criar gráfico de pizza para distribuição total de quitações
        total_quitacoes = quitacoes['GRUPO'].value_counts()
        
        fig_pizza = go.Figure(data=[go.Pie(
            labels=total_quitacoes.index,
            values=total_quitacoes.values,
            hole=.3,
            marker_colors=[cores[grupo] for grupo in total_quitacoes.index]
        )])
        
        fig_pizza.update_layout(
            title='Distribuição Total de Quitações por Grupo',
            height=400,
            showlegend=True
        )
        
        # Análise por banco (top 10)
        top_bancos = quit_banco.iloc[:-1].sum(axis=1).sort_values(ascending=False).head(10).index
        dados_top_bancos = quit_banco.loc[top_bancos]
        
        fig_bancos = go.Figure()
        
        for grupo in dados_top_bancos.columns[:-1]:  # Excluir coluna 'All'
            fig_bancos.add_trace(go.Bar(
                name=grupo,
                x=dados_top_bancos.index,
                y=dados_top_bancos[grupo],
                marker_color=cores[grupo]
            ))
        
        fig_bancos.update_layout(
            title='Quitações por Banco e Grupo (Top 10 Bancos)',
            barmode='group',
            xaxis_title='Banco',
            yaxis_title='Número de Quitações',
            xaxis_tickangle=-45,
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='white',
            height=500,
            showlegend=True
        )
        
        # Salvar todos os gráficos
        self.salvar_graficos({
            'taxas_quitacao': fig,
            'distribuicao_quitacoes': fig_pizza,
            'quitacoes_por_banco': fig_bancos
        })
        
        # Salvar dados em Excel com formatação
        with pd.ExcelWriter(os.path.join(self.diretorio_output, 'analise_detalhada.xlsx'), engine='openpyxl') as writer:
            taxas_sucesso.round(2).to_excel(writer, sheet_name='Taxas de Sucesso')
            quit_banco.to_excel(writer, sheet_name='Quitações por Banco')
            
            # Adicionar análise de performance individual
            performance = pd.DataFrame({
                'Total de Quitações': total_quitacoes,
                'Média Diária': total_quitacoes / self.df_quitados_jan['DATA'].dt.day.nunique(),
                'Bancos Atendidos': [
                    len(quit_banco[quit_banco[grupo] > 0]) for grupo in total_quitacoes.index
                ]
            })
            performance.round(2).to_excel(writer, sheet_name='Performance Individual')
        
        return fig, taxas_sucesso, fig_pizza, fig_bancos
    
    def salvar_graficos(self, graficos: Dict[str, go.Figure]):
        """Salva os gráficos em arquivos HTML e PNG."""
        for nome, grafico in graficos.items():
            grafico.write_html(os.path.join(self.diretorio_output, f'{nome}.html'))
            grafico.write_image(os.path.join(self.diretorio_output, f'{nome}.png'))
    
    def gerar_html(self):
        """Gera o relatório em HTML."""
        html_content = f"""
        <html>
        <head>
            <title>Relatório Comparativo de Demandas - Janeiro 2025</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 40px; }}
                h1 {{ color: rgba(0,0,0,0); }}
                h2 {{ color: rgba(0,0,0,0); margin-top: 30px; }}
                .performance {{ margin: 20px 0; }}
                .graficos {{ margin: 30px 0; }}
            </style>
        </head>
        <body>
            <h1>Relatório Comparativo de Demandas - Janeiro 2025</h1>
            
            <h2>1. Performance Geral</h2>
            <div class="performance">
                {self.analisar_performance_geral().to_html()}
            </div>
            
            <h2>2. Principais Bancos</h2>
            <div class="graficos">
                <iframe src="bancos_principais.html" width="100%" height="600px" frameborder="0"></iframe>
            </div>
            
            <h2>3. Evolução Diária</h2>
            <div class="graficos">
                <iframe src="evolucao_diaria.html" width="100%" height="600px" frameborder="0"></iframe>
            </div>
            
            <h2>4. Taxas de Quitação</h2>
            <div class="graficos">
                <iframe src="taxas_quitacao.html" width="100%" height="600px" frameborder="0"></iframe>
            </div>
            
            <h2>5. Distribuição de Quitações</h2>
            <div class="graficos">
                <iframe src="distribuicao_quitacoes.html" width="100%" height="400px" frameborder="0"></iframe>
            </div>
            
            <h2>6. Quitações por Banco</h2>
            <div class="graficos">
                <iframe src="quitacoes_por_banco.html" width="100%" height="500px" frameborder="0"></iframe>
            </div>
        </body>
        </html>
        """
        
        with open(os.path.join(self.diretorio_output, 'relatorio.html'), 'w', encoding='utf-8') as f:
            f.write(html_content)
    
    def gerar_relatorio(self) -> bool:
        """
        Gera o relatório completo com todas as análises.
        
        Returns:
            bool: True se o relatório foi gerado com sucesso
        """
        try:
            if not self.carregar_dados():
                raise RuntimeError("Falha ao carregar dados")
                
            if not self.validar_responsaveis():
                raise ValueError("Dados dos responsáveis incompletos")
            
            self.logger.info("Analisando performance geral...")
            self.analisar_performance_geral()
            
            self.logger.info("Analisando principais bancos...")
            self.analisar_bancos_principais()
            
            self.logger.info("Analisando evolução diária...")
            self.analisar_evolucao_diaria()
            
            self.logger.info("Analisando quitações...")
            fig_quitacoes, taxas, fig_pizza, fig_bancos = self.analisar_quitacoes_detalhado()
            
            # Salvar gráficos
            self.salvar_graficos({
                'taxas_quitacao': fig_quitacoes,
                'distribuicao_quitacoes': fig_pizza,
                'quitacoes_por_banco': fig_bancos
            })
            
            # Gerar relatório HTML
            self.gerar_html()
            
            self.logger.info("\nRelatório gerado com sucesso em: relatorios")
            self.logger.info("Arquivos gerados:")
            self.logger.info("- relatorio.html (Relatório completo)")
            self.logger.info("- performance_geral.xlsx (Métricas gerais)")
            self.logger.info("- bancos_principais.html/png (Gráfico de principais bancos)")
            self.logger.info("- evolucao_diaria.html/png (Gráfico de evolução)")
            self.logger.info("- taxas_quitacao.html/png (Gráfico de taxas)")
            self.logger.info("- distribuicao_quitacoes.html/png (Gráfico de distribuição)")
            self.logger.info("- quitacoes_por_banco.html/png (Gráfico de quitações por banco)")
            self.logger.info("- analise_detalhada.xlsx (Dados detalhados)")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Erro ao gerar relatório: {str(e)}")
            return False

def main():
    """Função principal."""
    # Encontrar arquivo mais recente
    analisador = AnalisadorComparativo()
    analisador.arquivo_dados = analisador.encontrar_arquivo_mais_recente()
    analisador.gerar_relatorio()

if __name__ == "__main__":
    main()

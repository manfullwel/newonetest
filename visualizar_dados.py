#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Visualizador de Dados do Pipeline
================================

Este módulo gera visualizações interativas e profissionais dos dados
processados pelo pipeline de demandas.

Autor: Sua Empresa
Data: Janeiro 2025
Versão: 1.0.0
"""

import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import json
from pathlib import Path
from datetime import datetime
import dash
from dash import html, dcc
import dash_bootstrap_components as dbc

class DemandaVisualizer:
    """
    Classe para geração de visualizações dos dados processados.
    """
    
    def __init__(self, arquivo_excel, arquivo_relatorio):
        """
        Inicializa o visualizador com os arquivos de dados.
        
        Args:
            arquivo_excel: Caminho para o arquivo Excel processado
            arquivo_relatorio: Caminho para o relatório JSON
        """
        self.excel_file = pd.ExcelFile(arquivo_excel)
        with open(arquivo_relatorio, 'r', encoding='utf-8') as f:
            self.relatorio = json.load(f)
            
        # Configuração de cores
        self.colors = {
            'primary': '#2C3E50',
            'secondary': '#E74C3C',
            'success': '#27AE60',
            'warning': '#F39C12',
            'info': '#3498DB',
            'background': '#ECF0F1'
        }
        
    def criar_grafico_status(self, aba):
        """Cria gráfico de pizza com status dos registros."""
        status_data = self.relatorio['resultados'][aba]['status']
        
        fig = go.Figure(data=[go.Pie(
            labels=list(status_data.keys()),
            values=list(status_data.values()),
            hole=.3,
            marker_colors=[self.colors['success'], self.colors['warning'], self.colors['secondary']]
        )])
        
        fig.update_layout(
            title=f"Status dos Registros - {aba}",
            showlegend=True,
            plot_bgcolor=self.colors['background'],
            paper_bgcolor='white'
        )
        
        return fig
    
    def criar_grafico_problemas(self, aba):
        """Cria gráfico de barras com problemas mais frequentes."""
        problemas = self.relatorio['resultados'][aba]['problemas_frequentes']
        
        fig = go.Figure(data=[go.Bar(
            x=list(problemas.values()),
            y=list(problemas.keys()),
            orientation='h',
            marker_color=self.colors['warning']
        )])
        
        fig.update_layout(
            title=f"Problemas Mais Frequentes - {aba}",
            xaxis_title="Quantidade de Ocorrências",
            yaxis_title="Problema",
            plot_bgcolor=self.colors['background'],
            paper_bgcolor='white'
        )
        
        return fig
    
    def criar_grafico_distribuicao_temporal(self, aba):
        """Cria gráfico de linha com distribuição temporal."""
        df = pd.read_excel(self.excel_file, sheet_name=aba)
        df['DATA'] = pd.to_datetime(df['DATA'])
        
        contagem_mensal = df.groupby(df['DATA'].dt.to_period('M')).size()
        
        fig = go.Figure(data=[go.Scatter(
            x=[str(period) for period in contagem_mensal.index],
            y=contagem_mensal.values,
            mode='lines+markers',
            line=dict(color=self.colors['info'], width=2),
            marker=dict(size=8)
        )])
        
        fig.update_layout(
            title=f"Distribuição Temporal - {aba}",
            xaxis_title="Mês",
            yaxis_title="Quantidade de Registros",
            plot_bgcolor=self.colors['background'],
            paper_bgcolor='white'
        )
        
        return fig
    
    def criar_grafico_bancos(self, aba):
        """Cria gráfico de barras com distribuição por banco."""
        bancos = self.relatorio['resultados'][aba]['distribuicoes']['bancos']
        
        fig = go.Figure(data=[go.Bar(
            x=list(bancos.keys()),
            y=list(bancos.values()),
            marker_color=self.colors['primary']
        )])
        
        fig.update_layout(
            title=f"Distribuição por Banco - {aba}",
            xaxis_title="Banco",
            yaxis_title="Quantidade de Registros",
            xaxis_tickangle=-45,
            plot_bgcolor=self.colors['background'],
            paper_bgcolor='white'
        )
        
        return fig
    
    def criar_grafico_comparativo_quitacoes(self):
        """Cria gráfico comparativo de quitações entre JULIO e LEANDRO/ADRIANO."""
        # Carregar dados
        df_julio = pd.read_excel(self.excel_file, sheet_name='DEMANDAS JULIO')
        df_leandro = pd.read_excel(self.excel_file, sheet_name='DEMANDA LEANDROADRIANO')
        df_quitados = pd.read_excel(self.excel_file, sheet_name='QUITADOS')
        
        # Converter datas
        for df in [df_julio, df_leandro, df_quitados]:
            df['DATA'] = pd.to_datetime(df['DATA'])
            
        # Filtrar apenas janeiro/2025
        janeiro_2025 = lambda df: df[df['DATA'].dt.to_period('M') == pd.Period('2025-01')]
        
        df_julio_jan = janeiro_2025(df_julio)
        df_leandro_jan = janeiro_2025(df_leandro)
        df_quitados_jan = janeiro_2025(df_quitados)
        
        # Análise por banco
        def contar_por_banco(df):
            return df['BANCO'].value_counts()
            
        quitacoes_julio = contar_por_banco(df_julio_jan)
        quitacoes_leandro = contar_por_banco(df_leandro_jan)
        
        # Criar DataFrame comparativo
        bancos = sorted(set(quitacoes_julio.index) | set(quitacoes_leandro.index))
        dados_comp = pd.DataFrame({
            'JULIO': [quitacoes_julio.get(banco, 0) for banco in bancos],
            'LEANDRO/ADRIANO': [quitacoes_leandro.get(banco, 0) for banco in bancos]
        }, index=bancos)
        
        # Criar gráfico de barras agrupadas
        fig = go.Figure()
        
        fig.add_trace(go.Bar(
            name='JULIO',
            x=bancos,
            y=dados_comp['JULIO'],
            marker_color=self.colors['primary']
        ))
        
        fig.add_trace(go.Bar(
            name='LEANDRO/ADRIANO',
            x=bancos,
            y=dados_comp['LEANDRO/ADRIANO'],
            marker_color=self.colors['secondary']
        ))
        
        fig.update_layout(
            title='Comparativo de Demandas por Banco - Janeiro 2025',
            xaxis_title='Banco',
            yaxis_title='Quantidade de Demandas',
            barmode='group',
            xaxis_tickangle=-45,
            showlegend=True,
            plot_bgcolor=self.colors['background'],
            paper_bgcolor='white',
            height=600
        )
        
        return fig
    
    def criar_grafico_pizza_quitacoes(self):
        """Cria gráfico de pizza comparando total de quitações."""
        df_quitados = pd.read_excel(self.excel_file, sheet_name='QUITADOS')
        df_quitados['DATA'] = pd.to_datetime(df_quitados['DATA'])
        
        # Filtrar janeiro/2025
        df_jan = df_quitados[df_quitados['DATA'].dt.to_period('M') == pd.Period('2025-01')]
        
        # Contar quitações por responsável
        quitacoes = df_jan['RESPONSAVEL'].value_counts()
        
        fig = go.Figure(data=[go.Pie(
            labels=quitacoes.index,
            values=quitacoes.values,
            hole=.3,
            marker_colors=[self.colors['primary'], self.colors['secondary']]
        )])
        
        fig.update_layout(
            title='Distribuição de Quitações por Responsável - Janeiro 2025',
            showlegend=True,
            plot_bgcolor=self.colors['background'],
            paper_bgcolor='white'
        )
        
        return fig
    
    def criar_grafico_timeline_quitacoes(self):
        """Cria gráfico de linha temporal comparando quitações diárias."""
        # Carregar e preparar dados
        df_julio = pd.read_excel(self.excel_file, sheet_name='DEMANDAS JULIO')
        df_leandro = pd.read_excel(self.excel_file, sheet_name='DEMANDA LEANDROADRIANO')
        
        for df in [df_julio, df_leandro]:
            df['DATA'] = pd.to_datetime(df['DATA'])
        
        # Filtrar janeiro/2025
        mask_jan = lambda df: df['DATA'].dt.to_period('M') == pd.Period('2025-01')
        df_julio_jan = df_julio[mask_jan(df_julio)]
        df_leandro_jan = df_leandro[mask_jan(df_leandro)]
        
        # Contagem diária
        contagem_julio = df_julio_jan.groupby(df_julio_jan['DATA'].dt.date).size()
        contagem_leandro = df_leandro_jan.groupby(df_leandro_jan['DATA'].dt.date).size()
        
        # Criar gráfico
        fig = go.Figure()
        
        fig.add_trace(go.Scatter(
            x=contagem_julio.index,
            y=contagem_julio.values,
            name='JULIO',
            mode='lines+markers',
            line=dict(color=self.colors['primary'], width=2),
            marker=dict(size=8)
        ))
        
        fig.add_trace(go.Scatter(
            x=contagem_leandro.index,
            y=contagem_leandro.values,
            name='LEANDRO/ADRIANO',
            mode='lines+markers',
            line=dict(color=self.colors['secondary'], width=2),
            marker=dict(size=8)
        ))
        
        fig.update_layout(
            title='Evolução Diária de Demandas - Janeiro 2025',
            xaxis_title='Data',
            yaxis_title='Quantidade de Demandas',
            showlegend=True,
            plot_bgcolor=self.colors['background'],
            paper_bgcolor='white',
            height=400
        )
        
        return fig
    
    def gerar_dashboard(self, port=8050):
        """Cria um dashboard interativo com Dash."""
        app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
        
        app.layout = dbc.Container([
            html.H1("Dashboard de Análise de Demandas", 
                   className="text-center my-4"),
            
            # Seção 1: Análise Geral
            html.H2("Análise Geral", className="mb-3"),
            dbc.Row([
                dbc.Col([
                    dcc.Dropdown(
                        id='aba-selector',
                        options=[
                            {'label': aba, 'value': aba}
                            for aba in self.relatorio['resultados'].keys()
                        ],
                        value=list(self.relatorio['resultados'].keys())[0],
                        className="mb-4"
                    )
                ])
            ]),
            
            dbc.Row([
                dbc.Col([
                    dcc.Graph(id='status-graph')
                ], md=6),
                dbc.Col([
                    dcc.Graph(id='problemas-graph')
                ], md=6)
            ]),
            
            # Seção 2: Análise Comparativa Janeiro/2025
            html.H2("Análise Comparativa - Janeiro 2025", className="mt-5 mb-3"),
            dbc.Row([
                dbc.Col([
                    dcc.Graph(id='comparativo-bancos')
                ], md=12)
            ]),
            
            dbc.Row([
                dbc.Col([
                    dcc.Graph(id='pizza-quitacoes')
                ], md=6),
                dbc.Col([
                    dcc.Graph(id='timeline-quitacoes')
                ], md=6)
            ])
        ], fluid=True)
        
        @app.callback(
            [dash.Output('status-graph', 'figure'),
             dash.Output('problemas-graph', 'figure'),
             dash.Output('comparativo-bancos', 'figure'),
             dash.Output('pizza-quitacoes', 'figure'),
             dash.Output('timeline-quitacoes', 'figure')],
            [dash.Input('aba-selector', 'value')]
        )
        def update_graphs(aba):
            return (
                self.criar_grafico_status(aba),
                self.criar_grafico_problemas(aba),
                self.criar_grafico_comparativo_quitacoes(),
                self.criar_grafico_pizza_quitacoes(),
                self.criar_grafico_timeline_quitacoes()
            )
        
        print(f"\nIniciando dashboard na porta {port}...")
        print(f"Acesse: http://localhost:{port}")
        app.run_server(debug=True, port=port)
    
    def exportar_graficos(self, diretorio_saida):
        """
        Exporta todos os gráficos como imagens estáticas.
        """
        diretorio_saida = Path(diretorio_saida)
        diretorio_saida.mkdir(exist_ok=True)
        
        for aba in self.relatorio['resultados'].keys():
            print(f"\nGerando gráficos para aba: {aba}")
            
            # Status
            self.criar_grafico_status(aba).write_image(
                diretorio_saida / f"status_{aba}.png"
            )
            
            # Problemas
            self.criar_grafico_problemas(aba).write_image(
                diretorio_saida / f"problemas_{aba}.png"
            )
            
            # Distribuição Temporal
            self.criar_grafico_distribuicao_temporal(aba).write_image(
                diretorio_saida / f"temporal_{aba}.png"
            )
            
            # Bancos
            self.criar_grafico_bancos(aba).write_image(
                diretorio_saida / f"bancos_{aba}.png"
            )
        
        print(f"\nGráficos exportados para: {diretorio_saida}")


def main():
    """Função principal do script."""
    # Encontrar arquivos mais recentes
    output_dirs = [d for d in Path().glob("output_*") if d.is_dir()]
    if not output_dirs:
        print("Diretório de output não encontrado!")
        return
    
    output_dir = max(output_dirs)
    excel_file = max(output_dir.glob("*PROCESSADO*.xlsx"))
    relatorio_file = max(output_dir.glob("relatorio*.json"))
    
    # Criar visualizador
    visualizer = DemandaVisualizer(excel_file, relatorio_file)
    
    # Gerar dashboard interativo
    visualizer.gerar_dashboard()
    
    # Exportar gráficos estáticos
    visualizer.exportar_graficos(output_dir / "graficos")


if __name__ == "__main__":
    main()

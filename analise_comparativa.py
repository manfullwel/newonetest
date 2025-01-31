#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Análise Comparativa de Demandas
==============================

Análise detalhada comparando as demandas entre JULIO e LEANDRO/ADRIANO,
com foco em quitações e performance por banco em Janeiro/2025.
"""

import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from pathlib import Path
from datetime import datetime

class AnalisadorComparativo:
    def __init__(self, arquivo_excel):
        self.excel_file = pd.ExcelFile(arquivo_excel)
        self.colors = {
            'julio': '#2C3E50',      # Azul escuro
            'leandro': '#E74C3C',    # Vermelho
            'quitado': '#27AE60',    # Verde
            'pendente': '#F39C12',   # Laranja
            'background': '#ECF0F1'  # Cinza claro
        }
        
        # Carregar dados
        self.df_julio = pd.read_excel(self.excel_file, 'DEMANDAS JULIO')
        self.df_leandro = pd.read_excel(self.excel_file, 'DEMANDA LEANDROADRIANO')
        self.df_quitados = pd.read_excel(self.excel_file, 'QUITADOS')
        
        # Preparar dados
        for df in [self.df_julio, self.df_leandro, self.df_quitados]:
            df['DATA'] = pd.to_datetime(df['DATA'])
            df['MES'] = df['DATA'].dt.to_period('M')
            
        # Filtrar janeiro/2025
        self.periodo = pd.Period('2025-01')
        self.df_julio_jan = self.df_julio[self.df_julio['MES'] == self.periodo]
        self.df_leandro_jan = self.df_leandro[self.df_leandro['MES'] == self.periodo]
        self.df_quitados_jan = self.df_quitados[self.df_quitados['MES'] == self.periodo]

    def analisar_performance_geral(self):
        """Análise geral de performance."""
        dados = {
            'JULIO': {
                'total_demandas': len(self.df_julio_jan),
                'media_diaria': len(self.df_julio_jan) / self.df_julio_jan['DATA'].dt.day.nunique(),
                'bancos_distintos': self.df_julio_jan['BANCO'].nunique(),
                'quitacoes': len(self.df_quitados_jan[self.df_quitados_jan['RESPONSAVEL'] == 'JULIO'])
            },
            'LEANDRO/ADRIANO': {
                'total_demandas': len(self.df_leandro_jan),
                'media_diaria': len(self.df_leandro_jan) / self.df_leandro_jan['DATA'].dt.day.nunique(),
                'bancos_distintos': self.df_leandro_jan['BANCO'].nunique(),
                'quitacoes': len(self.df_quitados_jan[self.df_quitados_jan['RESPONSAVEL'].isin(['LEANDRO', 'ADRIANO'])])
            }
        }
        
        return pd.DataFrame(dados).round(2)

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
            marker_color=self.colors['julio']
        ))
        
        # Barras para LEANDRO/ADRIANO
        fig.add_trace(go.Bar(
            name='LEANDRO/ADRIANO',
            x=leandro_bancos.index,
            y=leandro_bancos.values,
            marker_color=self.colors['leandro']
        ))
        
        fig.update_layout(
            title='Top 5 Bancos por Responsável - Janeiro 2025',
            barmode='group',
            xaxis_title='Banco',
            yaxis_title='Quantidade de Demandas',
            plot_bgcolor=self.colors['background'],
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
            marker=dict(color=self.colors['julio'], size=8)
        ))
        
        fig.add_trace(go.Scatter(
            x=julio_ma.index,
            y=julio_ma.values,
            name='JULIO - Média Móvel',
            line=dict(color=self.colors['julio'], width=2)
        ))
        
        # Dados diários e média móvel para LEANDRO/ADRIANO
        fig.add_trace(go.Scatter(
            x=leandro_daily.index,
            y=leandro_daily.values,
            name='LEANDRO/ADRIANO - Diário',
            mode='markers',
            marker=dict(color=self.colors['leandro'], size=8)
        ))
        
        fig.add_trace(go.Scatter(
            x=leandro_ma.index,
            y=leandro_ma.values,
            name='LEANDRO/ADRIANO - Média Móvel',
            line=dict(color=self.colors['leandro'], width=2)
        ))
        
        fig.update_layout(
            title='Evolução Diária de Demandas com Média Móvel (3 dias)',
            xaxis_title='Data',
            yaxis_title='Quantidade de Demandas',
            plot_bgcolor=self.colors['background'],
            paper_bgcolor='white',
            height=500
        )
        
        return fig

    def analisar_quitacoes_detalhado(self):
        """Análise detalhada das quitações por responsável e banco."""
        # Preparar dados de quitações
        quitacoes = self.df_quitados_jan.copy()
        
        # Mapear responsáveis para seus grupos
        quitacoes['GRUPO'] = quitacoes['RESPONSAVEL'].map(
            lambda x: 'LEANDRO/ADRIANO' if x in ['LEANDRO', 'ADRIANO'] else 'JULIO' if x == 'JULIO' else 'OUTROS'
        )
        
        # Filtrar apenas os grupos principais
        quitacoes = quitacoes[quitacoes['GRUPO'].isin(['JULIO', 'LEANDRO/ADRIANO'])]
        
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
            'JULIO': '#2C3E50',          # Azul escuro
            'LEANDRO/ADRIANO': '#E74C3C'  # Vermelho
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
            plot_bgcolor=self.colors['background'],
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
            plot_bgcolor=self.colors['background'],
            paper_bgcolor='white',
            height=500,
            showlegend=True
        )
        
        # Salvar todos os gráficos
        fig.write_html('relatorios/taxas_quitacao.html')
        fig.write_image('relatorios/taxas_quitacao.png')
        fig_pizza.write_html('relatorios/distribuicao_quitacoes.html')
        fig_pizza.write_image('relatorios/distribuicao_quitacoes.png')
        fig_bancos.write_html('relatorios/quitacoes_por_banco.html')
        fig_bancos.write_image('relatorios/quitacoes_por_banco.png')
        
        # Salvar dados em Excel com formatação
        with pd.ExcelWriter('relatorios/analise_detalhada.xlsx', engine='openpyxl') as writer:
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

    def gerar_relatorio(self, diretorio_saida='relatorios'):
        """Gera relatório completo com todas as análises."""
        diretorio_saida = Path(diretorio_saida)
        diretorio_saida.mkdir(exist_ok=True)
        
        # 1. Performance Geral
        print("\nAnalisando performance geral...")
        performance = self.analisar_performance_geral()
        performance.to_excel(diretorio_saida / 'performance_geral.xlsx')
        
        # 2. Bancos Principais
        print("Analisando principais bancos...")
        fig_bancos = self.analisar_bancos_principais()
        fig_bancos.write_html(diretorio_saida / 'bancos_principais.html')
        fig_bancos.write_image(diretorio_saida / 'bancos_principais.png')
        
        # 3. Evolução Diária
        print("Analisando evolução diária...")
        fig_evolucao = self.analisar_evolucao_diaria()
        fig_evolucao.write_html(diretorio_saida / 'evolucao_diaria.html')
        fig_evolucao.write_image(diretorio_saida / 'evolucao_diaria.png')
        
        # 4. Análise de Quitações
        print("Analisando quitações...")
        fig_quitacoes, taxas, fig_pizza, fig_bancos = self.analisar_quitacoes_detalhado()
        
        # Gerar relatório em HTML
        html_content = f"""
        <html>
        <head>
            <title>Relatório Comparativo de Demandas - Janeiro 2025</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 40px; }}
                h1 {{ color: #2C3E50; }}
                h2 {{ color: #E74C3C; margin-top: 30px; }}
                .performance {{ margin: 20px 0; }}
                .graficos {{ margin: 30px 0; }}
            </style>
        </head>
        <body>
            <h1>Relatório Comparativo de Demandas - Janeiro 2025</h1>
            
            <h2>1. Performance Geral</h2>
            <div class="performance">
                {performance.to_html()}
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
        
        with open(diretorio_saida / 'relatorio.html', 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        print(f"\nRelatório gerado com sucesso em: {diretorio_saida}")
        print("Arquivos gerados:")
        print("- relatorio.html (Relatório completo)")
        print("- performance_geral.xlsx (Métricas gerais)")
        print("- bancos_principais.html/png (Gráfico de principais bancos)")
        print("- evolucao_diaria.html/png (Gráfico de evolução)")
        print("- taxas_quitacao.html/png (Gráfico de taxas)")
        print("- distribuicao_quitacoes.html/png (Gráfico de distribuição)")
        print("- quitacoes_por_banco.html/png (Gráfico de quitações por banco)")
        print("- analise_detalhada.xlsx (Dados detalhados)")


def main():
    """Função principal."""
    # Encontrar arquivo mais recente
    arquivos = [f for f in Path().glob("output_*/*PROCESSADO*.xlsx")]
    if not arquivos:
        print("Arquivo processado não encontrado!")
        return
    
    arquivo_entrada = max(arquivos)
    print(f"Usando arquivo: {arquivo_entrada}")
    
    # Criar analisador e gerar relatório
    analisador = AnalisadorComparativo(arquivo_entrada)
    analisador.gerar_relatorio()


if __name__ == "__main__":
    main()

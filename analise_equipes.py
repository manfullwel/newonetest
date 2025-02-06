"""
Módulo para análise detalhada por equipe e membro.
"""
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, timedelta
import os
import logging

# Configurar logging com formato mais detalhado
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class AnalisadorEquipes:
    """Classe para análise detalhada por equipe e membro."""
    
    def __init__(self, caminho_arquivo):
        """Inicializa o analisador com o caminho do arquivo de dados."""
        try:
            logger.info(f"Carregando dados do arquivo: {caminho_arquivo}")
            
            # Carregar dados
            self.df = pd.read_excel(caminho_arquivo)
            logger.info(f"Colunas encontradas: {', '.join(self.df.columns)}")
            
            # Converter colunas de data com formato flexível
            for col in ['DATA_CRIACAO', 'DATA_RESOLUCAO']:
                logger.info(f"Processando coluna {col}")
                if col in self.df.columns:
                    try:
                        # Primeiro, tentar converter assumindo formato brasileiro
                        self.df[col] = pd.to_datetime(self.df[col], format='%d/%m/%Y')
                        logger.info(f"Coluna {col} convertida com formato %d/%m/%Y")
                    except:
                        try:
                            # Tentar formato ISO
                            self.df[col] = pd.to_datetime(self.df[col], format='%Y-%m-%d')
                            logger.info(f"Coluna {col} convertida com formato %Y-%m-%d")
                        except:
                            try:
                                # Tentar formato flexível
                                self.df[col] = pd.to_datetime(self.df[col], format='mixed')
                                logger.info(f"Coluna {col} convertida com formato mixed")
                            except:
                                logger.warning(f"Erro ao converter coluna {col}, usando parse genérico")
                                self.df[col] = pd.to_datetime(self.df[col], errors='coerce')
                                
                    # Verificar valores nulos após conversão
                    nulos = self.df[col].isnull().sum()
                    if nulos > 0:
                        logger.warning(f"Coluna {col} contém {nulos} valores nulos após conversão")
                else:
                    logger.warning(f"Coluna {col} não encontrada no DataFrame")
            
            # Calcular tempo de resolução
            logger.info("Calculando tempo de resolução")
            mask_resolvido = self.df['STATUS'].isin(['QUITADO', 'APROVADO'])
            self.df.loc[mask_resolvido, 'TEMPO_RESOLUCAO'] = (
                self.df.loc[mask_resolvido, 'DATA_RESOLUCAO'] - 
                self.df.loc[mask_resolvido, 'DATA_CRIACAO']
            ).dt.total_seconds() / (24 * 60 * 60)  # Converter para dias
            
            # Verificar dados calculados
            logger.info(f"Total de registros: {len(self.df)}")
            logger.info(f"Registros resolvidos: {mask_resolvido.sum()}")
            logger.info(f"Média de tempo de resolução: {self.df['TEMPO_RESOLUCAO'].mean():.2f} dias")
            
        except Exception as e:
            logger.error(f"Erro ao carregar dados: {str(e)}")
            raise
        
        # Criar diretório para os gráficos
        self.output_dir = 'output_graficos'
        os.makedirs(self.output_dir, exist_ok=True)
        logger.info(f"Diretório de saída criado: {self.output_dir}")

    def criar_grafico_pizza(self, dados, titulo, nome_arquivo):
        """Cria um gráfico de pizza com os dados fornecidos."""
        try:
            fig = px.pie(values=dados.values, names=dados.index, title=titulo)
            fig.write_html(os.path.join(self.output_dir, f'{nome_arquivo}.html'))
            fig.write_image(os.path.join(self.output_dir, f'{nome_arquivo}.png'))
            logger.info(f"Gráfico de pizza criado: {nome_arquivo}")
        except Exception as e:
            logger.error(f"Erro ao criar gráfico de pizza {nome_arquivo}: {str(e)}")
            raise

    def criar_grafico_barras(self, dados, titulo, nome_arquivo, layout_vertical=False):
        """Cria um gráfico de barras com os dados fornecidos."""
        try:
            if layout_vertical:
                fig = px.bar(dados, title=titulo)
            else:
                fig = px.bar(dados, orientation='h', title=titulo)
            fig.write_html(os.path.join(self.output_dir, f'{nome_arquivo}.html'))
            fig.write_image(os.path.join(self.output_dir, f'{nome_arquivo}.png'))
            logger.info(f"Gráfico de barras criado: {nome_arquivo}")
        except Exception as e:
            logger.error(f"Erro ao criar gráfico de barras {nome_arquivo}: {str(e)}")
            raise

    def criar_grafico_linhas(self, dados, titulo, nome_arquivo):
        """Cria um gráfico de linhas com os dados fornecidos."""
        try:
            fig = px.line(dados, title=titulo)
            fig.write_html(os.path.join(self.output_dir, f'{nome_arquivo}.html'))
            fig.write_image(os.path.join(self.output_dir, f'{nome_arquivo}.png'))
            logger.info(f"Gráfico de linhas criado: {nome_arquivo}")
        except Exception as e:
            logger.error(f"Erro ao criar gráfico de linhas {nome_arquivo}: {str(e)}")
            raise

    def analisar_distribuicao_status(self, equipe=None):
        """Analisa a distribuição de status para uma equipe específica ou todas."""
        try:
            if equipe:
                dados = self.df[self.df['EQUIPE'] == equipe]['STATUS'].value_counts()
                self.criar_grafico_pizza(dados, f'Distribuição de Status - {equipe}', f'status_distribuicao_{equipe}'.replace('/', '_'))
            else:
                dados = self.df['STATUS'].value_counts()
                self.criar_grafico_pizza(dados, 'Distribuição de Status - Todas as Equipes', 'status_distribuicao_geral')
        except Exception as e:
            logger.error(f"Erro ao analisar distribuição de status: {str(e)}")
            raise

    def analisar_status_por_operador(self, equipe=None):
        """Analisa o status das demandas por operador."""
        try:
            if equipe:
                dados = pd.crosstab(self.df[self.df['EQUIPE'] == equipe]['OPERADOR'], self.df[self.df['EQUIPE'] == equipe]['STATUS'])
                self.criar_grafico_barras(dados, f'Status por Operador - {equipe}', f'status_operador_{equipe}'.replace('/', '_'))
            else:
                dados = pd.crosstab(self.df['OPERADOR'], self.df['STATUS'])
                self.criar_grafico_barras(dados, 'Status por Operador - Todas as Equipes', 'status_operador_geral')
        except Exception as e:
            logger.error(f"Erro ao analisar status por operador: {str(e)}")
            raise

    def analisar_evolucao_temporal(self, equipe=None):
        """Analisa a evolução temporal dos status."""
        try:
            if equipe:
                df_equipe = self.df[self.df['EQUIPE'] == equipe]
                dados = pd.crosstab(df_equipe['DATA_CRIACAO'].dt.date, df_equipe['STATUS'])
                self.criar_grafico_linhas(dados, f'Evolução Temporal - {equipe}', f'evolucao_temporal_{equipe}'.replace('/', '_'))
            else:
                dados = pd.crosstab(self.df['DATA_CRIACAO'].dt.date, self.df['STATUS'])
                self.criar_grafico_linhas(dados, 'Evolução Temporal - Todas as Equipes', 'evolucao_temporal_geral')
        except Exception as e:
            logger.error(f"Erro ao analisar evolução temporal: {str(e)}")
            raise

    def analisar_tempo_resolucao(self, equipe=None):
        """Analisa o tempo médio de resolução por operador."""
        try:
            if equipe:
                dados = self.df[self.df['EQUIPE'] == equipe].groupby('OPERADOR')['TEMPO_RESOLUCAO'].mean()
                self.criar_grafico_barras(dados, f'Tempo Médio de Resolução (dias) - {equipe}', f'tempo_resolucao_{equipe}'.replace('/', '_'))
            else:
                dados = self.df.groupby('OPERADOR')['TEMPO_RESOLUCAO'].mean()
                self.criar_grafico_barras(dados, 'Tempo Médio de Resolução (dias) - Todas as Equipes', 'tempo_resolucao_geral')
        except Exception as e:
            logger.error(f"Erro ao analisar tempo de resolução: {str(e)}")
            raise

    def analisar_motivos_pendencia(self, equipe=None):
        """Analisa os motivos de pendência por equipe."""
        try:
            if equipe:
                dados = self.df[self.df['EQUIPE'] == equipe]['MOTIVO_PENDENCIA'].value_counts()
                self.criar_grafico_pizza(dados, f'Motivos de Pendência - {equipe}', f'motivos_pendencia_{equipe}'.replace('/', '_'))
            else:
                dados = self.df['MOTIVO_PENDENCIA'].value_counts()
                self.criar_grafico_pizza(dados, 'Motivos de Pendência - Todas as Equipes', 'motivos_pendencia_geral')
        except Exception as e:
            logger.error(f"Erro ao analisar motivos de pendência: {str(e)}")
            raise

    def analisar_quitacoes_por_banco(self):
        """Analisa as quitações por banco."""
        try:
            dados = pd.crosstab(self.df['BANCO'], self.df['STATUS'])
            self.criar_grafico_barras(dados, 'Quitações por Banco', 'quitacoes_por_banco')
        except Exception as e:
            logger.error(f"Erro ao analisar quitações por banco: {str(e)}")
            raise

    def calcular_taxa_quitacao(self):
        """Calcula a taxa de quitação por equipe."""
        try:
            dados = self.df.groupby('EQUIPE').apply(
                lambda x: (x['STATUS'] == 'QUITADO').mean()
            ).sort_values(ascending=False)
            self.criar_grafico_barras(dados, 'Taxa de Quitação por Equipe', 'taxas_quitacao')
        except Exception as e:
            logger.error(f"Erro ao calcular taxa de quitação: {str(e)}")
            raise

    def comparar_equipes(self):
        """Compara métricas entre as equipes."""
        try:
            # Tempo médio de resolução por equipe
            tempo_medio = self.df.groupby('EQUIPE')['TEMPO_RESOLUCAO'].mean()
            self.criar_grafico_barras(tempo_medio, 'Tempo Médio de Resolução por Equipe', 'tempo_medio_equipes')
            
            # Taxa de sucesso por equipe
            taxa_sucesso = self.df.groupby('EQUIPE').apply(
                lambda x: (x['STATUS'].isin(['QUITADO', 'APROVADO'])).mean()
            )
            self.criar_grafico_barras(taxa_sucesso, 'Taxa de Sucesso por Equipe', 'taxa_sucesso_equipes')
            
            # Volume de demandas por equipe
            volume_demandas = self.df['EQUIPE'].value_counts()
            self.criar_grafico_barras(volume_demandas, 'Volume de Demandas por Equipe', 'volume_demandas_equipes')
        except Exception as e:
            logger.error(f"Erro ao comparar equipes: {str(e)}")
            raise

    def gerar_relatorio_html(self):
        """Gera um relatório HTML com todos os gráficos."""
        try:
            equipes = self.df['EQUIPE'].unique()
            logger.info(f"Gerando relatório para {len(equipes)} equipes")
            
            html_content = """
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
                </style>
            </head>
            <body>
                <div class="container">
                    <h1>Relatório de Análise de Equipes</h1>
            """
            
            # Adicionar seção de comparação entre equipes
            html_content += """
                    <h2>Comparação entre Equipes</h2>
                    <div class="graph-container">
                        <iframe src="taxas_quitacao.html"></iframe>
                    </div>
            """
            
            # Adicionar seções para cada equipe
            for equipe in equipes:
                equipe_safe = equipe.replace('/', '_')
                html_content += f"""
                    <h2>Análise da Equipe: {equipe}</h2>
                    <div class="graph-container">
                        <h3>Distribuição de Status</h3>
                        <iframe src="status_distribuicao_{equipe_safe}.html"></iframe>
                    </div>
                    <div class="graph-container">
                        <h3>Status por Operador</h3>
                        <iframe src="status_operador_{equipe_safe}.html"></iframe>
                    </div>
                    <div class="graph-container">
                        <h3>Evolução Temporal</h3>
                        <iframe src="evolucao_temporal_{equipe_safe}.html"></iframe>
                    </div>
                    <div class="graph-container">
                        <h3>Tempo de Resolução</h3>
                        <iframe src="tempo_resolucao_{equipe_safe}.html"></iframe>
                    </div>
                    <div class="graph-container">
                        <h3>Motivos de Pendência</h3>
                        <iframe src="motivos_pendencia_{equipe_safe}.html"></iframe>
                    </div>
                """
            
            html_content += """
                </div>
            </body>
            </html>
            """
            
            with open(os.path.join(self.output_dir, 'relatorio_equipes.html'), 'w', encoding='utf-8') as f:
                f.write(html_content)
            
            logger.info("Relatório HTML gerado com sucesso")
            
        except Exception as e:
            logger.error(f"Erro ao gerar relatório HTML: {str(e)}")
            raise

    def gerar_relatorio_completo(self, arquivo):
        """
        Gera relatório completo com todas as análises.
        
        Args:
            arquivo: Caminho para o arquivo de dados
            
        Returns:
            bool: True se o relatório foi gerado com sucesso, False caso contrário
        """
        try:
            # Carregar dados
            if not self.carregar_dados(arquivo):
                return False
            
            # Realizar comparação entre equipes
            comparacao = self.comparar_equipes()
            
            # Gerar gráficos para cada equipe
            for equipe, metricas in comparacao.items():
                self.gerar_graficos_equipe(equipe, metricas)
            
            # Gerar relatório HTML
            self.gerar_relatorio_html(comparacao)
            
            logger.info("Relatório gerado com sucesso!")
            return True
            
        except Exception as e:
            logger.error(f"Erro ao gerar relatório: {str(e)}")
            return False

    def carregar_dados(self, arquivo):
        """
        Carrega dados do arquivo Excel.
        
        Args:
            arquivo: Caminho para o arquivo Excel
        """
        try:
            self.df = pd.read_excel(arquivo)
            
            # Converter colunas de data com formato flexível
            for col in ['DATA_CRIACAO', 'DATA_RESOLUCAO']:
                try:
                    self.df[col] = pd.to_datetime(self.df[col], format='mixed')
                except:
                    logger.warning(f"Erro ao converter coluna {col}, tentando outros formatos...")
                    try:
                        self.df[col] = pd.to_datetime(self.df[col], format='%d/%m/%Y')
                    except:
                        logger.warning(f"Erro ao converter coluna {col}, usando parse genérico...")
                        self.df[col] = pd.to_datetime(self.df[col], errors='coerce')
            
            return True
        except Exception as e:
            logger.error(f"Erro ao carregar dados: {str(e)}")
            return False
    
    def gerar_graficos_equipe(self, equipe, metricas):
        """
        Gera gráficos para uma equipe específica.
        
        Args:
            equipe: Nome da equipe
            metricas: Métricas da equipe
        """
        # Normalizar nome da equipe para uso em nome de arquivo
        nome_arquivo = equipe.replace('/', '_').replace('\\', '_').replace(' ', '_')
        
        # Filtrar dados da equipe
        df_equipe = self.df[self.df['EQUIPE'] == equipe]
        
        # 1. Gráfico de pizza com distribuição de status
        labels = ['Quitados', 'Aprovados', 'Em Análise', 'Pendentes', 'Resolvidos']
        values = [
            metricas['Quitados'],
            metricas['Aprovados'],
            metricas['Em Análise'],
            metricas['Pendentes'],
            metricas['Resolvidos']
        ]
        
        fig_pizza = go.Figure(data=[go.Pie(labels=labels, values=values)])
        fig_pizza.update_layout(
            title=f'Distribuição de Status - {equipe}',
            height=600
        )
        fig_pizza.write_html(os.path.join(self.output_dir, f'status_distribuicao_{nome_arquivo}.html'))
        fig_pizza.write_image(os.path.join(self.output_dir, f'status_distribuicao_{nome_arquivo}.png'))
        
        # 2. Gráfico de barras por operador
        df_op = metricas['Operadores']
        fig_op = go.Figure()
        
        status_cols = ['Quitados', 'Aprovados', 'Em Análise', 'Pendentes', 'Resolvidos']
        for status in status_cols:
            fig_op.add_trace(go.Bar(
                name=status,
                x=df_op['Operador'],
                y=df_op[status],
                text=df_op[status],
                textposition='auto',
            ))
        
        fig_op.update_layout(
            title=f'Status por Operador - {equipe}',
            barmode='group',
            xaxis_title='Operador',
            yaxis_title='Quantidade',
            height=600
        )
        fig_op.write_html(os.path.join(self.output_dir, f'status_operador_{nome_arquivo}.html'))
        fig_op.write_image(os.path.join(self.output_dir, f'status_operador_{nome_arquivo}.png'))
        
        # 3. Gráfico de evolução temporal
        df_temporal = df_equipe.copy()
        df_temporal['Data'] = df_temporal['DATA_CRIACAO'].dt.date
        df_temporal = df_temporal.groupby(['Data', 'STATUS']).size().unstack(fill_value=0)
        
        fig_temporal = go.Figure()
        for status in df_temporal.columns:
            fig_temporal.add_trace(go.Scatter(
                x=df_temporal.index,
                y=df_temporal[status],
                name=status,
                mode='lines+markers'
            ))
        
        fig_temporal.update_layout(
            title=f'Evolução Temporal por Status - {equipe}',
            xaxis_title='Data',
            yaxis_title='Quantidade',
            height=600
        )
        fig_temporal.write_html(os.path.join(self.output_dir, f'evolucao_temporal_{nome_arquivo}.html'))
        fig_temporal.write_image(os.path.join(self.output_dir, f'evolucao_temporal_{nome_arquivo}.png'))
        
        # 4. Gráfico de tempo médio de resolução por operador
        fig_tempo = go.Figure()
        fig_tempo.add_trace(go.Bar(
            x=df_op['Operador'],
            y=df_op['Tempo Médio (dias)'],
            text=df_op['Tempo Médio (dias)'].round(1),
            textposition='auto'
        ))
        
        fig_tempo.update_layout(
            title=f'Tempo Médio de Resolução por Operador - {equipe}',
            xaxis_title='Operador',
            yaxis_title='Dias',
            height=600
        )
        fig_tempo.write_html(os.path.join(self.output_dir, f'tempo_resolucao_{nome_arquivo}.html'))
        fig_tempo.write_image(os.path.join(self.output_dir, f'tempo_resolucao_{nome_arquivo}.png'))
        
        # 5. Gráfico de motivos de pendência
        if metricas['Motivos Pendência']:
            motivos = pd.Series(metricas['Motivos Pendência'])
            fig_motivos = go.Figure(data=[go.Bar(
                x=motivos.index,
                y=motivos.values,
                text=motivos.values,
                textposition='auto'
            )])
            
            fig_motivos.update_layout(
                title=f'Motivos de Pendência - {equipe}',
                xaxis_title='Motivo',
                yaxis_title='Quantidade',
                height=600
            )
            fig_motivos.write_html(os.path.join(self.output_dir, f'motivos_pendencia_{nome_arquivo}.html'))
            fig_motivos.write_image(os.path.join(self.output_dir, f'motivos_pendencia_{nome_arquivo}.png'))
    
    def comparar_equipes(self):
        """
        Compara métricas entre as equipes.
        
        Returns:
            dict: Dicionário com comparação entre equipes
        """
        comparacao = {}
        
        for equipe in self.df['EQUIPE'].unique():
            comparacao[equipe] = self.analisar_equipe(equipe)
        
        return comparacao
    
    def analisar_equipe(self, equipe):
        """
        Analisa métricas para uma equipe específica.
        
        Args:
            equipe: Nome da equipe para análise
        
        Returns:
            dict: Dicionário com métricas da equipe
        """
        df_equipe = self.df[self.df['EQUIPE'] == equipe]
        
        # Métricas por operador
        metricas_operadores = []
        for operador in df_equipe['OPERADOR'].unique():
            df_op = df_equipe[df_equipe['OPERADOR'] == operador]
            
            # Calcular tempo médio de resolução
            tempo_medio = df_op[df_op['TEMPO_RESOLUCAO'].notna()]['TEMPO_RESOLUCAO'].mean()
            
            metricas = {
                'Operador': operador,
                'Total Demandas': len(df_op),
                'Quitados': len(df_op[df_op['STATUS'] == 'QUITADO']),
                'Aprovados': len(df_op[df_op['STATUS'] == 'APROVADO']),
                'Em Análise': len(df_op[df_op['STATUS'] == 'ANALISE']),
                'Pendentes': len(df_op[df_op['STATUS'] == 'PENDENTE']),
                'Resolvidos': len(df_op[df_op['STATUS'] == 'RESOLVIDO']),
                'Tempo Médio (dias)': round(tempo_medio, 1) if not pd.isna(tempo_medio) else 0,
                'Alta Prioridade': len(df_op[df_op['PRIORIDADE'] == 'ALTA']),
                'Taxa Sucesso': round(
                    len(df_op[df_op['STATUS'].isin(['QUITADO', 'APROVADO', 'RESOLVIDO'])]) / len(df_op) * 100, 2
                ) if len(df_op) > 0 else 0
            }
            metricas_operadores.append(metricas)
        
        # Análise de motivos de pendência
        motivos_pendencia = df_equipe[df_equipe['MOTIVO_PENDENCIA'].notna()]['MOTIVO_PENDENCIA'].value_counts().to_dict()
        
        # Métricas gerais da equipe
        metricas_equipe = {
            'Total Demandas': len(df_equipe),
            'Quitados': len(df_equipe[df_equipe['STATUS'] == 'QUITADO']),
            'Aprovados': len(df_equipe[df_equipe['STATUS'] == 'APROVADO']),
            'Em Análise': len(df_equipe[df_equipe['STATUS'] == 'ANALISE']),
            'Pendentes': len(df_equipe[df_equipe['STATUS'] == 'PENDENTE']),
            'Resolvidos': len(df_equipe[df_equipe['STATUS'] == 'RESOLVIDO']),
            'Tempo Médio (dias)': round(df_equipe[df_equipe['TEMPO_RESOLUCAO'].notna()]['TEMPO_RESOLUCAO'].mean(), 1),
            'Taxa Sucesso': round(
                len(df_equipe[df_equipe['STATUS'].isin(['QUITADO', 'APROVADO', 'RESOLVIDO'])]) / len(df_equipe) * 100, 2
            ) if len(df_equipe) > 0 else 0,
            'Operadores': pd.DataFrame(metricas_operadores),
            'Motivos Pendência': motivos_pendencia
        }
        
        return metricas_equipe
    
def main():
    """Função principal que executa todas as análises."""
    try:
        logger.info("Iniciando análise de equipes")
        
        # Criar analisador
        analisador = AnalisadorEquipes('output_20250131_0218/dados_PROCESSADO_2025.xlsx')
        
        # Executar análises por equipe
        equipes = analisador.df['EQUIPE'].unique()
        logger.info(f"Analisando {len(equipes)} equipes")
        
        for equipe in equipes:
            logger.info(f"Analisando equipe: {equipe}")
            analisador.analisar_distribuicao_status(equipe)
            analisador.analisar_status_por_operador(equipe)
            analisador.analisar_evolucao_temporal(equipe)
            analisador.analisar_tempo_resolucao(equipe)
            analisador.analisar_motivos_pendencia(equipe)
        
        # Executar análises gerais
        logger.info("Executando análises gerais")
        analisador.analisar_quitacoes_por_banco()
        analisador.calcular_taxa_quitacao()
        analisador.comparar_equipes()
        
        # Gerar relatório HTML
        logger.info("Gerando relatório HTML")
        analisador.gerar_relatorio_html()
        
        logger.info("Análise concluída com sucesso!")
        
    except Exception as e:
        logger.error(f"Erro ao executar análise: {str(e)}")
        raise

if __name__ == "__main__":
    main()

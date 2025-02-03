"""
Dashboard de Performance de Equipes usando Streamlit
- Interface responsiva
- Busca rápida por responsável
- Métricas por equipe
- Visualizações otimizadas
"""

import streamlit as st

# Configuração da página - deve ser a primeira chamada Streamlit
st.set_page_config(
    page_title="Dashboard de Equipes",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Importações padrão
import pandas as pd
import numpy as np
from datetime import datetime
import os
import logging
import queue
import sys
import traceback
from pathlib import Path
import time
import tempfile
import psutil
from io import BytesIO
from typing import Dict, List, Optional, Union, Tuple
from dataclasses import dataclass
from enum import Enum, auto
import threading

# Importações para visualização
import plotly.graph_objects as go
import plotly.express as px

# Importações para análise de dados
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
from scipy.linalg import svd
from scipy import stats

# Configuração de logging e debug
DEBUG = True  # Modo debug ativado

# Definição de estados e tipos
class DataStatus(Enum):
    """Estados possíveis do processamento de dados"""
    IDLE = auto()        # Aguardando
    PROCESSING = auto()  # Processando
    COMPLETED = auto()   # Concluído
    ERROR = auto()       # Erro
    PENDING = auto()     # Pendente

@dataclass
class ProcessingResult:
    """Resultado do processamento de dados"""
    status: DataStatus
    data: Optional[pd.DataFrame] = None
    error: Optional[str] = None
    metrics: Optional[Dict] = None

class AppMonitor:
    """Monitor da aplicação para debug e tratamento de erros"""
    def __init__(self):
        self.start_time = time.time()
        self.error_count = 0
        self.last_error = None
        self.status = DataStatus.IDLE
        
    def log_error(self, error: Exception, context: str = ""):
        """Registra um erro"""
        self.error_count += 1
        self.last_error = {
            'error': str(error),
            'traceback': traceback.format_exc(),
            'context': context,
            'time': datetime.now().strftime("%H:%M:%S")
        }
        debug_logger.error(f"Erro em {context}: {str(error)}")
        
    def get_status(self) -> Dict:
        """Retorna o status atual da aplicação"""
        return {
            'uptime': f"{time.time() - self.start_time:.1f}s",
            'error_count': self.error_count,
            'memory_usage': f"{psutil.Process().memory_info().rss / 1024 / 1024:.1f}MB",
            'status': self.status.name,
            'last_error': self.last_error
        }
    
    def check_system(self) -> List[str]:
        """Verifica o estado do sistema"""
        warnings = []
        
        # Verifica uso de memória
        memory_usage = psutil.Process().memory_info().rss / 1024 / 1024
        if memory_usage > 500:  # Mais de 500MB
            warnings.append(f"Alto uso de memória: {memory_usage:.1f}MB")
        
        # Verifica tempo de execução
        uptime = time.time() - self.start_time
        if uptime > 3600:  # Mais de 1 hora
            warnings.append(f"Aplicação rodando há muito tempo: {uptime/3600:.1f}h")
        
        return warnings

# Inicializar monitor
if 'app_monitor' not in st.session_state:
    st.session_state.app_monitor = AppMonitor()

class DebugLogger:
    def __init__(self):
        self.logger = logging.getLogger('DebugLogger')
        self.logger.setLevel(logging.DEBUG if DEBUG else logging.INFO)
        
        # Handler para arquivo
        file_handler = logging.FileHandler('debug.log')
        file_handler.setFormatter(logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        ))
        self.logger.addHandler(file_handler)
        
        # Handler para console
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(logging.Formatter(
            '%(levelname)s: %(message)s'
        ))
        self.logger.addHandler(console_handler)
    
    def debug(self, msg, exc_info=None):
        if DEBUG:
            if exc_info:
                self.logger.debug(f"{msg}\n{traceback.format_exc()}")
            else:
                self.logger.debug(msg)
    
    def info(self, msg):
        self.logger.info(msg)
    
    def error(self, msg, exc_info=True):
        self.logger.error(msg, exc_info=exc_info)
        if 'app_monitor' in st.session_state:
            st.session_state.app_monitor.log_error(Exception(msg), "Logger")
    
    def exception(self, msg):
        self.logger.exception(msg)
        if 'app_monitor' in st.session_state:
            st.session_state.app_monitor.log_error(Exception(msg), "Logger")

# Inicializar logger
debug_logger = DebugLogger()

def debug_callback(func):
    """Decorator para funções que precisam de debug"""
    def wrapper(*args, **kwargs):
        try:
            debug_logger.debug(f"Chamando {func.__name__} com args={args}, kwargs={kwargs}")
            result = func(*args, **kwargs)
            debug_logger.debug(f"{func.__name__} retornou com sucesso")
            return result
        except Exception as e:
            debug_logger.error(f"Erro em {func.__name__}: {str(e)}")
            if DEBUG:
                st.error(f"""
                    **Erro em {func.__name__}**
                    ```python
                    {traceback.format_exc()}
                    ```
                """)
            raise
    return wrapper

# Estado global da aplicação
if 'debug_messages' not in st.session_state:
    st.session_state.debug_messages = []

def add_debug_message(message: str):
    """Adiciona uma mensagem de debug ao estado da sessão"""
    if DEBUG:
        timestamp = datetime.now().strftime("%H:%M:%S")
        st.session_state.debug_messages.append(f"[{timestamp}] {message}")

# Estilo CSS moderno e responsivo
st.markdown("""
<style>
    /* Reset e variáveis globais */
    :root {
        --primary-color: #FFD700;
        --secondary-color: #1E1E1E;
        --accent-color: #2C2C2C;
        --success-color: #00C853;
        --warning-color: #FFB300;
        --danger-color: #FF3D00;
        --text-primary: #333333;
        --text-secondary: #666666;
        --card-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        --transition-speed: 0.3s;
    }

    /* Layout responsivo */
    .stApp {
        background-color: #F8F9FA;
    }

    @media (max-width: 768px) {
        .responsive-grid {
            grid-template-columns: 1fr !important;
        }
        .metric-card {
            margin: 0.5rem 0 !important;
        }
    }

    /* Header e navegação */
    .header-container {
        background: linear-gradient(135deg, var(--secondary-color), var(--accent-color));
        padding: 1.5rem;
        border-radius: 12px;
        margin-bottom: 2rem;
        position: relative;
        overflow: hidden;
    }

    .header-container::after {
        content: '';
        position: absolute;
        top: 0;
        right: 0;
        width: 100%;
        height: 100%;
        background: linear-gradient(45deg, transparent, rgba(255, 215, 0, 0.1));
        z-index: 1;
    }

    .header-title {
        color: var(--primary-color);
        font-size: 2rem;
        font-weight: 700;
        margin: 0;
        position: relative;
        z-index: 2;
    }

    /* Cards e métricas */
    .metrics-grid {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
        gap: 1.5rem;
        margin-bottom: 2rem;
    }

    .metric-card {
        background: white;
        padding: 1.5rem;
        border-radius: 12px;
        box-shadow: var(--card-shadow);
        transition: transform var(--transition-speed);
        position: relative;
        overflow: hidden;
    }

    .metric-card:hover {
        transform: translateY(-5px);
    }

    .metric-card::before {
        content: '';
        position: absolute;
        top: 0;
        left: 0;
        width: 4px;
        height: 100%;
        background: var(--primary-color);
    }

    .metric-value {
        font-size: 2rem;
        font-weight: 700;
        color: var(--text-primary);
        margin: 0.5rem 0;
    }

    .metric-label {
        color: var(--text-secondary);
        font-size: 0.9rem;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }

    /* Tabelas */
    .styled-table {
        width: 100%;
        background: white;
        border-radius: 12px;
        overflow: hidden;
        box-shadow: var(--card-shadow);
    }

    .styled-table thead {
        background: var(--secondary-color);
        color: var(--primary-color);
    }

    .styled-table th {
        padding: 1rem;
        text-align: left;
        font-weight: 600;
    }

    .styled-table td {
        padding: 1rem;
        border-bottom: 1px solid #eee;
    }

    .styled-table tr:last-child td {
        border-bottom: none;
    }

    /* Gráficos */
    .chart-container {
        background: white;
        padding: 1.5rem;
        border-radius: 12px;
        box-shadow: var(--card-shadow);
        margin-bottom: 2rem;
    }

    /* Status indicators */
    .status-badge {
        padding: 0.25rem 0.75rem;
        border-radius: 50px;
        font-size: 0.85rem;
        font-weight: 500;
    }

    .status-success {
        background: rgba(0, 200, 83, 0.1);
        color: var(--success-color);
    }

    .status-warning {
        background: rgba(255, 179, 0, 0.1);
        color: var(--warning-color);
    }

    .status-danger {
        background: rgba(255, 61, 0, 0.1);
        color: var(--danger-color);
    }

    /* Animações */
    @keyframes fadeIn {
        from { opacity: 0; transform: translateY(20px); }
        to { opacity: 1; transform: translateY(0); }
    }

    .animate-fade-in {
        animation: fadeIn 0.5s ease-out forwards;
    }

    /* Mobile optimizations */
    @media (max-width: 480px) {
        .header-title {
            font-size: 1.5rem;
        }

        .metric-value {
            font-size: 1.5rem;
        }

        .styled-table {
            font-size: 0.9rem;
        }

        .styled-table th,
        .styled-table td {
            padding: 0.75rem;
        }
    }
</style>
""", unsafe_allow_html=True)

import concurrent.futures
from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Optional, Union, Tuple

# Classes e tipos para processamento de dados
class DataProcessor:
    """Classe para processamento assíncrono e análise avançada de dados."""
    
    def __init__(self):
        self.processing_queue = queue.Queue()
        self.processing_thread = None
        self.current_status = DataStatus.IDLE
        self.last_error = None
        debug_logger.debug("DataProcessor inicializado")

    @debug_callback
    def process_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Processa os dados com tratamento de erros e logging
        """
        try:
            add_debug_message("Iniciando processamento de dados")
            if df is None or df.empty:
                raise ValueError("DataFrame vazio ou None")

            # Cópia para não modificar o original
            df = df.copy()
            
            # Limpeza básica
            add_debug_message("Realizando limpeza básica")
            df = self._clean_data(df)
            
            # Detecção de outliers
            add_debug_message("Detectando outliers")
            df = self._handle_outliers(df)
            
            # Imputação de valores faltantes
            add_debug_message("Tratando valores faltantes")
            df = self._impute_missing_values(df)
            
            # Feature engineering
            add_debug_message("Criando novas features")
            df = self._feature_engineering(df)
            
            debug_logger.info("Processamento de dados concluído com sucesso")
            return df
            
        except Exception as e:
            debug_logger.error(f"Erro no processamento de dados: {str(e)}")
            raise

    @debug_callback
    def _clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Limpeza básica dos dados"""
        try:
            # Remove linhas duplicadas
            df = df.drop_duplicates()
            
            # Remove linhas com todos os valores ausentes
            df = df.dropna(how='all')
            
            # Normaliza nomes das colunas
            df.columns = [col.strip().upper() for col in df.columns]
            
            return df
        except Exception as e:
            debug_logger.error(f"Erro na limpeza de dados: {str(e)}")
            raise

    @debug_callback
    def _handle_outliers(self, df: pd.DataFrame) -> pd.DataFrame:
        """Detecção e tratamento de outliers"""
        try:
            for col in df.select_dtypes(include=[np.number]).columns:
                # Calcula z-score
                z_scores = stats.zscore(df[col], nan_policy='omit')
                # Marca outliers (|z| > 3)
                outliers = abs(z_scores) > 3
                # Registra quantidade de outliers
                n_outliers = outliers.sum()
                if n_outliers > 0:
                    add_debug_message(f"Encontrados {n_outliers} outliers na coluna {col}")
            return df
        except Exception as e:
            debug_logger.error(f"Erro no tratamento de outliers: {str(e)}")
            raise

    @debug_callback
    def _impute_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """Imputação de valores faltantes"""
        try:
            # Para colunas numéricas
            num_cols = df.select_dtypes(include=[np.number]).columns
            for col in num_cols:
                if df[col].isnull().any():
                    median_val = df[col].median()
                    df[col].fillna(median_val, inplace=True)
                    add_debug_message(f"Preenchidos valores faltantes na coluna {col}")
            
            # Para colunas categóricas
            cat_cols = df.select_dtypes(exclude=[np.number]).columns
            for col in cat_cols:
                if df[col].isnull().any():
                    mode_val = df[col].mode()[0]
                    df[col].fillna(mode_val, inplace=True)
                    add_debug_message(f"Preenchidos valores faltantes na coluna {col}")
            
            return df
        except Exception as e:
            debug_logger.error(f"Erro na imputação de valores: {str(e)}")
            raise

    @debug_callback
    def _feature_engineering(self, df: pd.DataFrame) -> pd.DataFrame:
        """Criação de novas features"""
        try:
            # Adiciona colunas de data/hora se houver coluna de data
            date_cols = [col for col in df.columns if 'DATA' in col.upper()]
            for col in date_cols:
                if pd.api.types.is_datetime64_any_dtype(df[col]):
                    df[f'{col}_HORA'] = df[col].dt.hour
                    df[f'{col}_DIA_SEMANA'] = df[col].dt.day_name()
                    add_debug_message(f"Criadas features temporais para coluna {col}")
            
            return df
        except Exception as e:
            debug_logger.error(f"Erro na engenharia de features: {str(e)}")
            raise

# Inicializa o processador de dados como singleton
if 'data_processor' not in st.session_state:
    st.session_state.data_processor = DataProcessor()

def upload_and_process_file():
    """Interface para upload e processamento de arquivos."""
    st.markdown("""
        <div class="header-container">
            <h1 class="header-title">Importar Dados</h1>
        </div>
    """, unsafe_allow_html=True)

    uploaded_file = st.file_uploader("Escolha um arquivo CSV ou Excel", type=['csv', 'xlsx'])
    
    if uploaded_file is not None:
        # Salva o arquivo temporariamente
        temp_path = f"temp_{uploaded_file.name}"
        with open(temp_path, 'wb') as f:
            f.write(uploaded_file.getvalue())
        
        # Submete para processamento
        df = st.session_state.data_processor.process_data(pd.read_csv(temp_path))
        st.session_state.current_file_id = df
        
        # Mostra status do processamento
        if df is not None:
            st.success("Arquivo processado com sucesso!")
            
            # Mostra métricas avançadas
            if df is not None:
                st.subheader("Métricas Avançadas")
                col1, col2 = st.columns(2)
                
                with col1:
                    st.metric("Rank Efetivo", f"{df.get('effective_rank', 0):.0f}")
                    st.metric("Variância Explicada", f"{df.get('explained_variance_ratio', 0):.2%}")
                
                with col2:
                    st.metric("Correlação Máxima", f"{df.get('max_correlation', 0):.2f}")
                    st.metric("Desvio Absoluto Médio", f"{df.get('mean_absolute_deviation', 0):.2f}")
            
            return df
            
        elif df is None:
            st.error(f"Erro no processamento: {df}")
        else:
            st.info("Processando arquivo...")
            st.progress(0.5)
    
    return None

def validar_dataframe(df: pd.DataFrame) -> bool:
    """Valida se o DataFrame tem todas as colunas necessárias."""
    colunas_necessarias = [
        'EQUIPE', 'OPERADOR', 'STATUS', 'DATA_CRIACAO', 
        'DATA_RESOLUCAO', 'TEMPO_RESOLUCAO'
    ]
    return all(coluna in df.columns for coluna in colunas_necessarias)

def tratar_dados(df: pd.DataFrame) -> pd.DataFrame:
    """Trata e limpa os dados do DataFrame."""
    try:
        # Copia o DataFrame para não modificar o original
        df = df.copy()
        
        # Converte datas
        for col in ['DATA_CRIACAO', 'DATA_RESOLUCAO']:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        
        # Remove linhas com datas inválidas em DATA_CRIACAO
        df = df.dropna(subset=['DATA_CRIACAO'])
        
        # Calcula tempo de resolução
        df['TEMPO_RESOLUCAO'] = (df['DATA_RESOLUCAO'] - df['DATA_CRIACAO']).dt.total_seconds() / (24 * 60 * 60)
        
        # Marca sucesso
        df['SUCESSO'] = df['STATUS'].isin(['QUITADO', 'APROVADO', 'RESOLVIDO'])
        
        # Adiciona coluna de mês
        df['MES'] = df['DATA_CRIACAO'].dt.to_period('M')
        
        # Limpa strings
        for col in ['EQUIPE', 'OPERADOR', 'STATUS']:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip().str.upper()
        
        return df
    except Exception as e:
        st.error(f"Erro ao tratar dados: {str(e)}")
        return None

@st.cache_data(ttl=300)  # Cache por 5 minutos
def carregar_dados(caminho_arquivo: str) -> pd.DataFrame:
    """
    Carrega e processa os dados do arquivo Excel/CSV com cache inteligente.
    """
    try:
        # Verifica se o arquivo existe
        if not os.path.exists(caminho_arquivo):
            st.error(f"Arquivo não encontrado: {caminho_arquivo}")
            return None

        # Obtém o timestamp da última modificação do arquivo
        last_modified = os.path.getmtime(caminho_arquivo)
        
        @st.cache_data(ttl=300)  # Cache por 5 minutos
        def load_file(path: str, modified: float) -> pd.DataFrame:
            try:
                if path.endswith('.xlsx'):
                    # Configurações específicas para Excel
                    df = pd.read_excel(
                        path,
                        engine='openpyxl',
                        na_values=['NA', 'N/A', ''],  # Valores considerados como NA
                        keep_default_na=True
                    )
                else:
                    # Configurações para CSV
                    df = pd.read_csv(
                        path,
                        encoding='utf-8',
                        na_values=['NA', 'N/A', ''],
                        keep_default_na=True
                    )
                
                # Validação básica dos dados
                if df.empty:
                    raise ValueError("O arquivo está vazio")
                
                # Remove linhas completamente vazias
                df = df.dropna(how='all')
                
                # Remove colunas completamente vazias
                df = df.dropna(axis=1, how='all')
                
                # Converte colunas de data
                for col in df.columns:
                    # Tenta converter para datetime se a coluna contiver 'DATA' no nome
                    if 'DATA' in col.upper():
                        try:
                            df[col] = pd.to_datetime(df[col], errors='coerce')
                        except Exception as e:
                            logger.warning(f"Não foi possível converter coluna {col} para datetime: {str(e)}")
                
                return df
                
            except Exception as e:
                logger.error(f"Erro ao carregar arquivo {path}: {str(e)}")
                raise
        
        # Tenta carregar o arquivo
        df = load_file(caminho_arquivo, last_modified)
        
        if df is None:
            st.error("Erro ao carregar dados do arquivo")
            return None
        
        # Processamento adicional dos dados
        df = processar_dados(df)
        
        return df
        
    except Exception as e:
        st.error(f"Erro ao carregar dados: {str(e)}")
        logger.exception("Erro detalhado ao carregar dados:")
        return None

def processar_dados(df: pd.DataFrame) -> pd.DataFrame:
    """
    Processa e limpa os dados carregados.
    """
    try:
        df = df.copy()
        
        # Normaliza nomes das colunas
        df.columns = [col.strip().upper() for col in df.columns]
        
        # Converte tipos de dados apropriados
        for col in df.columns:
            # Para colunas numéricas
            if df[col].dtype == 'object':
                try:
                    # Tenta converter para numérico
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                except:
                    # Se falhar, mantém como está
                    pass
            
            # Para colunas de texto
            if df[col].dtype == 'object':
                df[col] = df[col].astype(str).str.strip().str.upper()
        
        # Adiciona colunas calculadas
        if 'DATA' in df.columns:
            df['PERIODO'] = df['DATA'].dt.hour.apply(lambda x: 
                'Manhã' if 6 <= x < 12 else
                'Tarde' if 12 <= x < 18 else
                'Noite' if 18 <= x < 24 else 'Madrugada'
            )
        
        # Calcula métricas derivadas
        if 'STATUS' in df.columns:
            df['RESOLVIDO'] = df['STATUS'].str.contains('RESOLVIDO', case=False, na=False)
        
        if 'TEMPO_RESOLUCAO' in df.columns:
            df['TEMPO_RESOLUCAO'] = pd.to_numeric(df['TEMPO_RESOLUCAO'], errors='coerce')
        
        return df
        
    except Exception as e:
        logger.error(f"Erro ao processar dados: {str(e)}")
        raise

def get_available_columns(df: pd.DataFrame) -> Dict[str, List[str]]:
    """Retorna as colunas disponíveis para filtros"""
    colunas = {
        'periodo': 'PERIODO',
        'equipe': 'EQUIPE',
        'alternativas': ['Period', 'Período', 'PERIOD', 'PERÍODO'],
        'equipe_alt': ['Team', 'Equipe', 'TEAM', 'EQUIPE']
    }
    
    available = {}
    
    # Verificar coluna de período
    if colunas['periodo'] in df.columns:
        available['periodo'] = colunas['periodo']
    else:
        for alt in colunas['alternativas']:
            if alt in df.columns:
                available['periodo'] = alt
                break
    
    # Verificar coluna de equipe
    if colunas['equipe'] in df.columns:
        available['equipe'] = colunas['equipe']
    else:
        for alt in colunas['equipe_alt']:
            if alt in df.columns:
                available['equipe'] = alt
                break
    
    return available

def mostrar_filtros(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, str]]:
    """Mostra e aplica os filtros disponíveis"""
    colunas = get_available_columns(df)
    filtros_aplicados = {}
    
    if not colunas:
        st.warning("⚠️ Nenhuma coluna de filtro encontrada no arquivo")
        return df, {}
    
    col1, col2 = st.columns(2)
    
    # Filtro de período
    if 'periodo' in colunas:
        with col1:
            periodo = st.selectbox(
                "Filtrar por Período",
                ["Todos"] + sorted(df[colunas['periodo']].unique().tolist())
            )
            if periodo != "Todos":
                df = df[df[colunas['periodo']] == periodo]
                filtros_aplicados['periodo'] = periodo
    
    # Filtro de equipe
    if 'equipe' in colunas:
        with col2:
            equipe = st.selectbox(
                "Filtrar por Equipe",
                ["Todas"] + sorted(df[colunas['equipe']].unique().tolist())
            )
            if equipe != "Todas":
                df = df[df[colunas['equipe']] == equipe]
                filtros_aplicados['equipe'] = equipe
    
    return df, filtros_aplicados

def criar_grafico_status(df: pd.DataFrame, equipe: Optional[str] = None) -> go.Figure:
    """Cria gráfico de status."""
    if equipe:
        df_plot = df[df['EQUIPE'] == equipe]
    else:
        df_plot = df
        
    status_counts = df_plot['STATUS'].value_counts()
    
    cores = ['#ffd700', '#ffed4a', '#fff7b2', '#ffffd4', '#f4f4f4']
    
    fig = go.Figure(data=[
        go.Pie(
            labels=status_counts.index,
            values=status_counts.values,
            hole=0.4,
            marker=dict(colors=cores)
        )
    ])
    
    fig.update_layout(
        title=f"Distribuição de Status{' - ' + equipe if equipe else ''}",
        showlegend=True,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        paper_bgcolor='#252525',
        plot_bgcolor='#252525',
        font=dict(color='#ffd700'),
        height=400,
        margin=dict(t=50, l=0, r=0, b=0)
    )
    
    return fig

def criar_grafico_evolucao_temporal(df: pd.DataFrame, equipe: Optional[str] = None) -> go.Figure:
    """Cria gráfico de evolução temporal."""
    if equipe:
        df_plot = df[df['EQUIPE'] == equipe]
    else:
        df_plot = df
    
    evolucao = df_plot.groupby(['MES', 'STATUS']).size().unstack(fill_value=0)
    
    fig = go.Figure()
    
    for status in evolucao.columns:
        fig.add_trace(
            go.Scatter(
                x=evolucao.index.astype(str),
                y=evolucao[status],
                name=status,
                mode='lines+markers'
            )
        )
    
    fig.update_layout(
        title=f"Evolução Temporal{' - ' + equipe if equipe else ''}",
        xaxis_title="Mês",
        yaxis_title="Quantidade",
        showlegend=True,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        paper_bgcolor='#252525',
        plot_bgcolor='#252525',
        font=dict(color='#ffd700'),
        height=400,
        margin=dict(t=50, l=0, r=0, b=0)
    )
    
    return fig

def criar_grafico_status_operador(df: pd.DataFrame, equipe: Optional[str] = None) -> go.Figure:
    """Cria gráfico de status por operador."""
    if equipe:
        df_plot = df[df['EQUIPE'] == equipe]
    else:
        df_plot = df
    
    status_op = pd.crosstab(df_plot['OPERADOR'], df_plot['STATUS'])
    
    fig = go.Figure(data=[
        go.Bar(name=status, x=status_op.index, y=status_op[status])
        for status in status_op.columns
    ])
    
    fig.update_layout(
        barmode='stack',
        title=f"Status por Operador{' - ' + equipe if equipe else ''}",
        xaxis_title="Operador",
        yaxis_title="Quantidade",
        showlegend=True,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        paper_bgcolor='#252525',
        plot_bgcolor='#252525',
        font=dict(color='#ffd700'),
        height=400,
        margin=dict(t=50, l=0, r=0, b=0)
    )
    
    return fig

def criar_grafico_tempo(df: pd.DataFrame, equipe: Optional[str] = None) -> go.Figure:
    """Cria gráfico de tempo médio por operador."""
    if equipe:
        df_plot = df[df['EQUIPE'] == equipe]
    else:
        df_plot = df
        
    tempo_medio = df_plot.groupby('OPERADOR')['TEMPO_RESOLUCAO'].mean().sort_values(ascending=True)
    
    fig = go.Figure(data=[
        go.Bar(
            y=tempo_medio.index,
            x=tempo_medio.values,
            orientation='h',
            marker=dict(color='#ffd700')
        )
    ])
    
    fig.update_layout(
        title=f"Tempo Médio de Resolução por Operador{' - ' + equipe if equipe else ''}",
        xaxis_title="Dias",
        yaxis_title="Operador",
        showlegend=False,
        paper_bgcolor='#252525',
        plot_bgcolor='#252525',
        font=dict(color='#ffd700'),
        height=400,
        margin=dict(t=50, l=0, r=0, b=0)
    )
    
    return fig

def criar_grafico_motivos_pendencia(df: pd.DataFrame, equipe: Optional[str] = None) -> go.Figure:
    """Cria gráfico de motivos de pendência."""
    if equipe:
        df_plot = df[df['EQUIPE'] == equipe]
    else:
        df_plot = df
    
    df_pendentes = df_plot[df_plot['STATUS'] == 'PENDENTE']
    if 'MOTIVO_PENDENCIA' in df_pendentes.columns:
        motivos = df_pendentes['MOTIVO_PENDENCIA'].value_counts()
        
        fig = go.Figure(data=[
            go.Bar(
                x=motivos.values,
                y=motivos.index,
                orientation='h',
                marker=dict(color='#ffd700')
            )
        ])
        
        fig.update_layout(
            title=f"Motivos de Pendência{' - ' + equipe if equipe else ''}",
            xaxis_title="Quantidade",
            yaxis_title="Motivo",
            showlegend=False,
            paper_bgcolor='#252525',
            plot_bgcolor='#252525',
            font=dict(color='#ffd700'),
            height=400,
            margin=dict(t=50, l=0, r=0, b=0)
        )
        
        return fig
    return None

def validar_horario_trabalho(hora):
    """Valida se o horário está dentro do turno de trabalho (8h às 18h)."""
    return 8 <= hora < 18

def classificar_atividade(row):
    """Classifica a atividade com base no horário."""
    hora = row['DATA_CRIACAO'].hour
    if validar_horario_trabalho(hora):
        return 'Regular'
    return 'Fora do Turno'

def calcular_metricas(df):
    """Calcula métricas principais do dashboard."""
    total_quitadas = len(df[df['STATUS'] == 'QUITADO'])
    total_atividades = len(df)
    percentual_validade = (df['CLASSIFICACAO'] == 'Regular').mean() * 100
    
    return {
        'total_quitadas': total_quitadas,
        'percentual_validade': percentual_validade,
        'total_atividades': total_atividades
    }

def criar_tabela_interativa(df):
    """Cria uma tabela interativa com gradiente de cores."""
    df_styled = df.style.background_gradient(
        subset=['TEMPO_RESOLUCAO'],
        cmap='YlOrRd'
    ).format({
        'TEMPO_RESOLUCAO': '{:.1f}',
        'DATA_CRIACAO': lambda x: x.strftime('%d/%m/%Y %H:%M'),
        'DATA_RESOLUCAO': lambda x: x.strftime('%d/%m/%Y %H:%M') if pd.notnull(x) else ''
    })
    
    return df_styled

def criar_grafico_temporal(df):
    """Cria gráfico temporal de atividades."""
    df_temporal = df.groupby(df['DATA_CRIACAO'].dt.date).agg({
        'STATUS': 'count',
        'CLASSIFICACAO': lambda x: (x == 'Regular').mean() * 100
    }).reset_index()
    
    fig = go.Figure()
    
    fig.add_trace(go.Bar(
        x=df_temporal['DATA_CRIACAO'],
        y=df_temporal['STATUS'],
        name='Total de Atividades',
        marker_color='#FFD700'
    ))
    
    fig.add_trace(go.Scatter(
        x=df_temporal['DATA_CRIACAO'],
        y=df_temporal['CLASSIFICACAO'],
        name='% Atividades Regulares',
        yaxis='y2',
        line=dict(color='#000000')
    ))
    
    fig.update_layout(
        title='Evolução Temporal das Atividades',
        yaxis=dict(title='Quantidade de Atividades'),
        yaxis2=dict(
            title='% Atividades Regulares',
            overlaying='y',
            side='right'
        ),
        showlegend=True,
        height=400
    )
    
    return fig

def criar_analise_comparativa(df):
    """Cria análise comparativa detalhada entre equipes."""
    # Limpar dados inválidos
    df = df.copy()
    df['EQUIPE'] = df['EQUIPE'].fillna('SEM EQUIPE')
    df['OPERADOR'] = df['OPERADOR'].fillna('NÃO INFORMADO')
    
    # Filtrar apenas equipes válidas
    equipes_validas = ['DEMANDAS JULIO', 'LEANDRO ADRIANO']
    df = df[df['EQUIPE'].isin(equipes_validas)]
    
    # Criar dicionário para armazenar métricas por equipe
    metricas_equipes = {}
    
    for equipe in equipes_validas:
        df_equipe = df[df['EQUIPE'] == equipe]
        
        # Calcular métricas
        metricas = {
            'RESOLVIDOS': len(df_equipe[df_equipe['STATUS'].isin(['RESOLVIDO', 'QUITADO', 'APROVADO'])]),
            'PENDENTE_ATIVO': len(df_equipe[df_equipe['STATUS'] == 'PENDENTE ATIVO']),
            'PENDENTE_RECEPTIVO': len(df_equipe[df_equipe['STATUS'] == 'PENDENTE RECEPTIVO']),
            'PRIORIDADE': len(df_equipe[df_equipe['STATUS'] == 'PRIORIDADE']),
            'PRIORIDADE_TOTAL': len(df_equipe[df_equipe['STATUS'].str.contains('PRIORIDADE', na=False)]),
            'RECEPTIVO': len(df_equipe[df_equipe['STATUS'] == 'RECEPTIVO']),
            'QUITADO_CLIENTE': len(df_equipe[df_equipe['STATUS'] == 'QUITADO CLIENTE']),
            'QUITADO': len(df_equipe[df_equipe['STATUS'] == 'QUITADO']),
            'APROVADOS': len(df_equipe[df_equipe['STATUS'] == 'APROVADO'])
        }
        
        # Calcular soma das prioridades
        metricas['SOMA_PRIORIDADES'] = metricas['PRIORIDADE'] + metricas['PRIORIDADE_TOTAL']
        
        # Análise do dia
        total_demandas = len(df_equipe)
        if total_demandas > 0:
            taxa_resolucao = (metricas['RESOLVIDOS'] / total_demandas) * 100
            metricas['ANALISE_DIA'] = f"{taxa_resolucao:.1f}% resolvidos"
        else:
            metricas['ANALISE_DIA'] = "Sem demandas"
        
        metricas_equipes[equipe] = metricas
    
    return metricas_equipes

def mostrar_analise_comparativa(df):
    """Exibe a análise comparativa entre equipes."""
    st.title("Análise Comparativa entre Equipes")
    
    if df is None or df.empty:
        st.error("Dados não disponíveis para análise")
        return
    
    # Obter métricas
    metricas_equipes = criar_analise_comparativa(df)
    
    # Criar colunas para cada equipe
    col1, col2 = st.columns(2)
    
    # Lista de métricas para exibir
    metricas_exibir = [
        ('RESOLVIDOS', 'Resolvidos'),
        ('PENDENTE_ATIVO', 'Pendente Ativo'),
        ('PENDENTE_RECEPTIVO', 'Pendente Receptivo'),
        ('PRIORIDADE', 'Prioridade'),
        ('PRIORIDADE_TOTAL', 'Prioridade Total'),
        ('SOMA_PRIORIDADES', 'Soma das Prioridades'),
        ('RECEPTIVO', 'Receptivo'),
        ('QUITADO_CLIENTE', 'Quitado Cliente'),
        ('QUITADO', 'Quitado'),
        ('APROVADOS', 'Aprovados')
    ]
    
    # Exibir métricas para cada equipe
    for idx, (equipe, metricas) in enumerate(metricas_equipes.items()):
        col = col1 if idx == 0 else col2
        with col:
            st.subheader(equipe)
            
            # Card com análise do dia
            st.markdown(
                f"""
                <div class="metric-card" style="background-color: #FFD700; color: black; padding: 10px; border-radius: 5px; margin-bottom: 20px;">
                    <div style="font-size: 1.2em; font-weight: bold;">Análise do Dia</div>
                    <div style="font-size: 1.5em;">{metricas['ANALISE_DIA']}</div>
                </div>
                """,
                unsafe_allow_html=True
            )
            
            # Tabela de métricas
            for key, label in metricas_exibir:
                valor = metricas[key]
                st.markdown(
                    f"""
                    <div style="display: flex; justify-content: space-between; padding: 5px; border-bottom: 1px solid #ddd;">
                        <div>{label}</div>
                        <div style="font-weight: bold;">{valor}</div>
                    </div>
                    """,
                    unsafe_allow_html=True
                )
    
    # Gráfico comparativo
    st.subheader("Comparativo de Resolvidos por Equipe")
    dados_grafico = {
        'Equipe': [],
        'Status': [],
        'Quantidade': []
    }
    
    for equipe, metricas in metricas_equipes.items():
        for key, label in metricas_exibir:
            dados_grafico['Equipe'].append(equipe)
            dados_grafico['Status'].append(label)
            dados_grafico['Quantidade'].append(metricas[key])
    
    df_grafico = pd.DataFrame(dados_grafico)
    
    fig = px.bar(
        df_grafico,
        x='Status',
        y='Quantidade',
        color='Equipe',
        barmode='group',
        title='Comparativo de Status por Equipe',
        color_discrete_sequence=['#FFD700', '#000000']
    )
    
    fig.update_layout(
        xaxis_title="Status",
        yaxis_title="Quantidade",
        legend_title="Equipe",
        height=500
    )
    
    st.plotly_chart(fig, use_container_width=True)

def mostrar_relatorio_diario(df):
    """Exibe o relatório diário com design moderno e financeiro."""
    # Header com gradiente
    st.markdown("""
        <div class="header-container">
            <h1 class="header-title">Relatório Diário de Performance</h1>
        </div>
    """, unsafe_allow_html=True)

    # Data atual
    st.markdown(f"""
        <div style="text-align: right; color: var(--text-secondary); margin-bottom: 2rem;">
            {datetime.now().strftime('%d/%m/%Y %H:%M')}
        </div>
    """, unsafe_allow_html=True)

    # Grid de métricas principais
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("""
            <div class="metric-card animate-fade-in">
                <div>Atividades Quitadas</div>
                <div class="big-number">140</div>
                <div class="status-badge status-success">+15% vs ontem</div>
            </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("""
            <div class="metric-card animate-fade-in">
                <div>Taxa de Resolução</div>
                <div class="big-number">67.5%</div>
                <div class="status-badge status-warning">-2% vs meta</div>
            </div>
        """, unsafe_allow_html=True)
    
    with col3:
        st.markdown("""
            <div class="metric-card animate-fade-in">
                <div>Prioridades Pendentes</div>
                <div class="big-number">8</div>
                <div class="status-badge status-danger">Ação necessária</div>
            </div>
        """, unsafe_allow_html=True)

    # Tabs para diferentes visões
    tab1, tab2 = st.tabs(["Visão por Status", "Visão por Operador"])
    
    with tab1:
        st.markdown("""
            <div class="chart-container animate-fade-in">
                <table class="styled-table">
                    <thead>
                        <tr>
                            <th>Status</th>
                            <th>Quantidade</th>
                            <th>% do Total</th>
                        </tr>
                    </thead>
                    <tbody>
                        <tr>
                            <td>Resolvidos</td>
                            <td>140</td>
                            <td>45%</td>
                        </tr>
                        <tr>
                            <td>Pendente Ativo</td>
                            <td>311</td>
                            <td>35%</td>
                        </tr>
                        <tr>
                            <td>Pendente Receptivo</td>
                            <td>195</td>
                            <td>20%</td>
                        </tr>
                    </tbody>
                </table>
            </div>
        """, unsafe_allow_html=True)
    
    with tab2:
        st.markdown("""
            <div class="chart-container animate-fade-in">
                <table class="styled-table">
                    <thead>
                        <tr>
                            <th>Operador</th>
                            <th>Resolvidos</th>
                            <th>Performance</th>
                        </tr>
                    </thead>
                    <tbody>
                        <tr>
                            <td>Fabiana</td>
                            <td>22</td>
                            <td><div class="status-badge status-success">Acima da meta</div></td>
                        </tr>
                        <tr>
                            <td>Amanda Santana</td>
                            <td>19</td>
                            <td><div class="status-badge status-success">Acima da meta</div></td>
                        </tr>
                        <tr>
                            <td>Itaynnara</td>
                            <td>13</td>
                            <td><div class="status-badge status-warning">Na meta</div></td>
                        </tr>
                    </tbody>
                </table>
            </div>
        """, unsafe_allow_html=True)

    # Seção de alertas e prioridades
    st.markdown("""
        <div class="metric-card animate-fade-in" style="margin-top: 2rem;">
            <h3 style="color: var(--text-primary); margin-bottom: 1rem;">Alertas e Prioridades</h3>
            <div style="color: var(--danger-color); margin-bottom: 0.5rem;">
                ⚠️ 8 demandas prioritárias pendentes
            </div>
            <div style="color: var(--warning-color); margin-bottom: 0.5rem;">
                ⚠️ 195 demandas receptivas aguardando atendimento
            </div>
            <div style="color: var(--success-color);">
                ✓ Taxa de resolução dentro da meta para o período
            </div>
        </div>
    """, unsafe_allow_html=True)

def main():
    try:
        # Atualizar status do monitor
        monitor = st.session_state.app_monitor
        monitor.status = DataStatus.PROCESSING
        
        # Sidebar para debug e monitoramento
        with st.sidebar:
            if DEBUG:
                st.header("🔧 Debug e Monitoramento")
                
                # Status da aplicação
                st.subheader("Status do Sistema")
                status = monitor.get_status()
                st.json(status)
                
                # Avisos do sistema
                warnings = monitor.check_system()
                if warnings:
                    st.warning("⚠️ Avisos do Sistema:")
                    for warning in warnings:
                        st.warning(warning)
                
                # Painel de debug
                if st.checkbox("Mostrar Painel de Debug"):
                    st.subheader("Mensagens de Debug")
                    for msg in st.session_state.debug_messages:
                        st.text(msg)
                    if st.button("Limpar Log"):
                        st.session_state.debug_messages = []
                
                # Último erro
                if status['last_error']:
                    st.error("Último Erro:")
                    st.code(status['last_error']['traceback'])
        
        # Interface principal
        st.title("Dashboard de Equipes 📊")
        
        try:
            # Opções de dados
            st.header("Dados")
            data_source = st.radio(
                "Fonte dos dados:",
                ["Arquivo Padrão", "Upload de Arquivo"],
                help="Escolha a fonte dos dados para análise"
            )
            
            if data_source == "Upload de Arquivo":
                uploaded_file = st.file_uploader(
                    "Escolha um arquivo Excel ou CSV",
                    type=["xlsx", "csv"],
                    help="Selecione um arquivo Excel (.xlsx) ou CSV"
                )
                
                if uploaded_file is not None:
                    add_debug_message(f"Arquivo carregado: {uploaded_file.name}")
                    try:
                        # Salvar arquivo temporário
                        with tempfile.NamedTemporaryFile(delete=False, suffix=Path(uploaded_file.name).suffix) as tmp_file:
                            tmp_file.write(uploaded_file.getvalue())
                            temp_path = tmp_file.name
                        
                        # Processar dados
                        df = carregar_dados(temp_path)
                        if df is not None:
                            st.session_state.df = df
                            add_debug_message("Dados processados com sucesso")
                            
                            # Mostrar informações do DataFrame
                            st.subheader("Informações do DataFrame")
                            st.write("Colunas disponíveis:", df.columns.tolist())
                            st.write("Primeiras linhas:")
                            st.dataframe(df.head())
                        else:
                            st.error("Erro ao processar o arquivo")
                            add_debug_message("Erro no processamento do arquivo")
                        
                        # Limpar arquivo temporário
                        os.unlink(temp_path)
                        
                    except Exception as e:
                        monitor.log_error(e, "Processamento de arquivo")
                        st.error(f"Erro ao processar arquivo: {str(e)}")
            else:
                # Usar arquivo padrão
                try:
                    df = carregar_dados("dados_equipes.xlsx")
                    if df is not None:
                        st.session_state.df = df
                        add_debug_message("Dados padrão carregados com sucesso")
                        
                        # Mostrar informações do DataFrame
                        st.subheader("Informações do DataFrame")
                        st.write("Colunas disponíveis:", df.columns.tolist())
                        st.write("Primeiras linhas:")
                        st.dataframe(df.head())
                    else:
                        st.error("Erro ao carregar arquivo padrão")
                        add_debug_message("Erro ao carregar arquivo padrão")
                except Exception as e:
                    monitor.log_error(e, "Carregamento de arquivo padrão")
                    st.error(f"Erro ao carregar arquivo padrão: {str(e)}")
            
            # Se temos dados, mostrar dashboard
            if 'df' in st.session_state and st.session_state.df is not None:
                df = st.session_state.df
                
                # Aplicar filtros
                df, filtros = mostrar_filtros(df)
                
                if filtros:
                    st.info(f"Filtros aplicados: {filtros}")
                
                # Mostrar métricas e gráficos
                mostrar_metricas(df)
                mostrar_graficos(df)
                mostrar_relatorios_detalhados(df)
                
                # Opção para exportar dados
                if st.button("📥 Exportar Dados Processados"):
                    try:
                        # Criar arquivo Excel
                        output = BytesIO()
                        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                            df.to_excel(writer, index=False, sheet_name='Dados')
                        
                        # Oferecer para download
                        st.download_button(
                            label="📥 Baixar Excel",
                            data=output.getvalue(),
                            file_name="dados_processados.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        )
                        add_debug_message("Dados exportados com sucesso")
                    except Exception as e:
                        monitor.log_error(e, "Exportação de dados")
                        st.error(f"Erro ao exportar dados: {str(e)}")
        
        except Exception as e:
            monitor.log_error(e, "Interface principal")
            st.error(f"Erro na interface principal: {str(e)}")
            if DEBUG:
                st.error(f"""
                    **Erro não tratado**
                    ```python
                    {traceback.format_exc()}
                    ```
                """)
        
        finally:
            # Atualizar status final
            monitor.status = DataStatus.COMPLETED
    
    except Exception as e:
        if 'app_monitor' in st.session_state:
            st.session_state.app_monitor.log_error(e, "Main")
        st.error(f"Erro crítico na aplicação: {str(e)}")
        if DEBUG:
            st.error(f"""
                **Erro crítico**
                ```python
                {traceback.format_exc()}
                ```
            """)

if __name__ == "__main__":
    # Inicializar tempo de início
    if 'start_time' not in st.session_state:
        st.session_state.start_time = time.time()
    
    # Executar aplicação
    main()

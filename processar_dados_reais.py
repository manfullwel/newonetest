"""Script para processar os dados reais das planilhas."""
import pandas as pd
import os
from datetime import datetime

def limpar_texto(texto):
    """Remove caracteres especiais do texto."""
    if not isinstance(texto, str):
        return texto
    return texto.encode('ascii', 'ignore').decode('ascii')

def carregar_csv(arquivo):
    """Carrega um arquivo CSV com a codificação correta."""
    df = pd.read_csv(arquivo, encoding='latin1')
    # Renomear colunas removendo caracteres especiais
    df.columns = [limpar_texto(col) for col in df.columns]
    return df

def processar_dados():
    """Processa e consolida os dados reais."""
    # Diretório com os arquivos
    dir_dados = 'avaliar'
    
    # Carregar arquivos CSV
    df_julio = carregar_csv(os.path.join(dir_dados, '_DEMANDAS DE JANEIRO_2025 - DEMANDAS JULIO.csv'))
    df_leandro = carregar_csv(os.path.join(dir_dados, '_DEMANDAS DE JANEIRO_2025 - DEMANDA LEANDROADRIANO.csv'))
    df_quitados = carregar_csv(os.path.join(dir_dados, '_DEMANDAS DE JANEIRO_2025 - QUITADOS.csv'))
    df_aprovados = carregar_csv(os.path.join(dir_dados, '_DEMANDAS DE JANEIRO_2025 - APROVADO E QUITADO CLIENTE.csv'))
    
    # Consolidar dados
    df_consolidado = pd.concat([
        df_julio.assign(EQUIPE='DEMANDAS JULIO'),
        df_leandro.assign(EQUIPE='DEMANDAS LEANDRO/ADRIANO')
    ], ignore_index=True)
    
    # Adicionar informações de status
    df_consolidado['STATUS'] = 'PENDENTE'  # Status padrão
    
    # Marcar quitados e aprovados usando CTT
    for idx, row in df_consolidado.iterrows():
        ctt = str(row['CTT'])
        
        # Verificar nos quitados
        if any(df_quitados['CTT'].astype(str) == ctt):
            df_consolidado.at[idx, 'STATUS'] = 'QUITADO'
        
        # Verificar nos aprovados
        elif any(df_aprovados['CTT'].astype(str) == ctt):
            df_consolidado.at[idx, 'STATUS'] = 'APROVADO'
    
    # Adicionar data de criação (usando o primeiro dia do mês)
    df_consolidado['DATA_CRIACAO'] = '2025-01-01'
    
    # Encontrar a coluna de resolução
    resolucao_col = [col for col in df_consolidado.columns if 'RESOLU' in col][0]
    
    # Adicionar data de resolução
    df_consolidado['DATA_RESOLUCAO'] = df_consolidado[resolucao_col]
    
    # Converter datas para o formato correto
    def converter_data(data):
        if pd.isna(data):
            return None
        try:
            # Tentar diferentes formatos de data
            for fmt in ['%d/%m/%Y', '%Y-%m-%d']:
                try:
                    return pd.to_datetime(data, format=fmt)
                except:
                    continue
            return pd.to_datetime(data)
        except:
            return None

    # Converter datas
    df_consolidado['DATA_CRIACAO'] = pd.to_datetime(df_consolidado['DATA_CRIACAO'])
    df_consolidado['DATA_RESOLUCAO'] = df_consolidado['DATA_RESOLUCAO'].apply(converter_data)
    
    # Calcular tempo de resolução
    df_consolidado['TEMPO_RESOLUCAO'] = None
    mask_resolvido = df_consolidado['STATUS'].isin(['QUITADO', 'APROVADO'])
    df_consolidado.loc[mask_resolvido, 'TEMPO_RESOLUCAO'] = (
        df_consolidado.loc[mask_resolvido, 'DATA_RESOLUCAO'] - 
        df_consolidado.loc[mask_resolvido, 'DATA_CRIACAO']
    ).dt.total_seconds() / (24 * 60 * 60)  # Converter para dias
    
    # Adicionar prioridade baseada em regras de negócio
    df_consolidado['PRIORIDADE'] = 'MEDIA'  # Prioridade padrão
    df_consolidado.loc[df_consolidado['TEMPO_RESOLUCAO'] > 5, 'PRIORIDADE'] = 'ALTA'
    df_consolidado.loc[df_consolidado['TEMPO_RESOLUCAO'] <= 2, 'PRIORIDADE'] = 'BAIXA'
    
    # Adicionar motivo de pendência para demonstração
    df_consolidado['MOTIVO_PENDENCIA'] = None
    df_consolidado.loc[df_consolidado['STATUS'] == 'PENDENTE', 'MOTIVO_PENDENCIA'] = 'AGUARDANDO DOCUMENTAÇÃO'
    
    # Encontrar e renomear as colunas de responsável
    responsavel_col = [col for col in df_consolidado.columns if 'RESPONS' in col][0]
    diretor_col = 'DIRETOR'
    
    # Renomear colunas para o padrão esperado
    mapeamento_colunas = {
        resolucao_col: 'DATA_RESOLUCAO',
        responsavel_col: 'OPERADOR',
        diretor_col: 'RESPONSAVEL'
    }
    df_consolidado = df_consolidado.rename(columns=mapeamento_colunas)
    
    # Salvar arquivo consolidado
    output_dir = 'output_20250131_0218'
    os.makedirs(output_dir, exist_ok=True)
    df_consolidado.to_excel(os.path.join(output_dir, 'dados_PROCESSADO_2025.xlsx'), index=False)
    print("Dados reais processados com sucesso!")

if __name__ == "__main__":
    processar_dados()

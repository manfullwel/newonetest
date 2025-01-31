import pandas as pd
import numpy as np
from datetime import datetime

def format_currency(value):
    """Padroniza valores monetários para formato R$ #.###,##"""
    if pd.isna(value):
        return value
    try:
        # Remove R$, espaços e converte para float
        if isinstance(value, str):
            value = value.replace('R$', '').replace('.', '').replace(',', '.').strip()
        return f"R$ {float(value):,.2f}".replace(',', '_').replace('.', ',').replace('_', '.')
    except:
        return value

def format_date(value):
    """Padroniza datas para formato YYYY-MM-DD"""
    if pd.isna(value):
        return value
    try:
        if isinstance(value, str):
            # Tenta diferentes formatos de data comuns
            for fmt in ['%d/%m/%Y', '%Y-%m-%d', '%d-%m-%Y']:
                try:
                    return pd.to_datetime(value, format=fmt).strftime('%Y-%m-%d')
                except:
                    continue
        return pd.to_datetime(value).strftime('%Y-%m-%d')
    except:
        return value

def clean_text(value):
    """Padroniza textos para maiúsculas sem espaços extras"""
    if pd.isna(value):
        return value
    try:
        return str(value).strip().upper()
    except:
        return value

def format_percentage(value):
    """Padroniza percentuais para ##,##%"""
    if pd.isna(value):
        return value
    try:
        if isinstance(value, str):
            value = value.replace('%', '').replace(',', '.').strip()
        return f"{float(value):.2f}%".replace('.', ',')
    except:
        return value

def clean_worksheet(df, sheet_name):
    """Limpa e padroniza uma aba da planilha"""
    print(f"\nLimpando aba: {sheet_name}")
    
    # Remove duplicatas
    n_duplicates = df.duplicated().sum()
    df = df.drop_duplicates()
    print(f"Removidas {n_duplicates} linhas duplicadas")
    
    # Remove colunas vazias ou com mais de 90% de dados ausentes
    empty_cols = []
    for col in df.columns:
        missing_pct = df[col].isna().mean()
        if missing_pct > 0.9 or df[col].nunique() == 0:
            empty_cols.append(col)
    df = df.drop(columns=empty_cols)
    print(f"Removidas {len(empty_cols)} colunas vazias ou com muitos dados ausentes")
    
    # Renomeia colunas sem nome
    rename_cols = {}
    for col in df.columns:
        if 'Unnamed' in str(col):
            new_name = f'COLUNA_{str(col).split(":")[-1]}'
            rename_cols[col] = new_name
    df = df.rename(columns=rename_cols)
    
    # Aplica formatação específica por tipo de coluna
    for col in df.columns:
        col_lower = col.lower()
        if 'data' in col_lower:
            df[col] = df[col].apply(format_date)
        elif 'valor' in col_lower or 'saldo' in col_lower or 'desconto' in col_lower:
            df[col] = df[col].apply(format_currency)
        elif '%' in col_lower or 'percentual' in col_lower:
            df[col] = df[col].apply(format_percentage)
        else:
            df[col] = df[col].apply(clean_text)
    
    return df

def main():
    # Carrega o arquivo Excel
    excel_file = '_DEMANDAS_2025.xlsx'
    print(f"Processando arquivo: {excel_file}")
    
    # Lista de abas principais
    main_sheets = ['DEMANDAS JULIO', 'DEMANDA LEANDROADRIANO', 'QUITADOS']
    
    # Cria um novo arquivo Excel
    with pd.ExcelWriter(f'DEMANDAS_2025_LIMPO_{datetime.now().strftime("%Y%m%d")}.xlsx') as writer:
        # Processa cada aba principal
        for sheet_name in main_sheets:
            try:
                # Lê a aba
                df = pd.read_excel(excel_file, sheet_name=sheet_name)
                
                # Limpa e padroniza os dados
                df_clean = clean_worksheet(df, sheet_name)
                
                # Salva a aba limpa
                df_clean.to_excel(writer, sheet_name=sheet_name, index=False)
                
                print(f"Aba {sheet_name} processada com sucesso")
            except Exception as e:
                print(f"Erro ao processar aba {sheet_name}: {str(e)}")
    
    print("\nProcessamento concluído!")

if __name__ == "__main__":
    main()

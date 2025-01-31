import pandas as pd
import numpy as np
from datetime import datetime
import matplotlib.pyplot as plt
import seaborn as sns

def analyze_monetary_column(df, col_name):
    """Analisa coluna monetária, identificando outliers e padrões"""
    values = []
    for val in df[col_name]:
        try:
            if isinstance(val, str):
                # Remove R$, pontos e converte vírgula para ponto
                clean_val = float(val.replace('R$', '').replace('.', '').replace(',', '.').strip())
            else:
                clean_val = float(val)
            values.append(clean_val)
        except:
            continue
    
    if not values:
        return None
    
    values = np.array(values)
    q1 = np.percentile(values, 25)
    q3 = np.percentile(values, 75)
    iqr = q3 - q1
    lower_bound = q1 - 1.5 * iqr
    upper_bound = q3 + 1.5 * iqr
    
    outliers = values[(values < lower_bound) | (values > upper_bound)]
    
    return {
        'min': values.min(),
        'max': values.max(),
        'mean': values.mean(),
        'median': np.median(values),
        'outliers': len(outliers),
        'outliers_values': outliers.tolist()[:5]  # Mostra até 5 outliers
    }

def analyze_patterns(df, sheet_name):
    """Analisa padrões e discrepâncias nos dados"""
    print(f"\n{'='*50}")
    print(f"Análise de Padrões: {sheet_name}")
    print(f"{'='*50}")
    
    # 1. Análise por Diretor
    if 'DIRETOR' in df.columns:
        print("\nDistribuição por Diretor:")
        diretor_counts = df['DIRETOR'].value_counts()
        print(diretor_counts)
    
    # 2. Análise por Banco
    if 'BANCO' in df.columns:
        print("\nDistribuição por Banco:")
        banco_counts = df['BANCO'].value_counts().head()
        print(banco_counts)
    
    # 3. Análise de valores monetários
    monetary_columns = [col for col in df.columns if any(term in col.upper() for term in ['VALOR', 'SALDO', 'DESCONTO'])]
    for col in monetary_columns:
        print(f"\nAnálise de {col}:")
        stats = analyze_monetary_column(df, col)
        if stats:
            print(f"Mínimo: R$ {stats['min']:,.2f}")
            print(f"Máximo: R$ {stats['max']:,.2f}")
            print(f"Média: R$ {stats['mean']:,.2f}")
            print(f"Mediana: R$ {stats['median']:,.2f}")
            print(f"Número de outliers: {stats['outliers']}")
            if stats['outliers'] > 0:
                print(f"Exemplos de outliers: {[f'R$ {x:,.2f}' for x in stats['outliers_values']]}")
    
    # 4. Análise temporal
    date_columns = [col for col in df.columns if 'DATA' in col.upper()]
    for col in date_columns:
        try:
            df[col] = pd.to_datetime(df[col])
            print(f"\nAnálise temporal de {col}:")
            print(f"Período: {df[col].min()} até {df[col].max()}")
            print("Distribuição por mês:")
            monthly_counts = df[col].dt.to_period('M').value_counts().sort_index()
            print(monthly_counts)
        except:
            continue
    
    # 5. Valores ausentes por coluna
    print("\nPorcentagem de valores ausentes por coluna:")
    missing_pct = (df.isna().sum() / len(df) * 100).sort_values(ascending=False)
    print(missing_pct[missing_pct > 0])

def main():
    # Carrega o arquivo Excel limpo
    file_pattern = 'DEMANDAS_2025_LIMPO_'
    excel_files = [f for f in os.listdir() if f.startswith(file_pattern)]
    if not excel_files:
        print("Arquivo limpo não encontrado!")
        return
    
    excel_file = max(excel_files)  # Pega o arquivo mais recente
    print(f"Analisando arquivo: {excel_file}")
    
    # Lista de abas principais
    main_sheets = ['DEMANDAS JULIO', 'DEMANDA LEANDROADRIANO', 'QUITADOS']
    
    # Analisa cada aba
    for sheet_name in main_sheets:
        try:
            df = pd.read_excel(excel_file, sheet_name=sheet_name)
            analyze_patterns(df, sheet_name)
        except Exception as e:
            print(f"Erro ao analisar aba {sheet_name}: {str(e)}")
    
    print("\nAnálise concluída!")

if __name__ == "__main__":
    import os
    main()

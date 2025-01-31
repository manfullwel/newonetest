import pandas as pd
import numpy as np

def analyze_worksheet(df, sheet_name):
    print(f"\n{'='*50}")
    print(f"Análise da Aba: {sheet_name}")
    print(f"{'='*50}")
    
    # Informações básicas
    print(f"\nDimensões: {df.shape[0]} linhas x {df.shape[1]} colunas")
    
    # Análise das colunas
    print("\nColunas e Tipos de Dados:")
    for col in df.columns:
        missing = df[col].isna().sum()
        missing_pct = (missing / len(df)) * 100
        unique_values = df[col].nunique()
        print(f"\n- {col}")
        print(f"  Tipo: {df[col].dtype}")
        print(f"  Valores únicos: {unique_values}")
        print(f"  Valores ausentes: {missing} ({missing_pct:.1f}%)")
        
        # Amostra de valores únicos (limitado a 5)
        if unique_values > 0:
            sample_values = df[col].dropna().unique()[:5]
            print(f"  Exemplos: {sample_values}")
    
    # Verificar duplicatas
    duplicates = df.duplicated().sum()
    print(f"\nLinhas duplicadas: {duplicates}")

# Carregar o arquivo Excel
excel_file = '_DEMANDAS_2025.xlsx'
print(f"Analisando o arquivo: {excel_file}\n")

# Ler todas as abas
xl = pd.ExcelFile(excel_file)
print(f"Abas encontradas: {xl.sheet_names}")

# Analisar cada aba
for sheet_name in xl.sheet_names:
    df = pd.read_excel(excel_file, sheet_name=sheet_name)
    analyze_worksheet(df, sheet_name)

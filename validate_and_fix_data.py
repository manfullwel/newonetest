import pandas as pd
import numpy as np
from datetime import datetime
import re
import json
from pathlib import Path

class DataValidator:
    def __init__(self):
        self.validation_rules = {
            'RESPONSAVEL': {
                'required': True,
                'default': 'NÃO DEFINIDO',
                'type': 'text'
            },
            'SITUACAO': {
                'required': True,
                'default': 'PENDENTE',
                'valid_values': ['RESOLVIDO', 'PENDENTE', 'ANÁLISE', 'PRIORIDADE', 'PRIORIDADE TOTAL']
            },
            'DIRETOR': {
                'required': True,
                'valid_values': ['JULIO', 'LEANDRO', 'ADRIANO', 'ANTUNES', 'FECHADO', 'REVERSÃO']
            },
            'BANCO': {
                'required': True,
                'valid_values': ['BV FINANCEIRA', 'BRADESCO', 'SANTANDER', 'PANAMERICANO', 'OMNI', 'ITAÚ', 'RENNER']
            }
        }
        
        self.validation_report = {
            'missing_data': {},
            'invalid_values': {},
            'temporal_anomalies': {},
            'corrections_made': {}
        }

    def validate_date(self, value):
        """Valida e corrige datas"""
        if pd.isna(value):
            return None
        
        try:
            if isinstance(value, str):
                # Tenta diferentes formatos de data
                for fmt in ['%Y-%m-%d', '%d/%m/%Y', '%d-%m-%Y']:
                    try:
                        return pd.to_datetime(value, format=fmt)
                    except:
                        continue
            return pd.to_datetime(value)
        except:
            return None

    def validate_monetary(self, value):
        """Valida e corrige valores monetários"""
        if pd.isna(value):
            return None
        
        try:
            if isinstance(value, str):
                # Remove R$, pontos e converte vírgula para ponto
                clean_val = value.replace('R$', '').replace('.', '').replace(',', '.').strip()
                return float(clean_val)
            return float(value)
        except:
            return None

    def analyze_temporal_distribution(self, df, date_column):
        """Analisa distribuição temporal dos dados"""
        if date_column not in df.columns:
            return
        
        df[date_column] = pd.to_datetime(df[date_column])
        monthly_counts = df[date_column].dt.to_period('M').value_counts()
        
        # Verifica concentração em Janeiro 2025
        jan_2025_count = monthly_counts.get(pd.Period('2025-01'), 0)
        total_count = len(df)
        jan_2025_pct = (jan_2025_count / total_count) * 100
        
        if jan_2025_pct > 80:
            self.validation_report['temporal_anomalies'][date_column] = {
                'warning': 'Alta concentração em Janeiro 2025',
                'percentage': f'{jan_2025_pct:.2f}%',
                'count': int(jan_2025_count),
                'total_records': total_count
            }

    def validate_and_fix(self, df, sheet_name):
        """Valida e corrige dados de acordo com as regras definidas"""
        print(f"\nValidando aba: {sheet_name}")
        
        # Copia do DataFrame original para comparação
        df_original = df.copy()
        
        # 1. Preenchimento de dados ausentes em campos críticos
        for col, rules in self.validation_rules.items():
            if col in df.columns:
                missing_before = df[col].isna().sum()
                
                if rules.get('required', False):
                    if 'valid_values' in rules:
                        df[col] = df[col].fillna(rules['valid_values'][0])
                    else:
                        df[col] = df[col].fillna(rules.get('default', 'NÃO DEFINIDO'))
                
                missing_after = df[col].isna().sum()
                if missing_before > 0:
                    self.validation_report['missing_data'][f"{sheet_name}_{col}"] = {
                        'before': int(missing_before),
                        'after': int(missing_after),
                        'fixed': int(missing_before - missing_after)
                    }
        
        # 2. Validação de valores em campos com lista definida
        for col, rules in self.validation_rules.items():
            if col in df.columns and 'valid_values' in rules:
                invalid_mask = ~df[col].isin(rules['valid_values'])
                invalid_count = invalid_mask.sum()
                
                if invalid_count > 0:
                    self.validation_report['invalid_values'][f"{sheet_name}_{col}"] = {
                        'count': int(invalid_count),
                        'examples': df[col][invalid_mask].unique().tolist()[:5]
                    }
                    # Corrige valores inválidos
                    df.loc[invalid_mask, col] = rules['valid_values'][0]
        
        # 3. Análise temporal
        date_columns = [col for col in df.columns if 'DATA' in col.upper()]
        for col in date_columns:
            self.analyze_temporal_distribution(df, col)
        
        # 4. Validação de valores monetários
        monetary_columns = [col for col in df.columns if any(term in col.upper() for term in ['VALOR', 'SALDO', 'DESCONTO'])]
        for col in monetary_columns:
            if col in df.columns:
                df[col] = df[col].apply(self.validate_monetary)
        
        # 5. Registro de alterações feitas
        changes = {}
        for col in df.columns:
            if not (df[col] == df_original[col]).all():
                changes[col] = {
                    'rows_changed': int((df[col] != df_original[col]).sum()),
                    'total_rows': len(df)
                }
        
        if changes:
            self.validation_report['corrections_made'][sheet_name] = changes
        
        return df

def main():
    # Carrega o arquivo Excel limpo mais recente
    file_pattern = 'DEMANDAS_2025_LIMPO_'
    excel_files = [f for f in Path().glob(f'{file_pattern}*.xlsx')]
    if not excel_files:
        print("Arquivo limpo não encontrado!")
        return
    
    input_file = max(excel_files)
    output_file = f'DEMANDAS_2025_VALIDADO_{datetime.now().strftime("%Y%m%d")}.xlsx'
    report_file = f'validation_report_{datetime.now().strftime("%Y%m%d")}.json'
    
    print(f"Processando arquivo: {input_file}")
    
    # Inicializa o validador
    validator = DataValidator()
    
    # Lista de abas principais
    main_sheets = ['DEMANDAS JULIO', 'DEMANDA LEANDROADRIANO', 'QUITADOS']
    
    # Processa cada aba
    with pd.ExcelWriter(output_file) as writer:
        for sheet_name in main_sheets:
            try:
                df = pd.read_excel(input_file, sheet_name=sheet_name)
                df_validated = validator.validate_and_fix(df, sheet_name)
                df_validated.to_excel(writer, sheet_name=sheet_name, index=False)
                print(f"Aba {sheet_name} processada com sucesso")
            except Exception as e:
                print(f"Erro ao processar aba {sheet_name}: {str(e)}")
    
    # Salva o relatório de validação
    with open(report_file, 'w', encoding='utf-8') as f:
        json.dump(validator.validation_report, f, indent=2, ensure_ascii=False)
    
    print(f"\nProcessamento concluído!")
    print(f"Arquivo validado salvo como: {output_file}")
    print(f"Relatório de validação salvo como: {report_file}")
    
    # Exibe resumo das correções
    print("\nResumo das correções:")
    print("\n1. Dados ausentes corrigidos:")
    for key, value in validator.validation_report['missing_data'].items():
        print(f"{key}: {value['fixed']} correções")
    
    print("\n2. Valores inválidos encontrados:")
    for key, value in validator.validation_report['invalid_values'].items():
        print(f"{key}: {value['count']} valores inválidos")
    
    print("\n3. Anomalias temporais:")
    for key, value in validator.validation_report['temporal_anomalies'].items():
        print(f"{key}: {value['warning']} ({value['percentage']})")

if __name__ == "__main__":
    main()

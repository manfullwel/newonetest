import pandas as pd
import numpy as np
from datetime import datetime
import json
import sys
import os

# Configurar encoding para UTF-8
sys.stdout.reconfigure(encoding='utf-8')

def validar_campos(row, regras_validacao):
    """Valida campos de acordo com regras específicas mais flexíveis"""
    erros = []
    
    # Validação de Data - Único campo realmente crítico
    if pd.isna(row.get('DATA')):
        erros.append("Data não definida")
    else:
        try:
            data = pd.to_datetime(row.get('DATA'))
            # Verifica se a data está em um intervalo razoável (2020-2026)
            if not (pd.Timestamp('2020-01-01') <= data <= pd.Timestamp('2026-12-31')):
                erros.append("Data fora do intervalo esperado")
        except:
            erros.append("Data inválida")
    
    # Para outros campos, apenas registra se estiverem vazios
    campos_opcionais = {
        'RESPONSAVEL': "Responsável não definido",
        'SITUACAO': "Situação não definida",
        'BANCO': "Banco não definido",
        'DIRETOR': "Diretor não definido"
    }
    
    for campo, mensagem in campos_opcionais.items():
        if pd.isna(row.get(campo)):
            erros.append(f"Aviso: {mensagem}")
        elif campo == 'BANCO' and row.get(campo) not in regras_validacao['bancos_validos']:
            if row.get(campo) not in regras_validacao['novos_bancos']:
                regras_validacao['novos_bancos'].add(row.get(campo))
        elif campo == 'DIRETOR' and row.get(campo) not in regras_validacao['diretores_validos']:
            if row.get(campo) not in regras_validacao['novos_diretores']:
                regras_validacao['novos_diretores'].add(row.get(campo))
    
    return "; ".join(erros) if erros else "OK"

def analisar_concentracao_temporal(df, coluna_data):
    """Analisa a distribuição temporal dos registros"""
    if coluna_data not in df.columns:
        return None
    
    df[coluna_data] = pd.to_datetime(df[coluna_data])
    distribuicao = df[coluna_data].dt.to_period('M').value_counts()
    
    # Converter períodos para strings no formato YYYY-MM
    distribuicao_dict = {str(k): int(v) for k, v in distribuicao.items()}
    
    analise = {
        'distribuicao_mensal': distribuicao_dict,
        'registros_por_mes': len(df[df[coluna_data].dt.month == 1]),
        'total_registros': len(df),
        'primeiro_registro': df[coluna_data].min().strftime('%Y-%m-%d'),
        'ultimo_registro': df[coluna_data].max().strftime('%Y-%m-%d')
    }
    return analise

def gerar_relatorio_validacao(df, sheet_name, regras_validacao):
    """Gera relatório detalhado da validação"""
    def series_to_dict(series):
        return {str(k): int(v) if isinstance(v, (np.integer, int)) else str(v) 
                for k, v in series.items()}
    
    return {
        'total_registros': int(len(df)),
        'campos_vazios': series_to_dict(df.isna().sum()),
        'registros_problematicos': int(len(df[df['STATUS_VALIDACAO'] != 'OK'])),
        'tipos_problemas': series_to_dict(df[df['STATUS_VALIDACAO'] != 'OK']['STATUS_VALIDACAO'].value_counts()),
        'distribuicao_situacao': series_to_dict(df['SITUACAO'].value_counts()) if 'SITUACAO' in df.columns else {},
        'distribuicao_responsavel': series_to_dict(df['RESPONSAVEL'].value_counts()) if 'RESPONSAVEL' in df.columns else {},
        'distribuicao_banco': series_to_dict(df['BANCO'].value_counts()) if 'BANCO' in df.columns else {},
        'novos_bancos': list(regras_validacao['novos_bancos']),
        'novos_diretores': list(regras_validacao['novos_diretores'])
    }

def main():
    # Configurações de validação ainda mais flexíveis
    regras_validacao = {
        'bancos_validos': {
            'BV FINANCEIRA', 'BRADESCO', 'SANTANDER', 'PANAMERICANO', 
            'OMNI', 'ITAÚ', 'RENNER', 'GMAC', 'SAFRA', 'VOLKSWAGEN',
            'TOYOTA', 'HONDA', 'C6 BANK', 'PORTO SEGURO', 'RCI',
            'HYUNDAI', 'PSA', 'MONEYPLUS'
        },
        'diretores_validos': {
            'JULIO', 'LEANDRO', 'ADRIANO', 'ANTUNES', 'FECHADO', 'REVERSÃO',
            'PENDENTE', 'EM ANALISE', 'FINALIZADO'
        },
        'campos_criticos': [
            'DATA'  # Apenas data é realmente crítica
        ],
        'novos_bancos': set(),
        'novos_diretores': set()
    }
    
    # Carregar arquivo mais recente
    file_pattern = 'DEMANDAS_2025_LIMPO_'
    excel_files = [f for f in os.listdir() if f.startswith(file_pattern)]
    if not excel_files:
        print("Arquivo limpo não encontrado!")
        return
    
    input_file = max(excel_files)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    output_file = f'DEMANDAS_2025_VALIDADO_FLEXIVEL_{timestamp}.xlsx'
    report_file = f'relatorio_validacao_flexivel_{timestamp}.json'
    
    print(f"Processando arquivo: {input_file}")
    
    # Relatório completo
    relatorio_completo = {
        'meta': {
            'arquivo_entrada': input_file,
            'arquivo_saida': output_file,
            'data_processamento': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'regras_validacao': {
                'bancos_validos': list(regras_validacao['bancos_validos']),
                'diretores_validos': list(regras_validacao['diretores_validos']),
                'campos_criticos': regras_validacao['campos_criticos']
            }
        },
        'resultados': {}
    }
    
    # Processar cada aba
    with pd.ExcelWriter(output_file) as writer:
        for sheet_name in ['DEMANDAS JULIO', 'DEMANDA LEANDROADRIANO', 'QUITADOS']:
            try:
                print(f"\nProcessando aba: {sheet_name}")
                
                # Carregar dados
                df = pd.read_excel(input_file, sheet_name=sheet_name)
                
                # Limpar e padronizar dados
                for col in df.columns:
                    if df[col].dtype == 'object':
                        df[col] = df[col].astype(str).str.strip().replace('nan', np.nan)
                
                # Validar registros
                df['STATUS_VALIDACAO'] = df.apply(lambda row: validar_campos(row, regras_validacao), axis=1)
                
                # Classificar problemas
                df['TIPO_PROBLEMA'] = df['STATUS_VALIDACAO'].apply(
                    lambda x: 'Crítico' if 'Data' in x else ('Aviso' if x != 'OK' else 'OK')
                )
                
                # Análise temporal
                analise_temporal = analisar_concentracao_temporal(df, 'DATA')
                
                # Gerar relatório da aba
                relatorio_aba = gerar_relatorio_validacao(df, sheet_name, regras_validacao)
                if analise_temporal:
                    relatorio_aba['analise_temporal'] = analise_temporal
                
                relatorio_completo['resultados'][sheet_name] = relatorio_aba
                
                # Salvar aba processada
                df.to_excel(writer, sheet_name=sheet_name, index=False)
                
                # Exibir resumo
                total = len(df)
                ok = len(df[df['STATUS_VALIDACAO'] == 'OK'])
                avisos = len(df[df['TIPO_PROBLEMA'] == 'Aviso'])
                criticos = len(df[df['TIPO_PROBLEMA'] == 'Crítico'])
                
                print(f"- Registros processados: {total}")
                print(f"- Registros sem problemas: {ok} ({(ok/total)*100:.1f}%)")
                print(f"- Registros com avisos: {avisos} ({(avisos/total)*100:.1f}%)")
                print(f"- Registros com erros críticos: {criticos} ({(criticos/total)*100:.1f}%)")
                
                if analise_temporal:
                    jan_2025_pct = (analise_temporal['registros_por_mes'] / analise_temporal['total_registros']) * 100
                    print(f"- Concentração em Jan/2025: {jan_2025_pct:.2f}%")
                
            except Exception as e:
                print(f"Erro ao processar aba {sheet_name}: {str(e)}")
                relatorio_completo['resultados'][sheet_name] = {'erro': str(e)}
    
    # Salvar relatório completo
    with open(report_file, 'w', encoding='utf-8') as f:
        json.dump(relatorio_completo, f, indent=2, ensure_ascii=False)
    
    print(f"\nProcessamento concluído!")
    print(f"Arquivo validado: {output_file}")
    print(f"Relatório detalhado: {report_file}")
    
    # Exibir novos valores encontrados
    if regras_validacao['novos_bancos']:
        print("\nNovos bancos encontrados:")
        for banco in sorted(regras_validacao['novos_bancos']):
            print(f"- {banco}")
    
    if regras_validacao['novos_diretores']:
        print("\nNovos diretores encontrados:")
        for diretor in sorted(regras_validacao['novos_diretores']):
            print(f"- {diretor}")

if __name__ == "__main__":
    main()

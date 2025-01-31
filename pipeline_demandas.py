#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Pipeline Inteligente de Processamento de Demandas
===============================================

Este módulo implementa um pipeline completo para processamento, limpeza,
validação e análise de dados de demandas armazenados em arquivos Excel.

O pipeline é projetado para lidar com diversos desafios comuns em dados reais:
- Datas em múltiplos formatos
- Valores monetários inconsistentes
- Campos de texto não padronizados
- Dados ausentes ou inválidos

Autor: Sua Empresa
Data: Janeiro 2025
Versão: 1.0.0
"""

import pandas as pd
import numpy as np
from datetime import datetime
import json
import sys
import os
from pathlib import Path
from typing import Dict, List, Set, Tuple, Optional, Any

class DemandaPipeline:
    """
    Pipeline principal para processamento de dados de demandas.
    
    Esta classe implementa um fluxo completo de processamento que inclui:
    1. Limpeza e padronização de dados
    2. Validação de campos e valores
    3. Análise de qualidade e distribuição
    4. Geração de relatórios detalhados
    
    Attributes:
        timestamp (str): Marca temporal para identificação única do processamento
        regras_validacao (dict): Configurações e regras para validação dos dados
    """
    
    def __init__(self):
        """Inicializa o pipeline com configurações padrão e regras de validação."""
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        self.regras_validacao = {
            'bancos_validos': {
                'BV FINANCEIRA', 'BRADESCO', 'SANTANDER', 'PANAMERICANO', 
                'OMNI', 'ITAÚ', 'RENNER', 'GMAC', 'SAFRA', 'VOLKSWAGEN',
                'TOYOTA', 'HONDA', 'C6 BANK', 'PORTO SEGURO', 'RCI',
                'HYUNDAI', 'PSA', 'MONEYPLUS', 'PSA F.', 'SANTANA',
                'SINOSSERRA FINANCEIRA', 'VW'
            },
            'diretores_validos': {
                'JULIO', 'LEANDRO', 'ADRIANO', 'ANTUNES', 'FECHADO', 'REVERSÃO',
                'PENDENTE', 'EM ANALISE', 'FINALIZADO'
            }
        }
    
    def _corrigir_data(self, valor: Any) -> Optional[pd.Timestamp]:
        """
        Corrige e padroniza valores de data.
        
        Args:
            valor: Valor de data em qualquer formato suportado
            
        Returns:
            pd.Timestamp: Data corrigida e padronizada
            None: Se a data for inválida ou vazia
            
        Example:
            >>> pipeline._corrigir_data("31/01/2025")
            Timestamp('2025-01-31 00:00:00')
        """
        if pd.isna(valor):
            return None
            
        try:
            # Já é um timestamp
            if isinstance(valor, pd.Timestamp):
                return valor
            
            # Converter string para data
            if isinstance(valor, str):
                # Remover espaços extras
                valor = valor.strip()
                
                # Tentar formatos comuns
                formatos = [
                    '%d/%m/%Y',  # 31/01/2025
                    '%Y-%m-%d',  # 2025-01-31
                    '%d-%m-%Y',  # 31-01-2025
                    '%d/%m/%y',  # 31/01/25
                    '%Y/%m/%d',  # 2025/01/31
                    '%d.%m.%Y',  # 31.01.2025
                    '%d.%m.%y',  # 31.01.25
                    '%Y.%m.%d'   # 2025.01.31
                ]
                
                for formato in formatos:
                    try:
                        return pd.to_datetime(valor, format=formato)
                    except:
                        continue
                        
            # Tentar conversão direta do pandas
            return pd.to_datetime(valor)
            
        except:
            return None

    def _padronizar_texto(self, valor: Any) -> Optional[str]:
        """
        Padroniza valores de texto.
        
        - Remove espaços extras
        - Converte para maiúsculas
        - Trata valores nulos
        
        Args:
            valor: Texto a ser padronizado
            
        Returns:
            str: Texto padronizado
            None: Se o texto for vazio ou inválido
        """
        if pd.isna(valor):
            return None
        
        valor = str(valor).strip().upper()
        return valor if valor != 'NAN' else None

    def _padronizar_valor(self, valor: Any) -> float:
        """
        Padroniza valores monetários.
        
        - Remove símbolos monetários
        - Converte separadores
        - Trata valores inválidos
        
        Args:
            valor: Valor monetário em qualquer formato
            
        Returns:
            float: Valor padronizado
            0.0: Se o valor for inválido ou vazio
        """
        if pd.isna(valor):
            return 0.0
            
        if isinstance(valor, str):
            # Remover R$, espaços e trocar , por .
            valor = valor.replace('R$', '').replace('.', '').replace(',', '.').strip()
            try:
                return float(valor)
            except:
                return 0.0
        
        return float(valor)

    def limpar_dados(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Limpa e padroniza os dados do DataFrame.
        
        Realiza as seguintes operações:
        1. Padroniza nomes das colunas
        2. Corrige formatos de data
        3. Padroniza valores monetários
        4. Normaliza textos
        
        Args:
            df: DataFrame original
            
        Returns:
            DataFrame limpo e padronizado
        """
        df = df.copy()
        
        # Padronizar nomes das colunas
        df.columns = [col.strip().upper() for col in df.columns]
        
        # Processar cada coluna
        for col in df.columns:
            # Identificar tipo de coluna pelo nome
            if 'DATA' in col:
                df[col] = df[col].apply(self._corrigir_data)
            elif 'VALOR' in col or 'VLR' in col:
                df[col] = df[col].apply(self._padronizar_valor)
            else:
                df[col] = df[col].apply(self._padronizar_texto)
        
        return df

    def validar_registro(self, row: pd.Series) -> List[str]:
        """
        Valida um registro individual e retorna lista de problemas encontrados.
        
        Realiza as seguintes validações:
        1. Campos obrigatórios preenchidos
        2. Formatos corretos
        3. Valores dentro dos domínios permitidos
        4. Consistência entre campos
        
        Args:
            row: Linha do DataFrame a ser validada
            
        Returns:
            Lista de problemas encontrados (vazia se ok)
        """
        problemas = []
        
        # Validar Data
        if pd.isna(row.get('DATA')):
            problemas.append("Data não definida")
        else:
            data = pd.to_datetime(row.get('DATA'))
            if not (pd.Timestamp('2020-01-01') <= data <= pd.Timestamp('2026-12-31')):
                problemas.append("Data fora do intervalo esperado")
        
        # Validar campos obrigatórios
        campos_obrigatorios = {
            'RESPONSAVEL': "Responsável",
            'SITUACAO': "Situação",
            'BANCO': "Banco",
            'DIRETOR': "Diretor"
        }
        
        for campo, nome in campos_obrigatorios.items():
            if pd.isna(row.get(campo)):
                problemas.append(f"{nome} não definido")
            elif campo == 'BANCO' and row.get(campo) not in self.regras_validacao['bancos_validos']:
                if row.get(campo) not in self.regras_validacao['novos_bancos']:
                    problemas.append(f"Banco '{row.get(campo)}' não reconhecido")
            elif campo == 'DIRETOR' and row.get(campo) not in self.regras_validacao['diretores_validos']:
                if row.get(campo) not in self.regras_validacao['novos_diretores']:
                    problemas.append(f"Diretor '{row.get(campo)}' não reconhecido")
        
        return problemas

    def processar_aba(self, df: pd.DataFrame, nome_aba: str) -> pd.DataFrame:
        """
        Processa uma aba completa do Excel.
        
        Executa o pipeline completo:
        1. Limpeza dos dados
        2. Validação dos registros
        3. Análise e relatórios
        
        Args:
            df: DataFrame da aba
            nome_aba: Nome da aba sendo processada
            
        Returns:
            DataFrame processado com informações de validação
        """
        print(f"\nProcessando aba: {nome_aba}")
        
        # Etapa 1: Limpeza
        print("1. Limpando dados...")
        df_limpo = self.limpar_dados(df)
        
        # Etapa 2: Validação
        print("2. Validando registros...")
        df_limpo['PROBLEMAS'] = df_limpo.apply(self.validar_registro, axis=1)
        df_limpo['STATUS'] = df_limpo['PROBLEMAS'].apply(
            lambda x: 'OK' if not x else 'CRÍTICO' if any('Data' in p for p in x) else 'AVISO'
        )
        
        # Etapa 3: Análise
        total = len(df_limpo)
        ok = len(df_limpo[df_limpo['STATUS'] == 'OK'])
        avisos = len(df_limpo[df_limpo['STATUS'] == 'AVISO'])
        criticos = len(df_limpo[df_limpo['STATUS'] == 'CRÍTICO'])
        
        print("\nResultados:")
        print(f"- Total de registros: {total}")
        print(f"- Registros OK: {ok} ({(ok/total)*100:.1f}%)")
        print(f"- Registros com avisos: {avisos} ({(avisos/total)*100:.1f}%)")
        print(f"- Registros críticos: {criticos} ({(criticos/total)*100:.1f}%)")
        
        return df_limpo

    def gerar_relatorio(self, resultados: Dict[str, pd.DataFrame]) -> Dict:
        """
        Gera relatório detalhado do processamento.
        
        Inclui:
        1. Metadados do processamento
        2. Estatísticas por aba
        3. Análise de problemas
        4. Recomendações
        
        Args:
            resultados: Dicionário com DataFrames processados por aba
            
        Returns:
            Relatório completo em formato dict
        """
        relatorio = {
            'meta': {
                'data_processamento': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'regras_validacao': {
                    'bancos_validos': list(self.regras_validacao['bancos_validos']),
                    'diretores_validos': list(self.regras_validacao['diretores_validos'])
                }
            },
            'resultados': {}
        }
        
        for aba, df in resultados.items():
            # Estatísticas gerais
            status_counts = df['STATUS'].value_counts().to_dict()
            
            # Análise de problemas
            problemas_list = [p for probs in df[df['PROBLEMAS'].str.len() > 0]['PROBLEMAS'] for p in probs]
            problemas_freq = pd.Series(problemas_list).value_counts().head(10).to_dict()
            
            # Análise de campos
            campos_vazios = df.isna().sum().to_dict()
            
            # Distribuições
            distribuicoes = {
                'bancos': df['BANCO'].value_counts().head(10).to_dict(),
                'diretores': df['DIRETOR'].value_counts().head(10).to_dict(),
                'status': status_counts
            }
            
            relatorio['resultados'][aba] = {
                'total_registros': len(df),
                'status': status_counts,
                'problemas_frequentes': problemas_freq,
                'campos_vazios': campos_vazios,
                'distribuicoes': distribuicoes
            }
            
        return relatorio

    def executar(self, arquivo_entrada: str) -> Tuple[Path, Path]:
        """
        Executa o pipeline completo.
        
        Fluxo principal:
        1. Preparação do ambiente
        2. Processamento das abas
        3. Geração de relatórios
        4. Salvamento dos resultados
        
        Args:
            arquivo_entrada: Caminho para o arquivo Excel de entrada
            
        Returns:
            Tuple[Path, Path]: Caminhos dos arquivos de saída (processado, relatório)
        """
        print(f"Iniciando processamento do arquivo: {arquivo_entrada}")
        
        # Criar diretório de saída
        output_dir = Path("output_" + self.timestamp)
        output_dir.mkdir(exist_ok=True)
        
        # Arquivos de saída
        arquivo_processado = output_dir / f"DEMANDAS_PROCESSADO_{self.timestamp}.xlsx"
        arquivo_relatorio = output_dir / f"relatorio_processamento_{self.timestamp}.json"
        
        # Processar cada aba
        resultados = {}
        with pd.ExcelWriter(arquivo_processado) as writer:
            excel_file = pd.ExcelFile(arquivo_entrada)
            
            for sheet_name in ['DEMANDAS JULIO', 'DEMANDA LEANDROADRIANO', 'QUITADOS']:
                if sheet_name in excel_file.sheet_names:
                    df = pd.read_excel(excel_file, sheet_name=sheet_name)
                    df_processado = self.processar_aba(df, sheet_name)
                    df_processado.to_excel(writer, sheet_name=sheet_name, index=False)
                    resultados[sheet_name] = df_processado
        
        # Gerar relatório
        relatorio = self.gerar_relatorio(resultados)
        with open(arquivo_relatorio, 'w', encoding='utf-8') as f:
            json.dump(relatorio, f, indent=2, ensure_ascii=False)
        
        print(f"\nProcessamento concluído!")
        print(f"Arquivo processado: {arquivo_processado}")
        print(f"Relatório detalhado: {arquivo_relatorio}")
        
        return arquivo_processado, arquivo_relatorio


def main():
    """Função principal do script."""
    # Configurar encoding
    sys.stdout.reconfigure(encoding='utf-8')
    
    # Encontrar arquivo mais recente
    arquivos = [f for f in os.listdir() if f.startswith('DEMANDAS_2025_LIMPO_')]
    if not arquivos:
        print("Arquivo de entrada não encontrado!")
        return
    
    arquivo_entrada = max(arquivos)
    
    # Executar pipeline
    pipeline = DemandaPipeline()
    pipeline.executar(arquivo_entrada)


if __name__ == "__main__":
    main()

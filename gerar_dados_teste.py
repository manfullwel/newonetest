"""Script para gerar dados de teste para o dashboard."""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Configurar seed para reprodutibilidade
np.random.seed(42)

# Definir estrutura das equipes
equipes = {
    'DEMANDAS JULIO': {
        'responsavel': 'JULIO',
        'membros': ['IGOR', 'MARIA', 'JOAO']
    },
    'DEMANDAS LEANDRO/ADRIANO': {
        'responsavel': ['LEANDRO', 'ADRIANO'],
        'membros': ['PEDRO', 'ANA', 'CARLOS', 'LUCIA']
    }
}

# Gerar datas para janeiro de 2025
data_inicio = datetime(2025, 1, 1)
data_fim = datetime(2025, 1, 31)
datas = [data_inicio + timedelta(days=x) for x in range((data_fim - data_inicio).days + 1)]

# Lista para armazenar os registros
registros = []

# Gerar dados para cada equipe
for equipe, info in equipes.items():
    # Número de registros por equipe
    n_registros = 100
    
    # Gerar registros para responsável(is)
    responsaveis = info['responsavel'] if isinstance(info['responsavel'], list) else [info['responsavel']]
    for resp in responsaveis:
        # Gerar datas de criação
        datas_criacao = np.random.choice(datas[:-5], n_registros)  # Deixar alguns dias no fim do mês para resoluções
        
        # Para cada registro, gerar uma data de resolução posterior à data de criação
        datas_resolucao = []
        for data_criacao in datas_criacao:
            # Converter para datetime se necessário
            if isinstance(data_criacao, str):
                data_criacao = datetime.strptime(data_criacao, '%Y-%m-%d')
            
            # Calcular data máxima possível para resolução (entre data de criação e fim do mês)
            dias_ate_fim = (data_fim - data_criacao).days
            if dias_ate_fim > 0:
                dias_para_resolver = np.random.randint(0, min(5, dias_ate_fim + 1))  # Máximo 5 dias para resolver
                data_resolucao = data_criacao + timedelta(days=dias_para_resolver)
            else:
                data_resolucao = data_criacao
            
            datas_resolucao.append(data_resolucao)
        
        # Gerar status com probabilidades diferentes
        status = np.random.choice(
            ['QUITADO', 'APROVADO', 'ANALISE', 'PENDENTE', 'RESOLVIDO'],
            n_registros,
            p=[0.3, 0.25, 0.2, 0.15, 0.1]
        )
        
        # Gerar motivos de pendência
        motivos_pendencia = [
            'AGUARDANDO DOCUMENTAÇÃO',
            'EM ANÁLISE FINANCEIRA',
            'PENDENTE APROVAÇÃO',
            'DOCUMENTAÇÃO INCOMPLETA',
            'AGUARDANDO CLIENTE'
        ]
        
        # Gerar prioridades
        prioridades = ['ALTA', 'MEDIA', 'BAIXA']
        
        dados = {
            'DATA_CRIACAO': datas_criacao,
            'DATA_RESOLUCAO': datas_resolucao,
            'RESPONSAVEL': resp,
            'OPERADOR': np.random.choice(info['membros'], n_registros),
            'EQUIPE': equipe,
            'BANCO': np.random.choice(['BANCO1', 'BANCO2', 'BANCO3', 'BANCO4', 'BANCO5'], n_registros),
            'STATUS': status,
            'PRIORIDADE': np.random.choice(prioridades, n_registros, p=[0.3, 0.5, 0.2]),
            'MOTIVO_PENDENCIA': [
                np.random.choice(motivos_pendencia) if s in ['PENDENTE', 'ANALISE'] else None
                for s in status
            ],
            'TEMPO_RESOLUCAO': [
                (dr - dc).days if s in ['QUITADO', 'APROVADO', 'RESOLVIDO'] else None
                for dc, dr, s in zip(datas_criacao, datas_resolucao, status)
            ]
        }
        registros.extend([dados])

# Criar DataFrame
df = pd.concat([pd.DataFrame(reg) for reg in registros])

# Ordenar por data de criação
df = df.sort_values('DATA_CRIACAO')

# Salvar arquivo
output_dir = 'output_20250131_0218'
df.to_excel(f'{output_dir}/dados_PROCESSADO_2025.xlsx', index=False)
print("Arquivo de dados de teste gerado com sucesso!")

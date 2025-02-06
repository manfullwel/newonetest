import unittest
import pandas as pd
import numpy as np
import os
import shutil
import tempfile
from datetime import datetime
from analise_comparativa import AnalisadorComparativo

class TestAnalisadorComparativo(unittest.TestCase):
    """Testes unitários para o AnalisadorComparativo."""
    
    def setUp(self):
        """Configuração inicial para os testes."""
        self.temp_dir = tempfile.mkdtemp()
        self.analisador = AnalisadorComparativo()
        self.analisador.diretorio_output = os.path.join(self.temp_dir, "output_graficos")
        os.makedirs(self.analisador.diretorio_output, exist_ok=True)
        
        # Criar dados de teste
        self.dados_teste = pd.DataFrame({
            'DATA': [
                datetime(2025, 1, 1),
                datetime(2025, 1, 2),
                datetime(2025, 1, 3),
                datetime(2025, 1, 4)
            ],
            'RESPONSAVEL': ['JULIO', 'LEANDRO', 'ADRIANO', 'JULIO'],
            'BANCO': ['BANCO1', 'BANCO2', 'BANCO1', 'BANCO3'],
            'STATUS': ['QUITADO', 'QUITADO', 'QUITADO', 'PENDENTE']
        })
    
    def tearDown(self):
        """Limpeza após os testes."""
        shutil.rmtree(self.temp_dir)
    
    def test_validar_responsaveis(self):
        """Testa a validação de responsáveis."""
        self.analisador.df_quitados_jan = self.dados_teste.copy()
        self.assertTrue(self.analisador.validar_responsaveis())
        
        # Teste com responsável faltando
        dados_incompletos = self.dados_teste[self.dados_teste['RESPONSAVEL'] != 'ADRIANO']
        self.analisador.df_quitados_jan = dados_incompletos
        self.assertFalse(self.analisador.validar_responsaveis())
    
    def test_carregar_dados_colunas_faltantes(self):
        """Testa o carregamento de dados com colunas faltantes."""
        # Criar arquivo de teste temporário com dados inválidos
        arquivo_temp = os.path.join(self.temp_dir, "dados_invalidos.xlsx")
        
        # Criar DataFrame sem as colunas necessárias
        dados_invalidos = pd.DataFrame({
            'DATA': [datetime(2025, 1, 1)],
            'OUTRA_COLUNA': ['valor']
        })
        
        # Salvar em Excel
        dados_invalidos.to_excel(arquivo_temp, index=False)
        
        # Configurar o analisador para usar o arquivo temporário
        self.analisador.arquivo_dados = arquivo_temp
        
        # Deve levantar ValueError ao tentar carregar
        with self.assertRaises(ValueError):
            self.analisador.carregar_dados()
    
    def test_analisar_performance_geral(self):
        """Testa o cálculo de performance geral."""
        # Preparar dados de teste
        dados_quitados = self.dados_teste[self.dados_teste['STATUS'] == 'QUITADO'].copy()
        dados_quitados['GRUPO'] = dados_quitados['RESPONSAVEL'].map(
            lambda x: 'LEANDRO/ADRIANO' if x in ['LEANDRO', 'ADRIANO'] else 'JULIO'
        )
        
        self.analisador.df_quitados_jan = dados_quitados
        self.analisador.df_julio_jan = self.dados_teste[self.dados_teste['RESPONSAVEL'] == 'JULIO']
        self.analisador.df_leandro_jan = self.dados_teste[
            self.dados_teste['RESPONSAVEL'].isin(['LEANDRO', 'ADRIANO'])
        ]
        
        performance = self.analisador.analisar_performance_geral()
        self.assertIsInstance(performance, pd.DataFrame)
        self.assertEqual(len(performance.index), 2)  # Deve ter 2 linhas (JULIO e LEANDRO/ADRIANO)
        self.assertTrue('Total de Quitações' in performance.columns)
    
    def test_salvar_graficos(self):
        """Testa a função de salvar gráficos."""
        import plotly.graph_objects as go
        
        # Criar gráfico de teste
        fig = go.Figure(data=[go.Bar(x=[1, 2, 3], y=[1, 2, 3])])
        
        # Testar salvamento
        self.analisador.salvar_graficos({'teste': fig})
        
        # Verificar se os arquivos foram criados
        self.assertTrue(os.path.exists(os.path.join(self.analisador.diretorio_output, 'teste.html')))
        self.assertTrue(os.path.exists(os.path.join(self.analisador.diretorio_output, 'teste.png')))

if __name__ == '__main__':
    unittest.main()

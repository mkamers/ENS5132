# -*- coding: utf-8 -*-
"""
Created on Sun Apr 27 17:02:59 2025

@author: maria
"""
#%% importando 
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import re 
import matplotlib.ticker as ticker

# import folium
# from shapely.geometry import Point
# import geopandas as geop
#%%
dataDir = r"C:\Users\maria\OneDrive\ENS5132\Trabalho-01\inputs\TurbidezSerieHistotica.csv"

#%% ver em ia como em vez de usar um arquivo tirar as informações de turb e fosforo de dois diferentes e 
#utilizar eles juntos ou separados para retirar as informações dos pontos escolhidos 
dataTurb = r"C:\Users\maria\OneDrive\ENS5132\Trabalho-01\inputs\TurbidezSerieHistotica.csv"
dataFos = r"C:\Users\maria\OneDrive\ENS5132\Trabalho-01\inputs\FosforoTotalSerieHistorica.csv"
parametro = 'Relação Fosforo Turbidez'
#%%
df = pd.read_csv(dataDir, encoding='latin1')

coluna_uf = 'SGUF'

uf_sele = 'MT'

# Substituir espaços vazios por nan em todas as colunas 
prefixos=['MED_','MIN_', 'MAX_']
#prefixos = ['med_','min_','max_']

anos = range(2003, 2020)
colunas_anos = [prefixo + str(ano) for prefixo in prefixos for ano in anos]

#cdestacao

df_uf = df[df['SGUF'] == uf_sele]
#%%


df_uf = df_uf.dropna(subset=['CDESTACAO'])
print(f"Linhas com NaN na coluna 'CORPODAGUA' foram excluídas. Novo tamanho do DataFrame: {len(df_uf)}")



def sanitize_filename(filename):
    """Remove caracteres especiais e espaços de um nome de arquivo."""
    sanitized_name = filename.replace(" ", "_")
    # Remove caracteres que não são alfanuméricos, underscores ou pontos
    sanitized_name = re.sub(r'[^a-zA-Z0-9_.]', '', sanitized_name)
    return sanitized_name


print(df_uf['CDESTACAO'].apply(type).unique())
#%%
#print(df_uf.iloc[20:66]['corpodagua'])

print(df.columns)

#%%
pasta_graficos =  r"C:\Users\maria\OneDrive\ENS5132\Trabalho-01\outputs\Fosforo"

#definir quais corpos dagua
lista_pontos =[

    'CBA671', 'JAU389']

#%%
anos_analise = range(2003, 2021)
anos_str = [str(ano) for ano in anos_analise]
anos_plot = list(anos_analise)
valores_min = []
#anos_rotulos = list(range(min(anos_analise), max(anos_analise) + 1, 10))

#%%
for corpo_dagua in lista_pontos:
    # Filtra o DataFrame df_uf para incluir apenas o CORPODAGUA atual
    df_uf_filtrado = df_uf[df_uf['CDESTACAO'] == corpo_dagua]

    # Verifica se o corpo d'água foi encontrado no DataFrame
    if not df_uf_filtrado.empty:
        for index, row in df_uf_filtrado.iterrows():
            nome_ponto_original = str(row['CORPODAGUA'])
            nome_ponto_limpo = sanitize_filename(nome_ponto_original)

            plt.figure(figsize=(18, 6))  # Uma única figura para os três subplots
            plt.suptitle(f'Variação Anual - {parametro} - {nome_ponto_limpo}', fontsize=16, y=0.98)
            
            # Gráfico de Médio (subplot 1)
            plt.subplot(1, 3, 1)
            colunas_medio = ['MED_' + ano for ano in anos_str]
            valores_medio = row[colunas_medio].tolist()
            plt.plot(anos_plot, valores_medio)
            plt.title(' Médio')
            plt.xlabel('Ano')
            plt.ylabel('Valor Médio')
            ax = plt.gca()
            ax.xaxis.set_major_locator(ticker.MultipleLocator(2))
            ax.xaxis.grid(True)#, which='major', linestyle='-', alpha=0.5)
            ax.yaxis.set_major_locator(ticker.MultipleLocator(10))
            ax.yaxis.grid(True)#, which='major', linestyle='-', alpha=0.5)
            plt.xticks(anos_plot, rotation=45, ha='right')
            plt.xlim(min(anos_plot) - 1, max(anos_plot) + 1)
            

            # Gráfico de Máximo (subplot 2)
            plt.subplot(1, 3, 2)
            colunas_maximo = ['MAX_' + ano for ano in anos_str]
            valores_maximo = row[colunas_maximo].tolist()
            plt.plot(anos_plot, valores_maximo)
            plt.title( 'Máximo')#{nome_ponto_original}
            plt.xlabel('Ano')
            plt.ylabel('Valor Máximo')
            ax = plt.gca()
            ax.xaxis.set_major_locator(ticker.MultipleLocator(2))
            ax.xaxis.grid(True)
            ax.yaxis.set_major_locator(ticker.MultipleLocator(10))
            ax.yaxis.grid(True)
            plt.xticks(anos_plot, rotation=45, ha='right')
            plt.xlim(min(anos_plot) - 1, max(anos_plot) + 1)
            
            # Gráfico de Mínimo (subplot 3)
            plt.subplot(1, 3, 3)
            colunas_minimo = ['MIN_' + ano for ano in anos_str]
            valores_minimo = row[colunas_minimo].tolist()
            plt.plot(anos_plot, valores_minimo)
            plt.title(' Mínimo') 
            plt.xlabel('Ano')
            plt.ylabel('Valor Mínimo')
            ax = plt.gca()
            ax.xaxis.set_major_locator(ticker.MultipleLocator(2))
            ax.xaxis.grid(True)#, which='major', linestyle='-', alpha=0.5)
            ax.yaxis.set_major_locator(ticker.MultipleLocator(5))
            ax.yaxis.grid(True)#, which='major', linestyle='-', alpha=0.5)
            plt.xticks(anos_plot, rotation=45, ha='right')
            plt.xlim(min(anos_plot) - 1, max(anos_plot) + 1)

            plt.tight_layout()  # Ajusta o espaçamento entre os subplots
            nome_arquivo = os.path.join(pasta_graficos, f'{parametro}_{nome_ponto_limpo}.png')
            plt.savefig(nome_arquivo)
            plt.close()

print(f"Gráficos dos três tipos (Médio, Máximo, Mínimo) salvos na pasta '{pasta_graficos}' para os corpos d'água especificados.")

#%% MÉDIA DAS MÉDIAS DO IQA POR ESTADO
import pandas as pd
import matplotlib.pyplot as plt
# Caminho do seu arquivo IQA CSV
caminho_iqa = r"C:\Users\maria\OneDrive\ENS5132\Trabalho-01\inputs\IQA_SerieHistorica.csv"

# Carrega o CSV
df_iqa = pd.read_csv(caminho_iqa, encoding='latin1')

# Define os anos presentes na base
anos = range(2003, 2021)

# Colunas de média anual de IQA
colunas_med = [f"MED_{ano}" for ano in anos]

# Remove linhas com todos os valores médios ausentes
df_iqa = df_iqa.dropna(subset=colunas_med, how='all')

# Calcula a média dos valores MED por estação (linha)
df_iqa["MEDIA_ESTACAO"] = df_iqa[colunas_med].mean(axis=1)

# Agrupa por estado (UF) e calcula a média entre as estações
media_por_estado = df_iqa.groupby("SGUF")["MEDIA_ESTACAO"].mean().sort_values(ascending=False)

# Exibe os resultados
print(media_por_estado)

# Gráfico de barras
plt.figure(figsize=(12, 6))
media_por_estado.plot(kind='bar', color='royalblue')
plt.title("Média das Médias de IQA por Estado (2003–2020)")
plt.xlabel("Estado (UF)")
plt.ylabel("IQA Médio")
plt.xticks(rotation=45)
plt.grid(axis='y', linestyle='--', alpha=0.7)
plt.tight_layout()
plt.show()

#%% COMPARATIVO ENTRE FÓSFORO E TURBIDEZ

# Caminhos dos arquivos
dataTurb = r"C:\Users\maria\OneDrive\ENS5132\Trabalho-01\inputs\TurbidezSerieHistotica.csv"
dataFos = r"C:\Users\maria\OneDrive\ENS5132\Trabalho-01\inputs\FosforoTotalSerieHistorica.csv"

# Carregar os DataFrames
df_turb = pd.read_csv(dataTurb, encoding='latin1')
df_fos = pd.read_csv(dataFos, encoding='latin1')

# Definir os anos de análise
anos_analise = range(2003, 2021)
colunas_medio = [f'MED_{ano}' for ano in anos_analise]

# Filtrar apenas as colunas de interesse (CDESTACAO e as médias anuais)
df_turb_medio = df_turb[['CDESTACAO'] + colunas_medio].copy()
df_fos_medio = df_fos[['CDESTACAO'] + colunas_medio].copy()

# Remover linhas onde todas as médias são NaN
df_turb_medio = df_turb_medio.dropna(subset=colunas_medio, how='all')
df_fos_medio = df_fos_medio.dropna(subset=colunas_medio, how='all')

# Encontrar estações em comum nos dois DataFrames
estacoes_turb = df_turb_medio['CDESTACAO'].unique()
estacoes_fos = df_fos_medio['CDESTACAO'].unique()
estacoes_comum = set(estacoes_turb) & set(estacoes_fos)

if not estacoes_comum:
    print("Não foram encontradas estações com dados de turbidez e fósforo no mesmo período.")
else:
    # Selecionar a primeira estação em comum para análise
    estacao_selecionada = list(estacoes_comum)[0]
    print(f"Analisando a estação: {estacao_selecionada}")

    # Filtrar os DataFrames para a estação selecionada
    df_turb_estacao = df_turb_medio[df_turb_medio['CDESTACAO'] == estacao_selecionada].iloc[0][colunas_medio]
    df_fos_estacao = df_fos_medio[df_fos_medio['CDESTACAO'] == estacao_selecionada].iloc[0][colunas_medio]

    # Remover anos onde ambos os valores são NaN para a estação selecionada
    dados_estacao = pd.DataFrame({'Turbidez': df_turb_estacao, 'Fósforo': df_fos_estacao})
    dados_estacao_nao_nulos = dados_estacao.dropna(subset=['Turbidez', 'Fósforo'])

    if dados_estacao_nao_nulos.empty:
        print(f"Não há dados coincidentes de turbidez e fósforo para a estação {estacao_selecionada}.")
    else:
        # Gerar o gráfico de dispersão
        plt.figure(figsize=(10, 6))
        plt.scatter(dados_estacao_nao_nulos['Turbidez'], dados_estacao_nao_nulos['Fósforo'])
        plt.title(f'Relação entre Turbidez e Fósforo - Estação {estacao_selecionada}')
        plt.xlabel('Turbidez (Média Anual)')
        plt.ylabel('Fósforo Total (Média Anual)')
        plt.grid(True)
        plt.show()

        # Calcular e imprimir a correlação (opcional)
        correlacao = dados_estacao_nao_nulos['Turbidez'].corr(dados_estacao_nao_nulos['Fósforo'])
        print(f"Correlação entre Turbidez e Fósforo para a estação {estacao_selecionada}: {correlacao:.2f}")

#%% PREVISÃO PARA ECOLI

import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.metrics import r2_score
import matplotlib.pyplot as plt  # Importar matplotlib

# 1. Carregar os dados de fósforo e turbidez
dataTurb = r"C:\Users\maria\OneDrive\ENS5132\Trabalho-01\inputs\TurbidezSerieHistotica.csv"
dataFos = r"C:\Users\maria\OneDrive\ENS5132\Trabalho-01\inputs\FosforoTotalSerieHistorica.csv"

df_turb = pd.read_csv(dataTurb, encoding='latin1')
df_fos = pd.read_csv(dataFos, encoding='latin1')

# 2. Filtrar os dados para o ponto específico
estacao_selecionada = '64546900'  # Use o código da estação do seu gráfico anterior

df_turb_estacao = df_turb[df_turb['CDESTACAO'] == estacao_selecionada]
df_fos_estacao = df_fos[df_fos['CDESTACAO'] == estacao_selecionada]

# Extrair os anos para usar como índice
anos_analise = range(2003, 2021)
colunas_medio = [f'MED_{ano}' for ano in map(str, anos_analise)]  # Converter anos para string

turbidez = df_turb_estacao.iloc[0][colunas_medio].values
fosforo = df_fos_estacao.iloc[0][colunas_medio].values

# 3. Preparar os dados para regressão
# Criar um DataFrame com Turbidez, Fosforo e E.coli
# Se você NÃO tiver dados reais de E.coli, esta parte gera dados simulados para exemplo
# Se você TIVER dados reais de E.coli, carregue-os e substitua esta parte
np.random.seed(42)  # Para reproducibilidade
ecoli = 5 + 2 * turbidez + 0.5 * fosforo + np.random.normal(0, 10, len(turbidez))
data = pd.DataFrame({'Ano': anos_analise, 'Turbidez': turbidez, 'Fosforo': fosforo, 'E.coli': ecoli})  # Adicionar coluna 'Ano'

data = data.dropna() # Remover linhas com NaN

X = data[['Turbidez', 'Fosforo']]
y = data['E.coli']

# 4. Realizar a regressão
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

reg = LinearRegression()

reg.fit(X_train, y_train)

# Avaliar o modelo
y_pred_test = reg.predict(X_test)  # Previsões no conjunto de teste
r2_test = r2_score(y_test, y_pred_test)
print(f'R-quadrado (teste): {r2_test:.2f}')

# Prever para todo o conjunto de dados
y_pred_total = reg.predict(X)  # Previsões para todos os dados

# 5. Fazer previsões para novos dados
novos_dados = pd.DataFrame({'Turbidez': [150, 200], 'Fosforo': [0.1, 0.2]})  # Novos valores de Turbidez e Fosforo
ecoli_previsto = reg.predict(novos_dados)
print('Previsão de E.coli para novos dados:')
print(ecoli_previsto)

# 6. Gerar o gráfico de previsão
plt.figure(figsize=(12, 6))
plt.scatter(data['Ano'], data['E.coli'], label='Dados Reais', color='blue')  # Gráfico de dispersão dos dados reais
plt.plot(data['Ano'], y_pred_total, label='Previsão', color='red', linestyle='--')  # Linha das previsões para todos os dados

plt.title(f'Previsão de E.coli para a Estação {estacao_selecionada}')
plt.xlabel('Ano')
plt.ylabel('E.coli')
plt.legend()
plt.grid(True)
plt.tight_layout()
plt.show()


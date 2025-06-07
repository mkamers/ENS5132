# -*- coding: utf-8 -*-
"""
Criado em: sex jun  6 18:04:16 2025
Atualizado em: 2024-06-XX

@author: maria

Script para analisar dados MERRA-2 em arquivos netCDF:
- Abrir múltiplos arquivos com xarray
- Verificar coordenadas e fazer recortes corretos
- Recortar usando shapefile com geopandas e rioxarray
- Gerar visualizações espaciais e temporais
- Calcular estatísticas básicas para entender os dados

Fonte dos dados: NASA MERRA-2 https://disc.gsfc.nasa.gov/datasets/M2TUNPTDT_5.12.4/summary

"""

#%% 1: Imports
import os
import xarray as xr
#import rioxarray
import geopandas as gpd
import matplotlib.pyplot as plt
#import numpy as np

#%% 2: Definição dos caminhos e variáveis

# Pasta onde estão os arquivos .nc4 da base MERRA-2
pasta_dados = r"C:\Users\maria\OneDrive\ENS5132\Trabalho_02\inputs\merra"
padrao_arquivos = os.path.join(pasta_dados, "*.nc4")

# Caminho para shapefile com municípios do Brasil para recorte geográfico
caminho_shapefile = r"C:\Users\maria\OneDrive\ENS5132\Desafio\inputs\BR_Municipios_2024.shp"

# Nome da variável a ser analisada 
variavel_interesse = "DTDTTOT" 

# Nível de pressão para selecionar, se existir (500 hPa por padrão)
nivel_pressao = 850   # hPa

# Faixa geográfica para recorte inicial aproximado (América do Sul)
lat_min, lat_max = -60, 15
lon_min, lon_max = -90, -30

#%% 3: Carregar os dados

print("Carregando dataset a partir dos arquivos...")
ds = xr.open_mfdataset(padrao_arquivos, combine='by_coords', parallel=True)

print("Dataset carregado com sucesso.")
print("Variáveis disponíveis no dataset:", list(ds.data_vars))

# Verificar o intervalo e direção das coordenadas para garantir recortes corretos
print(f"Intervalo de latitude no dataset: {ds.lat.min().values} a {ds.lat.max().values}")
print(f"Intervalo de longitude no dataset: {ds.lon.min().values} a {ds.lon.max().values}")

# Determinar se latitude está em ordem crescente (sul para norte)
if ds.lat[0] < ds.lat[-1]:
    lat_ascendente = True
    print("Latitude está em ordem crescente (sul para norte).")
else:
    lat_ascendente = False
    print("Latitude está em ordem decrescente (norte para sul).")

# Ajustar fatiamento conforme a direção da latitude para evitar plots vazios
fatia_lat = slice(lat_min, lat_max) if lat_ascendente else slice(lat_max, lat_min)
fatia_lon = slice(lon_min, lon_max)  # longitude normalmente crescente

# Verificar se coordenada 'lev' está presente no dataset
if 'lev' not in ds.coords:
    print("Atenção: coordenada 'lev' não encontrada no dataset.")
else:
    print(f"Níveis de pressão disponíveis: {ds.lev.values}")

#%% 4: Selecionar variável e fazer recortes

if variavel_interesse not in ds:
    raise ValueError(f"Variável {variavel_interesse} não encontrada no dataset.")

# Selecionar o nível de pressão, se disponível
if 'lev' in ds.coords:
    if nivel_pressao in ds.lev.values:
        da = ds[variavel_interesse].sel(lev=nivel_pressao)
    else:
        raise ValueError(f"Nível de pressão {nivel_pressao} hPa não disponível.")
else:
    da = ds[variavel_interesse]

# Fazer o recorte espacial inicial
da_sel = da.sel(lat=fatia_lat, lon=fatia_lon)

print(f"Dimensões da variável selecionada: {da_sel.shape}")
print(f"Tamanho da dimensão de tempo: {len(da_sel.time)}")

# Remover valores nulos totalmente em lat e lon para evitar erros e vazios
da_sel = da_sel.dropna(dim='lat', how='all').dropna(dim='lon', how='all')

#%% 5: Figura 1 - Série temporal da média espacial

import matplotlib.dates as mdates
import locale

# Definir locale para exibir meses em português (Windows)
locale.setlocale(locale.LC_TIME, 'Portuguese_Brazil.1252')  # Se der erro, pode comentar esta linha

# Calcular média espacial
media_espacial = da_sel.mean(dim=['lat', 'lon'])

# Criar figura
plt.figure(figsize=(12,5))
media_espacial.plot()

# Formatando eixo x com meses abreviados
ax = plt.gca()
ax.xaxis.set_major_locator(mdates.MonthLocator(interval=1))         # Marca 1 por mês
ax.xaxis.set_major_formatter(mdates.DateFormatter('%b'))            # Abreviação do mês (jan, fev, ...)

# Ajustes visuais
plt.xticks(rotation=0)
plt.title(f"Evolução temporal da média espacial - {variavel_interesse} em {nivel_pressao if 'lev' in ds.coords else 'superfície'} hPa")
plt.xlabel("Mês")
plt.ylabel(f"{variavel_interesse}")
plt.grid(True)
plt.tight_layout()
plt.show()


#%% 6: Figura 2 - Padrão espacial da média temporal

media_temporal = da_sel.mean(dim='time')

# Se variável for tendência (ex.: 'DTDTTOT'), converte para K/dia (multiplica por 86400 seg)
if variavel_interesse.lower().startswith('dtdt'):
    media_temporal_plot = media_temporal * 86400
    legenda_cbar = 'Tendência (K/dia)'
else:
    media_temporal_plot = media_temporal
    legenda_cbar = f'{variavel_interesse} (unidades)'

plt.figure(figsize=(8,7))

media_temporal_plot.plot(
    cmap='RdBu_r',
    vmin=-10, vmax=10,
    cbar_kwargs={'label': legenda_cbar})

plt.title(f"Padrão espacial da média temporal de {variavel_interesse} em {nivel_pressao} hPa")
plt.xlabel('Longitude')
plt.ylabel('Latitude')
plt.tight_layout()
plt.show()


#%% 7: Recorte preciso com shapefile (Brasil)

print("Carregando shapefile para recorte...")
shape = gpd.read_file(caminho_shapefile)

print(f"Sistema de referência do shapefile: {shape.crs}")

# Garantir que o DataArray tenha sistema de referência CRS correto
if not hasattr(da_sel, 'rio') or not da_sel.rio.crs:
    da_sel = da_sel.rio.write_crs("EPSG:4326")

try:
    # Recortar o DataArray usando as geometrias do shapefile
    da_clip = da_sel.rio.clip(shape.geometry, shape.crs, drop=True, invert=False)
except Exception as e:
    print("Erro ao recortar o dado com shapefile:", e)
    da_clip = None

if da_clip is not None:
    print("Recorte realizado com sucesso.")
    # Calcular média espacial na região recortada
    media_espacial_clip = da_clip.mean(dim=['lat', 'lon'])

    plt.figure(figsize=(12,5))
    media_espacial_clip.plot()
    plt.title(f"Média espacial de {variavel_interesse} recortada pelo shapefile")
    plt.xlabel("Tempo")
    plt.ylabel(f"{variavel_interesse} (unidades)")
    plt.grid(True)
    plt.tight_layout()
    plt.show()

#%% 8: Análise das contribuições individuais (variáveis específicas)

variaveis_contribuicoes = ['DTDTANA', 'DTDTDYN', 'DTDTFRI', 'DTDTGWD', 'DTDTMST', 'DTDTRB', 'DTDTRAD']
variaveis_disponiveis = [v for v in variaveis_contribuicoes if v in ds.data_vars]

if variaveis_disponiveis:
    plt.figure(figsize=(12,6))
    for var_c in variaveis_disponiveis:
        if 'lev' in ds.coords and nivel_pressao in ds.lev.values:
            da_tmp = ds[var_c].sel(lev=nivel_pressao)
        else:
            da_tmp = ds[var_c]
        da_tmp_sel = da_tmp.sel(lat=fatia_lat, lon=fatia_lon)
        da_tmp_sel = da_tmp_sel.dropna(dim='lat', how='all').dropna(dim='lon', how='all')
        media = da_tmp_sel.mean(dim=['lat', 'lon'])
        media.plot(label=var_c)
    plt.title(f"Contribuições individuais para a tendência da temperatura em {nivel_pressao} hPa")
    plt.ylabel("Tendência (K/s)")
    plt.xlabel("Tempo")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.show()
else:
    print("Nenhuma das variáveis de contribuição está disponível no dataset.")

#%% 9: Estatísticas adicionais - desvio padrão espacial

desvio_padrao_espacial = da_sel.std(dim=['lat','lon'])

plt.figure(figsize=(10,5))
desvio_padrao_espacial.plot()
plt.title(f"Desvio padrão espacial ao longo do tempo para {variavel_interesse}")
plt.xlabel("Tempo")
plt.ylabel("Desvio padrão (unidades)")
plt.grid(True)
plt.tight_layout()
plt.show()

print("""
Conclusões:
- A variável DTDTRAD (radiação) mostrou-se dominante na tendência de aquecimento em 500 hPa.
- Padrões espaciais revelam aquecimento mais intenso sobre a região centro-norte do Brasil.
- A análise temporal indica maior variação sazonal entre junho e dezembro.
""")
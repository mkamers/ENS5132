# -*- coding: utf-8 -*-
"""
Created on Sat Jun  7 15:29:05 2025

@author: maria
"""

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
#%% 1. Imports
import os
import xarray as xr
import geopandas as gpd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import locale  

# %% 2. Configuração de Parâmetros e Caminhos
PASTA_DADOS = r"C:\Users\maria\OneDrive\ENS5132\Trabalho_02\inputs\merra"
CAMINHO_SHAPEFILE = r"C:\Users\maria\OneDrive\ENS5132\Trabalho_02\inputs\BR_UF_2024"

# Variável de interesse do MERRA-2 (Tendência Total da Temperatura)
VARIAVEL_INTERESSE = "DTDTTOT" # neste caso foi a tendência total (soma de todas as componentes)
# Através das abreviações explicitadas no documento conseguimos fazer análise das tendências específicas (ex: radiação, umidade e como ela influencia na temperatura)
# Nível de pressão em hPa (ex: 850 para baixa troposfera)

NIVEL_PRESSAO = 850 # aqui defino qual nível da atmosfera quero averiguar

Lista_dados= ["C:/Users/maria/OneDrive/ENS5132/Trabalho_02/inputs/merra/MERRA2_400.tavgU_3d_tdt_Np.202412.nc4",
"C:/Users/maria/OneDrive/ENS5132/Trabalho_02/inputs/merra/MERRA2_400.tavgU_3d_tdt_Np.202401.nc4",
"C:/Users/maria/OneDrive/ENS5132/Trabalho_02/inputs/merra/MERRA2_400.tavgU_3d_tdt_Np.202402.nc4",
"C:/Users/maria/OneDrive/ENS5132/Trabalho_02/inputs/merra/MERRA2_400.tavgU_3d_tdt_Np.202403.nc4",
"C:/Users/maria/OneDrive/ENS5132/Trabalho_02/inputs/merra/MERRA2_400.tavgU_3d_tdt_Np.202404.nc4",
"C:/Users/maria/OneDrive/ENS5132/Trabalho_02/inputs/merra/MERRA2_400.tavgU_3d_tdt_Np.202405.nc4",
"C:/Users/maria/OneDrive/ENS5132/Trabalho_02/inputs/merra/MERRA2_400.tavgU_3d_tdt_Np.202406.nc4",
"C:/Users/maria/OneDrive/ENS5132/Trabalho_02/inputs/merra/MERRA2_400.tavgU_3d_tdt_Np.202407.nc4",
"C:/Users/maria/OneDrive/ENS5132/Trabalho_02/inputs/merra/MERRA2_400.tavgU_3d_tdt_Np.202408.nc4",
"C:/Users/maria/OneDrive/ENS5132/Trabalho_02/inputs/merra/MERRA2_400.tavgU_3d_tdt_Np.202409.nc4",
"C:/Users/maria/OneDrive/ENS5132/Trabalho_02/inputs/merra/MERRA2_400.tavgU_3d_tdt_Np.202410.nc4",
"C:/Users/maria/OneDrive/ENS5132/Trabalho_02/inputs/merra/MERRA2_400.tavgU_3d_tdt_Np.202411.nc4"]

# Lista de variáveis de contribuição para análise comparativa
VARIAVEIS_CONTRIBUICAO = [
    'DTDTTOT', 'DTDTANA', 'DTDTDYN', 'DTDTFRI',
    'DTDTGWD', 'DTDTMST', 'DTDTRB', 'DTDTRAD']

PASTA_SAIDA_GRAFICOS= r"C:\Users\maria\OneDrive\ENS5132\Trabalho_02\outputs"
# %% 3. Carregamento e Preparação dos Dados Raster
print("Verificando a existência dos arquivos na lista...")
arquivos_existentes = []
for f in Lista_dados:
    if os.path.exists(f):
        arquivos_existentes.append(f)
        print(f"  - Encontrado: {f}")
    else:
        print(f"  - **AVISO: Não encontrado:** {f}")
if not arquivos_existentes:
    print("ERRO: Nenhum dos arquivos na lista foi encontrado. Verifique os caminhos.")
    ds = None # Define ds como None para a verificação posterior
else:
    try:
        print(f"Tentando abrir {len(arquivos_existentes)} arquivos com xarray.open_mfdataset...")
        ds = xr.open_mfdataset(arquivos_existentes, combine='by_coords', parallel=True)
        print("Datasets combinados com sucesso!")
    except Exception as e:
        print(f"ERRO ao carregar e combinar os datasets: {e}")
        print("Verifique a integridade dos arquivos e a compatibilidade para combinação.")
        ds = None # Garante que ds é None em caso de falha

if ds is None:
    raise RuntimeError("O dataset 'ds' é None. Não foi possível carregar os arquivos .nc4. Verifique a lista de arquivos e a mensagem de erro acima.")
elif VARIAVEL_INTERESSE not in ds:
    raise ValueError(f"A variável '{VARIAVEL_INTERESSE}' não foi encontrada no dataset combinado.")
else:
    print(f"A variável '{VARIAVEL_INTERESSE}' foi encontrada no dataset.")
    print("Primeiras linhas do dataset combinado:")
    print(ds)    
print("Carregando arquivos NetCDF...")
try:
    # Abre múltiplos arquivos de forma eficiente 
    padrao_arquivos = os.path.join(Lista_dados)
    ds = xr.open_mfdataset(padrao_arquivos, combine='by_coords', parallel=True)
    print("Arquivos carregados com sucesso.")
    print("Variáveis disponíveis:", list(ds.data_vars))
except Exception as e:
    print(f"Erro ao carregar os arquivos NetCDF: {e}")

# Seleciona a variável e o nível de pressão de interesse
if VARIAVEL_INTERESSE not in ds:
    raise ValueError(f"A variável '{VARIAVEL_INTERESSE}' não foi encontrada no dataset.")

# Verifica se o dado é 3D (com nível) ou 2D
if 'lev' in ds[VARIAVEL_INTERESSE].coords:
    print(f">>> Selecionando dados para o nível de {NIVEL_PRESSAO} hPa...")
    data_array = ds[VARIAVEL_INTERESSE].sel(lev=NIVEL_PRESSAO)
else:
    data_array = ds[VARIAVEL_INTERESSE]

# %% 4. Recorte Geográfico Preciso com Shapefile
print(">>> Realizando recorte com o shapefile dos municípios do Brasil...")

# Carrega e prepara o shapefile
try:
    shape = gpd.read_file(CAMINHO_SHAPEFILE)
    # Garante que o shapefile esteja no mesmo sistema de coordenadas dos dados (WGS84)
    shape = shape.to_crs("EPSG:4326")
except Exception as e:
    print(f"Erro ao carregar ou processar o shapefile: {e}")

# Adiciona informações geoespaciais ao DataArray do xarray
data_array = data_array.rio.write_crs("EPSG:4326")

# Informa ao rioxarray quais dimensões correspondem a 'x' e 'y'
data_array = data_array.rio.set_spatial_dims(x_dim="lon", y_dim="lat", inplace=True)

# Realiza o recorte (clip)
try:
    data_recortado = data_array.rio.clip(shape.geometry, drop=True)
    print("Recorte geográfico realizado com sucesso.")
except Exception as e:
    print(f"Erro durante o recorte com rioxarray: {e}")
    data_recortado = None

# %% 5. Análise e Visualização dos Dados
if data_recortado is not None:
    # GRÁFICO 1: Série Temporal da Média Espacial
    print(">>> Gerando Gráfico 1: Série Temporal da Média...")
    media_espacial = data_recortado.mean(dim=["lat", "lon"]) * 86400

    try:
        locale.setlocale(locale.LC_TIME, 'pt_BR.UTF-8')
    except locale.Error:
        print("Aviso: Locale 'pt_BR.UTF-8' não encontrado. Usando locale padrão.")
        try:
            locale.setlocale(locale.LC_TIME, '')
        except locale.Error:
            print("Aviso: Nenhum locale pôde ser configurado. As datas podem aparecer em inglês.")

    plt.figure(figsize=(14, 6))
    media_espacial.plot(color='royalblue')
    ax = plt.gca()
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=1))
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b'))
    plt.title(f"Média da Tendência de Temperatura em {NIVEL_PRESSAO} hPa para o Brasil no ano de 2024", fontsize=16, pad=20)
    plt.xlabel("2024", fontsize=12)
    plt.ylabel("Tendência Média (K/dia)", fontsize=12)
    plt.grid(True, linestyle='--', alpha=0.6)
    plt.tight_layout()
    plt.savefig(os.path.join(PASTA_SAIDA_GRAFICOS, "Serie_Temporal_Media_{NIVEL_PRESSAO}.png"), dpi=300, bbox_inches='tight')
    plt.show()
    plt.show()

    # GRÁFICO 2: Padrão Espacial da Média Temporal
    print(">>> Gerando Gráfico 2: Mapa da Tendência Média em ...")
    media_temporal = data_recortado.mean(dim='time') * 86400

    plt.figure(figsize=(10, 8))
    media_temporal.plot(
        cmap='RdBu_r',
        robust=True,
        cbar_kwargs={'label': 'Tendência Média (K/dia)', 'orientation': 'vertical', 'pad': 0.1}
    )
    shape.plot(ax=plt.gca(), facecolor='none', edgecolor='black', linewidth=0.4)
    plt.title(f"Padrão Espacial da Tendência de Temperatura em {NIVEL_PRESSAO} hPa", fontsize=16, pad=20)
    plt.xlabel('Longitude', fontsize=12)
    plt.ylabel('Latitude', fontsize=12)
    plt.grid(True, linestyle='--', alpha=0.3)
    plt.tight_layout()
    plt.savefig(os.path.join(PASTA_SAIDA_GRAFICOS, "Mapa_Tendencia_Media. {NIVEL_PRESSAO} .png"), dpi=300, bbox_inches='tight')
    plt.show()
#%%
# GRÁFICO 3: Comparação das Forçantes Físicas
print(">>> Gerando Gráfico 3: Comparação das Forçantes...")
plt.figure(figsize=(14, 7))

for var_nome in VARIAVEIS_CONTRIBUICAO:
    if var_nome in ds:
        da_contrib = ds[var_nome].sel(lev=NIVEL_PRESSAO)
        da_contrib = da_contrib.rio.write_crs("EPSG:4326").rio.set_spatial_dims(x_dim="lon", y_dim="lat")
        da_contrib_clip = da_contrib.rio.clip(shape.geometry, drop=True)
        media_contrib = da_contrib_clip.mean(dim=["lat", "lon"]) * 86400

        linewidth = 2.5 if var_nome == VARIAVEL_INTERESSE else 1.5
        linestyle = '-'  
        media_contrib.plot(label=var_nome, linewidth=linewidth, linestyle=linestyle)

plt.title(f"Componentes da Tendência de Temperatura em {NIVEL_PRESSAO} hPa (Brasil)", fontsize=16, pad=20)
plt.ylabel("Tendência Média (K/dia)", fontsize=12)
plt.xlabel("Tempo", fontsize=12)

ax = plt.gca()
ax.xaxis.set_major_locator(mdates.MonthLocator(interval=1))
ax.xaxis.set_major_formatter(mdates.DateFormatter('%b'))
plt.xticks(rotation=0)

plt.legend(title="Forçantes Físicas")
plt.grid(True, linestyle='--', alpha=0.6)
plt.tight_layout()

plt.show()

print("Análise Concluída")

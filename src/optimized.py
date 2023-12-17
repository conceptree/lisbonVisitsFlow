import dask.dataframe as dd
import pandas as pd
import os

sample_dir = "../data/datasets/"

# Definir o caminho dos arquivos
quadriculas_path = '../data/wktComplete.csv'
meteorologia_path = '../data/meteorologia.csv'

# Lista de caminhos dos arquivos de amostra
sample_paths = sample_dir+"*.csv"

# Colunas necessárias
colunas_necessarias = ['Grid_ID', 'Datetime', 'C2', 'C4', 'C7', 'D1']

# Especificar os tipos de dados para colunas problemáticas
dtypes = {
    'D1': 'object', 'C9': 'object', 'E2': 'object', 'E3': 'object',
    'C1': 'object', 'C10': 'object', 'C2': 'object', 'C3': 'object', 'C4': 'object',
    'C5': 'object', 'C6': 'object', 'C7': 'object', 'E1': 'object', 'E4': 'object',
    'extract_day': 'object', 'extract_month': 'object', 'extract_year': 'object', 'C11': 'object',
    'E5': 'object', 'Grid_ID': 'object'
}

# Ler os arquivos de amostra usando Dask
sample_dask_df = dd.read_csv(sample_paths, usecols=colunas_necessarias, encoding='ISO-8859-1', assume_missing=True, dtype=dtypes, blocksize='1000MB')

# Converter 'Datetime' para datetime
sample_dask_df['Datetime'] = dd.to_datetime(sample_dask_df['Datetime'], errors='coerce')

# Converter colunas numéricas
colunas_numericas = ['C2', 'C4', 'C7']
for coluna in colunas_numericas:
    sample_dask_df[coluna] = dd.to_numeric(sample_dask_df[coluna], errors='coerce')

# Filtrar os terminais em roaming
sample_dask_df = sample_dask_df[sample_dask_df['C2'] > 0]

# Ler outros arquivos CSV
quadriculas_df = pd.read_csv(quadriculas_path, encoding='ISO-8859-1')
meteorologia_df = pd.read_csv(meteorologia_path, parse_dates=['datetime'], encoding='ISO-8859-1')

# Converter Pandas DataFrames para Dask DataFrames
quadriculas_dask_df = dd.from_pandas(quadriculas_df, npartitions=1)
meteorologia_dask_df = dd.from_pandas(meteorologia_df, npartitions=1)

# Assegurar compatibilidade de tipos para merge
sample_dask_df['Grid_ID'] = sample_dask_df['Grid_ID'].astype('object')
quadriculas_dask_df['grelha_id'] = quadriculas_dask_df['grelha_id'].astype('object')

# Merge com quadriculas
quadriculas_dask_df = quadriculas_dask_df.rename(columns={'grelha_id':'Grid_ID'})
sample_dask_df = sample_dask_df.merge(quadriculas_dask_df[['Grid_ID', 'wkt', 'nome', 'freguesia']], on='Grid_ID', how='left')

# Merge com meteorologia
sample_dask_df['date'] = sample_dask_df['Datetime'].dt.date
meteorologia_dask_df['date'] = meteorologia_dask_df['datetime'].dt.date
sample_dask_df = sample_dask_df.merge(meteorologia_dask_df[['date', 'temp', 'precip']], on='date', how='left')

# Renomear colunas
sample_dask_df = sample_dask_df.rename(columns={'temp': 'temperatura', 'precip': 'chuva'})

# Computar o DataFrame final
final_df = sample_dask_df.compute()

# Selecionar e renomear colunas finais
columns_to_keep = ['Grid_ID', 'Datetime', 'C2', 'C4', 'C7', 'D1', 'nome', 'freguesia', 'wkt', 'temperatura', 'chuva']
final_df = final_df[columns_to_keep]

# Renomear colunas
rename_dict = {
    'Grid_ID': 'Grid_ID', 'Datetime': 'Date_Time', 'C2': 'Roaming_Terminals',
    'C4': 'Remaining_Roaming_Terminals', 'C7': 'Enter_Roaming_Terminals', 'D1': 'Top_10_Countries',
    'nome': 'Grid_Name', 'freguesia': 'Town', 'wkt': 'Geo_Location',
    'temperatura': 'Temperature', 'chuva': 'Precipitation'
}
sample_dask_df = sample_dask_df.rename(columns=rename_dict)

# Ordem desejada das colunas
ordem_desejada = ['Grid_ID', 'Date_Time', 'Roaming_Terminals', 'Remaining_Roaming_Terminals', 'Enter_Roaming_Terminals', 'Top_10_Countries', 'Grid_Name', 'Town', 'Geo_Location', 'Temperature', 'Precipitation']

# Certificar de que todas as colunas desejadas estão presentes
final_df = sample_dask_df[ordem_desejada]

# Computar o DataFrame final
final_df = final_df.compute()

# Salvar o resultado final
output_file_path = os.path.join(os.getcwd(), '../data/all_final.csv')
final_df.to_csv(output_file_path, index=False)

print(f"CSV final criado com sucesso em {output_file_path}")

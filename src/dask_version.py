import dask.dataframe as dd
import pandas as pd
import os

# Definir o caminho dos arquivos
quadriculas_path = '../data/wktComplete.csv'
meteorologia_path = '../data/meteorologia.csv'

# Lista de caminhos dos arquivos de amostra
sample_paths = "../data/datasets/*.csv"

colunas_necessarias = ['Grid_ID', 'Datetime', 'C2', 'C4', 'C7', 'D1']

# Especificar os tipos de dados para colunas problemáticas
dtypes = {'D1': 'object', 'C9': 'object', 'E2': 'object', 'E3': 'object', 'C1': 'object', 'C10': 'object', 'C2': 'object', 'C3': 'object', 'C4': 'object', 'C5': 'object', 'C6': 'object', 'C7': 'object', 'E1': 'object', 'E4': 'object', 'extract_day': 'object', 'extract_month': 'object', 'extract_year': 'object', 'C11': 'object', 'E5': 'object', 'Grid_ID': 'object'}

# Ler os arquivos de amostra usando Dask com os tipos de dados especificados
print("Iniciar leitura de datasets...")
sample_dask_df = dd.read_csv(sample_paths, usecols=colunas_necessarias, encoding='ISO-8859-1', assume_missing=True, dtype=dtypes)

print("Colunas após a leitura inicial:")
print(sample_dask_df.columns)

# Verificar se a coluna 'Datetime' existe e converter para datetime
print("A verificar se Datetime existe...")
if 'Datetime' in sample_dask_df.columns:
    print("Datetime existe! Converter para datetime...")
    sample_dask_df['Datetime'] = dd.to_datetime(sample_dask_df['Datetime'], errors='coerce')
else:
    raise ValueError("A coluna 'Datetime' não foi encontrada no arquivo CSV.")

# Converter colunas para tipos numéricos antes de operações numéricas
print("Iniciar conversão de C2, C4 e C7 para tipos numéricos antes de operações de calculo...")
colunas_numericas = ['C2', 'C4', 'C7']
for coluna in colunas_numericas:
    sample_dask_df[coluna] = dd.to_numeric(sample_dask_df[coluna], errors='coerce')

# Filtrar os terminais em roaming
print("Filtrar coluna C2 para valores acima de zero...")
sample_dask_df = sample_dask_df[sample_dask_df['C2'] > 0]

# Ler os outros arquivos CSV usando Pandas
print("Ler csv de quadriculas...")
quadriculas_df = pd.read_csv(quadriculas_path, encoding='ISO-8859-1')
print("Ler csv de meteorologia...")
meteorologia_df = pd.read_csv(meteorologia_path, parse_dates=['datetime'], encoding='ISO-8859-1')

# Converter os DataFrames do Pandas para Dask DataFrames
print("Converter de panda dataframes para Dask dataframes...")
quadriculas_dask_df = dd.from_pandas(quadriculas_df, npartitions=1)
meteorologia_dask_df = dd.from_pandas(meteorologia_df, npartitions=1)

# Certificar que 'Grid_ID' e 'grelha_id' são do mesmo tipo
print("Forçar Grid_ID para objecto...")
sample_dask_df['Grid_ID'] = sample_dask_df['Grid_ID'].astype('object')
quadriculas_dask_df['grelha_id'] = quadriculas_dask_df['grelha_id'].astype('object')

# Extrair as colunas relevantes da quadricula e fazer merge
print("Extrair grelha_id, wkt, name e freguesia para merge...")
quadriculas_dask_df = quadriculas_dask_df[['grelha_id', 'wkt', 'nome', 'freguesia']]
sample_dask_df = sample_dask_df.merge(quadriculas_dask_df, left_on='Grid_ID', right_on='grelha_id', how='left')

# Assegurar que ambos os campos de data estão no mesmo formato para o merge
print("Forçar Datetime para date type...")
sample_dask_df['date'] = sample_dask_df['Datetime'].dt.date
meteorologia_dask_df['date'] = meteorologia_dask_df['datetime'].dt.date

# Merge com base na coluna 'date'
print("Iniciar merge dos campos date, temp e percip do dataset de meteorologia...")
sample_dask_df = sample_dask_df.merge(meteorologia_dask_df[['date', 'temp', 'precip']], on='date', how='left')

# Renomear colunas
print("Renomear colunas temp e precip...")
sample_dask_df = sample_dask_df.rename(columns={'temp': 'temperatura', 'precip': 'chuva'})

# Funções para cálculos
def calculate_interval_average(row, column):
    hour = row['Datetime'].hour

    try:
        value = float(row[column])  # Tentativa de converter o valor para float
    except ValueError:
        return None  # Retorna None ou algum valor padrão se a conversão falhar

    if 9 <= hour < 13:
        return value / 4  # Média para 9-13h
    elif 13 <= hour < 18:
        return value / 5  # Média para 13-18h
    else:  # 18-24h
        return value / 6  # Média para 18-00h


def count_unique_countries(row):
    # Converter o valor para string antes de tentar dividi-lo
    countries_str = str(row['D1']) if pd.notna(row['D1']) else ''
    countries = countries_str.split(';') if countries_str else []
    unique_countries = set(countries)
    return len(unique_countries)


def calculate_country_average(row):
    hour = row['Datetime'].hour
    count = row['unique_countries']
    if 9 <= hour < 13:
        return count / 4  # Média para 9-13h
    elif 13 <= hour < 18:
        return count / 5  # Média para 13-18h
    else:  # 18-24h
        return count / 6  # Média para 18-00h

# Aplicar funções que requerem Pandas DataFrame
meta_count_unique_countries = ('unique_countries', 'int64')
meta_calculate_country_average = ('average_countries', 'float64')

print("Aplicar formatação dos paises no dataframe...")
sample_dask_df['unique_countries'] = sample_dask_df.apply(count_unique_countries, axis=1, meta=meta_count_unique_countries)
sample_dask_df['average_countries'] = sample_dask_df.apply(calculate_country_average, axis=1, meta=meta_calculate_country_average)

print("Aplicar calculo de intervalos...")
columns_to_average = ['C2', 'C4', 'C7']
for column in columns_to_average:
    sample_dask_df[f'average_{column}'] = sample_dask_df.apply(calculate_interval_average, column=column, axis=1, meta=('x', 'f8'))

# Computar o resultado final para um Pandas DataFrame
print("Iniciar criação de dataframe final...")
final_df = sample_dask_df.compute()

# Selecionar as colunas desejadas para o novo CSV
columns_to_keep = ['Grid_ID', 'Datetime', 'C2', 'C4', 'C7', 'D1', 'nome', 'freguesia', 'wkt', 'temperatura', 'chuva'] + [f'average_{column}' for column in columns_to_average] + ['unique_countries', 'average_countries'] 
final_df = final_df[columns_to_keep]

# Renomear colunas para algo mais semântico
print("Renomear colunas...")
final_df = final_df.rename(columns={
    'Grid_ID': 'Grid_ID',
    'Datetime': 'Date_Time',
    'C2': 'Roaming_Terminals',
    'C4': 'Remaining_Roaming_Terminals',
    'C7': 'Enter_Roaming_Terminals',
    'D1': 'Top_10_Countries',
    'nome': 'Grid_Name',
    'freguesia': 'Town',
    'wkt': 'Geo_Location',
    'temperatura': 'Temperature',
    'chuva': 'Precipitation',
    'average_C2':'Average_Unique_Roaming',
    'average_C4':'Average_Remaining_Roaming',
    'average_C7':'Average_Entries_Roaming'
})

# Salvar o resultado final
print("concatenar datasets num só...")
output_file_path = os.path.join(os.getcwd(), '../data/final.csv')
final_df.to_csv(output_file_path, index=False)

print(f"CSV final criado com sucesso em {output_file_path}")

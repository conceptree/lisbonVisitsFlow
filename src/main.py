import pandas as pd
import os

# Definir o caminho dos arquivos
quadriculas_path = '../data/wktComplete.csv'
meteorologia_path = '../data/meteorologia.csv'

chunk_size = 10000  # Ajustar conforme a necessidade de memória onde o script é corrido

# Lista de caminhos dos arquivos de amostra
sample_paths = []
months = ["June", "July", "August"]
year = "2022"
for month in months:
    if month == "January":
        year = "2023"
    sample_paths.append("../data/datasets/"+month+year+".csv")

print(sample_paths)
# Lista para armazenar os DataFrames
dataframes = []  # List to store processed chunks

for sample_path in sample_paths:
    for chunk in pd.read_csv(sample_path, encoding='ISO-8859-1', chunksize=chunk_size, low_memory=False):
        # Check if 'Datetime' column exists in the current chunk
        if 'Datetime' in chunk.columns:
            chunk['Datetime'] = pd.to_datetime(chunk['Datetime'], utc=True)
        else:
            raise ValueError("A coluna 'Datetime' não foi encontrada no arquivo CSV.")

        # Apply any additional processing needed for each chunk
        # For example, filtering based on 'Datetime' and 'C2'
        chunk = chunk[(chunk['Datetime'].dt.hour >= 9) & (chunk['Datetime'].dt.hour < 24) & (chunk['C2'] > 0)]

        # Append the processed chunk to the list
        dataframes.append(chunk)


# Concatenar todos os DataFrames em um único
sample_df = pd.concat(dataframes, ignore_index=True)
quadriculas_df = pd.read_csv(quadriculas_path, encoding='ISO-8859-1')
meteorologia_df = pd.read_csv(meteorologia_path, parse_dates=['datetime'], encoding='ISO-8859-1')

# Extrair as colunas relevantes da quadricula
quadriculas_df = quadriculas_df[['grelha_id', 'wkt', 'nome', 'freguesia']]
sample_df = sample_df.merge(quadriculas_df, left_on='Grid_ID', right_on='grelha_id', how='left')

# Assegurar que ambos os campos de data estão no mesmo formato para o merge
sample_df['date'] = sample_df['Datetime'].dt.date
meteorologia_df['date'] = meteorologia_df['datetime'].dt.date

# Agora podemos fazer o merge com base na coluna 'date' que é do mesmo tipo nos dois DataFrames
sample_df = sample_df.merge(meteorologia_df[['date', 'temp', 'precip']], on='date', how='left')

# Renomear as colunas de temperatura e precipitação
sample_df.rename(columns={'temp': 'temperatura', 'precip': 'chuva'}, inplace=True)

def calculate_interval_average(row, column):
    hour = row['Datetime'].hour
    value = float(row[column])  # Converte o valor para float
    if 9 <= hour < 13:
        return value / 4  # Média para 9-13h
    elif 13 <= hour < 18:
        return value / 5  # Média para 13-18h
    else:  # 18-24h
        return value / 6  # Média para 18-00h

# Colunas para as quais queremos calcular a média (excluindo D1)
columns_to_average = ['C2', 'C4', 'C7']

# Certifique-se de que as colunas são numéricas
for column in columns_to_average:
    sample_df[column] = sample_df[column].astype(float)

# Aplicar a função para criar as novas colunas de média
for column in columns_to_average:
    sample_df[f'average_{column}'] = sample_df.apply(calculate_interval_average, column=column, axis=1)

# Criar os nomes das novas colunas de média
average_columns = [f'average_{column}' for column in columns_to_average]

def count_unique_countries(row):
    countries = row['D1'].split(';') if pd.notna(row['D1']) else []
    unique_countries = set(countries)
    return len(unique_countries)

# Aplicar a função para contar países únicos
sample_df['unique_countries'] = sample_df.apply(count_unique_countries, axis=1)

def calculate_country_average(row):
    hour = row['Datetime'].hour
    count = row['unique_countries']
    if 9 <= hour < 13:
        return count / 4  # Média para 9-13h
    elif 13 <= hour < 18:
        return count / 5  # Média para 13-18h
    else:  # 18-24h
        return count / 6  # Média para 18-00h

# Calcular a média de países únicos para cada intervalo de tempo
sample_df['average_countries'] = sample_df.apply(calculate_country_average, axis=1)

# Selecionar as colunas desejadas para o novo CSV
columns_to_keep = ['Grid_ID', 'Datetime', 'C2', 'C4', 'C7', 'D1', 'nome', 'freguesia', 'wkt', 'temperatura', 'chuva'] + average_columns + ['unique_countries', 'average_countries'] 
final_df = sample_df[columns_to_keep]

# Renomear columnas para algo mais semantico
final_df.rename(columns={
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
}, inplace=True)

# Imprimir as informações do DataFrame final
print(final_df.info())

# Obter o caminho da pasta raiz
root_folder = os.getcwd()

# Caminho completo para o novo arquivo CSV
output_file_path = os.path.join(root_folder, '../data/final.csv')

# Salvar o novo dataframe em um CSV na pasta raiz
final_df.to_csv(output_file_path, index=False)

print(f"CSV final criado com sucesso em {output_file_path}")

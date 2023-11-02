import pandas as pd
import os

# Definir o caminho dos arquivos
sample_path = '../data/sample_June2022.csv'
quadriculas_path = '../data/wktComplete.csv'
meteorologia_path = '../data/meteoJune2022June2023.csv'

# Carregar os dados dos CSVs com a codificação correta e suprimir o aviso de baixa memória
sample_df = pd.read_csv(sample_path, parse_dates=['Datetime'], encoding='ISO-8859-1', low_memory=False)
quadriculas_df = pd.read_csv(quadriculas_path, encoding='ISO-8859-1')
meteorologia_df = pd.read_csv(meteorologia_path, parse_dates=['datetime'], encoding='ISO-8859-1')

# Filtrar os terminais em roaming e no intervalo de tempo desejado
sample_df['Datetime'] = pd.to_datetime(sample_df['Datetime'])
sample_df = sample_df[(sample_df['Datetime'].dt.hour >= 9) & (sample_df['Datetime'].dt.hour <= 24)]
sample_df = sample_df[sample_df['C2'] > 0]  # C2 é o n.º de terminais distintos, em roaming

# Extrair as coordenadas da quadricula
quadriculas_df = quadriculas_df[['grelha_id', 'latitude', 'longitude']]
sample_df = sample_df.merge(quadriculas_df, left_on='Grid_ID', right_on='grelha_id', how='left')

# Filtrar os dados meteorológicos para o intervalo de datas do dataframe sample
meteorologia_df = meteorologia_df.set_index('datetime')
sample_df['date'] = sample_df['Datetime'].dt.date
sample_df = sample_df.join(meteorologia_df[['temp', 'precip']], on='date', how='left')

# Renomear as colunas de temperatura e precipitação
sample_df.rename(columns={'temp': 'temperatura', 'precip': 'chuva'}, inplace=True)

# Selecionar as colunas desejadas para o novo CSV
columns_to_keep = ['Grid_ID', 'Datetime', 'C2', 'latitude', 'longitude', 'temperatura', 'chuva']
final_df = sample_df[columns_to_keep]

# Obter o caminho da pasta raiz
root_folder = os.getcwd()  # Isso obtém o diretório de trabalho atual

# Caminho completo para o novo arquivo CSV
output_file_path = os.path.join(root_folder, 'resultado_final.csv')

# Salvar o novo dataframe em um CSV na pasta raiz
final_df.to_csv(output_file_path, index=False)

print(f"CSV final criado com sucesso em {output_file_path}!")

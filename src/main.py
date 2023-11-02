import pandas as pd
import os

# Definir o caminho dos arquivos
sample_path = '../data/sample_June2022.csv'
quadriculas_path = '../data/wktComplete.csv'
meteorologia_path = '../data/meteorologia.csv'

# Carregar os dados dos CSVs com a codificação correta e suprimir o aviso de baixa memória
sample_df = pd.read_csv(sample_path, encoding='ISO-8859-1', low_memory=False)
quadriculas_df = pd.read_csv(quadriculas_path, encoding='ISO-8859-1')
meteorologia_df = pd.read_csv(meteorologia_path, parse_dates=['datetime'], encoding='ISO-8859-1')

# Verificar se a coluna 'Datetime' existe e converter para datetime
if 'Datetime' in sample_df.columns:
    sample_df['Datetime'] = pd.to_datetime(sample_df['Datetime'])
else:
    raise ValueError("A coluna 'Datetime' não foi encontrada no arquivo CSV.")

# Filtrar os terminais em roaming e no intervalo de tempo desejado
sample_df = sample_df[(sample_df['Datetime'].dt.hour >= 9) & (sample_df['Datetime'].dt.hour < 24)]
sample_df = sample_df[sample_df['C2'] > 0]  # C2 é o n.º de terminais distintos, em roaming

# Extrair a coluna wkt da quadricula
quadriculas_df = quadriculas_df[['grelha_id', 'wkt']]
sample_df = sample_df.merge(quadriculas_df, left_on='Grid_ID', right_on='grelha_id', how='left')

# Assegurar que ambos os campos de data estão no mesmo formato para o merge
sample_df['date'] = sample_df['Datetime'].dt.date
meteorologia_df['date'] = meteorologia_df['datetime'].dt.date

# Agora podemos fazer o merge com base na coluna 'date' que é do mesmo tipo nos dois DataFrames
sample_df = sample_df.merge(meteorologia_df[['date', 'temp', 'precip']], on='date', how='left')

# Renomear as colunas de temperatura e precipitação
sample_df.rename(columns={'temp': 'temperatura', 'precip': 'chuva'}, inplace=True)

# Selecionar as colunas desejadas para o novo CSV
columns_to_keep = ['Grid_ID', 'Datetime', 'C2', 'wkt', 'temperatura', 'chuva']
final_df = sample_df[columns_to_keep]

# Obter o caminho da pasta raiz
root_folder = os.getcwd()  # Isso obtém o diretório de trabalho atual

# Caminho completo para o novo arquivo CSV
output_file_path = os.path.join(root_folder, '../data/resultado_final.csv')

# Salvar o novo dataframe em um CSV na pasta raiz
final_df.to_csv(output_file_path, index=False)

print(f"CSV final criado com sucesso em {output_file_path}!")

import dask.dataframe as dd
import pandas as pd
import os
import glob

# Definindo os caminhos para os arquivos e diretórios
sample_dir = "../data/datasets/"
quadriculas_path = '../data/wktComplete.csv'
meteorologia_path = '../data/meteorologia.csv'
sample_paths = sample_dir+"*.csv"

# Colunas necessárias para a análise
colunas_necessarias = ['Grid_ID', 'Datetime', 'C2', 'C4', 'C7', 'D1']

# Tipos de dados para cada coluna para otimizar a leitura
# Especificações dos tipos de dados para as colunas
dtypes = {
    'D1': 'object', 'C9': 'object', 'E2': 'object', 'E3': 'object',
    'C1': 'object', 'C10': 'object', 'C2': 'object', 'C3': 'object', 'C4': 'object',
    'C5': 'object', 'C6': 'object', 'C7': 'object', 'E1': 'object', 'E4': 'object',
    'extract_day': 'object', 'extract_month': 'object', 'extract_year': 'object', 'C11': 'object',
    'E5': 'object', 'Grid_ID': 'object'
}

# Carregando dados de quadriculas e meteorologia usando o pandas
quadriculas_df = pd.read_csv(quadriculas_path, encoding='ISO-8859-1')
meteorologia_df = pd.read_csv(meteorologia_path, parse_dates=['datetime'], encoding='ISO-8859-1')

# Convertendo DataFrames do pandas para DataFrames do Dask para processamento paralelo
quadriculas_dask_df = dd.from_pandas(quadriculas_df, npartitions=1)
meteorologia_dask_df = dd.from_pandas(meteorologia_df, npartitions=1)

# Preparação do DataFrame de quadriculas
quadriculas_dask_df['grelha_id'] = quadriculas_dask_df['grelha_id'].astype('int64')
quadriculas_dask_df = quadriculas_dask_df.rename(columns={'grelha_id':'Grid_ID'})

# Lista de IDs de grelhas para filtragem
grids_to_filter = [683, 6, 3668, ...]  # Completa lista de grelhas

# Encontrando arquivos de datasets específicos
arquivos_csv = glob.glob(os.path.join(sample_dir, "July2022.csv"))

for arquivo in arquivos_csv:
    # Obtendo o nome base do arquivo para uso no nome do arquivo de saída
    nome_arquivo_base = os.path.splitext(os.path.basename(arquivo))[0]
    
    # Lendo o dataset com as colunas necessárias e convertendo os tipos
    sample_dask_df = dd.read_csv(arquivo, usecols=colunas_necessarias, encoding='ISO-8859-1', assume_missing=True, dtype=dtypes, blocksize='1000MB')

    # Convertendo a coluna 'Datetime' para o tipo datetime e arredondando para a hora mais próxima
    sample_dask_df['Datetime'] = dd.to_datetime(sample_dask_df['Datetime'], errors='coerce')
    sample_dask_df['Rounded_Datetime'] = sample_dask_df['Datetime'].dt.floor('H')

    # Removendo duplicatas baseadas em 'Rounded_Datetime' e 'Grid_ID'
    sample_dask_df = sample_dask_df.drop_duplicates(subset=['Rounded_Datetime', 'Grid_ID'])

    # Convertendo colunas numéricas
    colunas_numericas = ['C2', 'C4', 'C7']
    for coluna in colunas_numericas:
        sample_dask_df[coluna] = dd.to_numeric(sample_dask_df[coluna], errors='coerce')

    # Filtrando registros com valor maior que 0 na coluna 'C2'
    sample_dask_df = sample_dask_df[sample_dask_df['C2'] > 0]

    # Filtrando pelo 'Grid_ID'
    sample_dask_df['Grid_ID'] = sample_dask_df['Grid_ID'].astype('int64')
    sample_dask_df = sample_dask_df[sample_dask_df['Grid_ID'].isin(grids_to_filter)]

    # Mesclando com informações de quadriculas
    sample_dask_df = sample_dask_df.merge(quadriculas_dask_df[['Grid_ID', 'freguesia', 'nome']], on='Grid_ID', how='left')

    # Mesclando com dados de meteorologia
    sample_dask_df['date'] = sample_dask_df['Datetime'].dt.date
    meteorologia_dask_df['date'] = meteorologia_dask_df['datetime'].dt.date
    sample_dask_df = sample_dask_df.merge(meteorologia_dask_df[['date', 'temp', 'precip']], on='date', how='left')

    # Renomeando colunas para clareza
    sample_dask_df = sample_dask_df.rename(columns={
        'temp': 'temperatura', 'precip': 'chuva',
        # Outras renomeações conforme necessário
    })

    # Convertendo o DataFrame do Dask para o pandas
    final_df = sample_dask_df.compute()

    # Selecionando e reordenando colunas finais
    columns_to_keep = ['Grid_ID', 'Datetime', 'C2', 'C4', 'C7', 'D1', 'nome', 'freguesia', 'temperatura', 'chuva']
    final_df = final_df[columns_to_keep]

    # Renomeando colunas finais
    rename_dict = {
        'Grid_ID': 'Grid_ID', 'Datetime': 'Date_Time', 'C2': 'Roaming_Terminals',
        'C4': 'Remaining_Roaming_Terminals', 'C7': 'Enter_Roaming_Terminals', 'D1': 'Top_10_Countries',
        'nome': 'Grid_Name', 'freguesia': 'Town',
        'temperatura': 'Temperature', 'chuva': 'Precipitation'
    }
    final_df = final_df.rename(columns=rename_dict)

    # Definindo a ordem desejada das colunas
    ordem_desejada = ['Grid_ID', 'Date_Time', 'Roaming_Terminals', 'Remaining_Roaming_Terminals', 'Enter_Roaming_Terminals', 'Top_10_Countries', 'Grid_Name', 'Town', 'Temperature', 'Precipitation']
    final_df = final_df[ordem_desejada]

    # Gravando o DataFrame final em um arquivo CSV
    output_file_path = os.path.join(os.getcwd(), f'../data/singles/{nome_arquivo_base}_optimized.csv')
    final_df.to_csv(output_file_path, index=False)

    print(f"CSV final criado com sucesso em {output_file_path}")

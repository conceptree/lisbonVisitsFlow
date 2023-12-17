import pandas as pd
import glob
import os

# Caminho para os datasets
caminho_datasets = '../data/datasets/*.csv'

# Colunas necessárias e a ordem desejada
colunas_necessarias = ['Grid_ID', 'Datetime', 'C2', 'C4', 'C7', 'D1']

# Encontrar todos os arquivos CSV no diretório especificado
arquivos_csv = glob.glob(caminho_datasets)

# Listas para armazenar informações sobre arquivos problemáticos
arquivos_problematicos = []
arquivos_com_ordem_incorreta = []

# Verificar cada arquivo
for arquivo in arquivos_csv:
    try:
        # Ler apenas os cabeçalhos para verificar as colunas
        df = pd.read_csv(arquivo, nrows=0)
        colunas_atuais = df.columns.tolist()
        colunas_faltando = [coluna for coluna in colunas_necessarias if coluna not in colunas_atuais]
        ordem_incorreta = colunas_atuais != colunas_necessarias

        if colunas_faltando:
            arquivos_problematicos.append((os.path.basename(arquivo), colunas_faltando))
        elif ordem_incorreta:
            arquivos_com_ordem_incorreta.append((os.path.basename(arquivo), colunas_atuais))

    except Exception as e:
        print(f"Erro ao processar o arquivo {arquivo}: {e}")

# Exibir relatório
if arquivos_problematicos or arquivos_com_ordem_incorreta:
    print("Foram encontrados problemas nos seguintes arquivos:")

    if arquivos_problematicos:
        print("\nArquivos com colunas faltando:")
        for arquivo, colunas in arquivos_problematicos:
            print(f"Arquivo: {arquivo}, Colunas faltando: {colunas}")

    if arquivos_com_ordem_incorreta:
        print("\nArquivos com ordem de colunas incorreta:")
        for arquivo, ordem_atual in arquivos_com_ordem_incorreta:
            print(f"Arquivo: {arquivo}, Ordem atual: {ordem_atual}")

else:
    print("Todos os arquivos contêm as colunas necessárias e na ordem correta.")

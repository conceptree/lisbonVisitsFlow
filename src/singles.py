import dask.dataframe as dd
import pandas as pd
import os
import glob

sample_dir = "../data/datasets/"

quadriculas_path = '../data/wktComplete.csv'
meteorologia_path = '../data/meteorologia.csv'
sample_paths = sample_dir+"*.csv"
colunas_necessarias = ['Grid_ID', 'Datetime', 'C2', 'C4', 'C7', 'D1']
dtypes = {
    'D1': 'object', 'C9': 'object', 'E2': 'object', 'E3': 'object',
    'C1': 'object', 'C10': 'object', 'C2': 'object', 'C3': 'object', 'C4': 'object',
    'C5': 'object', 'C6': 'object', 'C7': 'object', 'E1': 'object', 'E4': 'object',
    'extract_day': 'object', 'extract_month': 'object', 'extract_year': 'object', 'C11': 'object',
    'E5': 'object', 'Grid_ID': 'object'
}

quadriculas_df = pd.read_csv(quadriculas_path, encoding='ISO-8859-1')
meteorologia_df = pd.read_csv(meteorologia_path, parse_dates=['datetime'], encoding='ISO-8859-1')

quadriculas_dask_df = dd.from_pandas(quadriculas_df, npartitions=1)
meteorologia_dask_df = dd.from_pandas(meteorologia_df, npartitions=1)

quadriculas_dask_df['grelha_id'] = quadriculas_dask_df['grelha_id'].astype('int64')
quadriculas_dask_df = quadriculas_dask_df.rename(columns={'grelha_id':'Grid_ID'})

grids_to_filter = [683, 6, 3668, 3351, 3450, 3448, 3402, 3495, 2590, 2591, 2589, 2461, 5, 2090, 1838, 2026, 567, 568, 676, 3688, 3740, 3718, 3702, 3703, 3742, 3571, 2774, 2654, 1189, 1187, 1307, 1366, 1608, 519, 631, 638, 407, 466, 3189, 3245, 3685, 3732, 3739, 3715, 3719, 2652, 2715, 2837, 2896, 2955, 2956, 1482, 1599, 2025, 2836, 1901, 682, 793, 3600, 3572, 3400, 3650, 3669, 2525, 3016, 1070, 1129, 518, 19, 1253, 576, 635, 412, 624, 734, 685, 851, 3227, 3228, 3172, 3493, 3649, 3569, 3731, 3686, 2460, 2396, 2587, 2714, 2653, 2651, 2835, 2775, 2954, 1130, 1131, 1247, 1246, 1188, 1424, 1426, 1365, 1541, 1839, 1900, 577, 18, 7, 88, 1252, 1251, 467, 628, 408, 464, 686, 3492, 3449, 3399, 3447, 3626, 3672, 3535, 3531, 3533, 3684, 3716, 2996, 1306, 1363, 1543, 1542, 1600, 87, 86, 17, 355, 356, 357, 3054, 411, 514, 633, 1193, 1194, 563, 620, 619, 580, 3568, 3599, 3603, 3532, 3700, 3704, 3738, 2526, 2527, 2592, 2522, 2713, 1071, 1962, 1899, 3353, 3131, 3074, 3055, 3114, 2995, 1485, 520, 570, 1135, 1192, 1312, 1607, 634, 575, 523, 625, 579, 3352, 3171, 3687, 3701, 3733, 3730, 3717, 3743, 3729, 2650, 2776, 1483, 1601, 358, 4, 1963, 678, 677, 735, 3652, 3670, 3491, 3401, 3570, 2528, 2524, 2586, 2588, 2523, 2895, 3113, 3015, 1128, 1186]

arquivos_csv = glob.glob(os.path.join(sample_dir, "July2022.csv"))

for arquivo in arquivos_csv:
    
    nome_arquivo_base = os.path.splitext(os.path.basename(arquivo))[0]
    
    sample_dask_df = dd.read_csv(arquivo, usecols=colunas_necessarias, encoding='ISO-8859-1', assume_missing=True, dtype=dtypes, blocksize='1000MB')

    sample_dask_df['Datetime'] = dd.to_datetime(sample_dask_df['Datetime'], errors='coerce')

    # Round down the 'Datetime' to the nearest hour
    sample_dask_df['Rounded_Datetime'] = sample_dask_df['Datetime'].dt.floor('H')

    # Drop duplicates based on the 'Rounded_Datetime'
    sample_dask_df = sample_dask_df.drop_duplicates(subset=['Rounded_Datetime', 'Grid_ID'])

    colunas_numericas = ['C2', 'C4', 'C7']
    
    for coluna in colunas_numericas:
        sample_dask_df[coluna] = dd.to_numeric(sample_dask_df[coluna], errors='coerce')

    sample_dask_df = sample_dask_df[sample_dask_df['C2'] > 0]

    sample_dask_df['Grid_ID'] = sample_dask_df['Grid_ID'].astype('int64')
    
    sample_dask_df = sample_dask_df[sample_dask_df['Grid_ID'].isin(grids_to_filter)]

    sample_dask_df = sample_dask_df.merge(quadriculas_dask_df[['Grid_ID', 'freguesia', 'nome']], on='Grid_ID', how='left')

    sample_dask_df['date'] = sample_dask_df['Datetime'].dt.date
    meteorologia_dask_df['date'] = meteorologia_dask_df['datetime'].dt.date
    sample_dask_df = sample_dask_df.merge(meteorologia_dask_df[['date', 'temp', 'precip']], on='date', how='left')

    sample_dask_df = sample_dask_df.rename(columns={'temp': 'temperatura', 'precip': 'chuva'})

    final_df = sample_dask_df.compute()

    columns_to_keep = ['Grid_ID', 'Datetime', 'C2', 'C4', 'C7', 'D1', 'nome', 'freguesia', 'temperatura', 'chuva']
    final_df = final_df[columns_to_keep]

    rename_dict = {
        'Grid_ID': 'Grid_ID', 'Datetime': 'Date_Time', 'C2': 'Roaming_Terminals',
        'C4': 'Remaining_Roaming_Terminals', 'C7': 'Enter_Roaming_Terminals', 'D1': 'Top_10_Countries',
        'nome': 'Grid_Name', 'freguesia': 'Town',
        'temperatura': 'Temperature', 'chuva': 'Precipitation'
    }
    sample_dask_df = sample_dask_df.rename(columns=rename_dict)

    ordem_desejada = ['Grid_ID', 'Date_Time', 'Roaming_Terminals', 'Remaining_Roaming_Terminals', 'Enter_Roaming_Terminals', 'Top_10_Countries', 'Grid_Name', 'Town', 'Temperature', 'Precipitation']

    final_df = sample_dask_df[ordem_desejada]

    final_df = final_df.compute()

    output_file_path = os.path.join(os.getcwd(), f'../data/singles/{nome_arquivo_base}_optimized.csv')
    final_df.to_csv(output_file_path, index=False)

    print(f"CSV final criado com sucesso em {output_file_path}")

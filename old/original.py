import pandas as pd
import re

directory = r'C:\Users\vinic\Downloads\Data'

MONTH_RANGE = [*range(1, 13)]

WORKING_FILES = [
    {
        "month": 1,
        "file_name": 'planilha_202301.csv',
    },
    {
        "month": 2,
        "file_name": 'planilha_202302.csv',
    },
    {
        "month": 3,
        "file_name": 'planilha_202303.csv',
    },
    {
        "month": 4,
        "file_name": 'planilha_202304.csv',
    },
    {
        "month": 5,
        "file_name": 'planilha_202305.csv',
    },
    {
        "month": 6,
        "file_name": 'planilha_202306.csv',
    },
    {
        "month": 7,
        "file_name": 'planilha_202307.csv',
    },
    {
        "month": 8,
        "file_name": 'planilha_202308.csv',
    },
    {
        "month": 9,
        "file_name": 'planilha_202309.csv',
    },
    {
        "month": 10,
        "file_name": 'planilha_202310.csv',
    },
    {
        "month": 11,
        "file_name": 'planilha_202311.csv',
    },
    {
        "month": 12,
        "file_name": 'planilha_202312.csv',
    },
]

desired_cols = [
    'carteira_ativa',
    'carteira_inadimplida_arrastada',
    'ativo_problematico'
]

def format_number(value):
    if value < 1e3:
        return f"{value:.2f}"
    elif value < 1e6:
        return f"{value / 1e3:.2f} K"
    elif value < 1e9:
        return f"{value / 1e6:.2f} M"
    elif value < 1e12:
        return f"{value / 1e9:.2f} B"
    else:
        return f"{value / 1e12:.2f} T"

metrics = {
    month: {
        'sr': {},
        'uf': {},
        'porte': {},
        'ocupacao': {},
        'modalidade': {},
    } for month in MONTH_RANGE
}

def convert_to_float(value):
    pattern = r'\s|R\$'
    # Use re.sub() to replace the matched patterns with an empty string
    value = re.sub(pattern, '', value)

    # Handle the case where 'R$ -' should be treated as 0
    if value == '-':
        return 0.0

    # Replace commas with periods for decimal conversion
    # 3.500.452,32
    value=value.replace('.','')
    value = value.replace(',', '.')

    # Convert the cleaned string to float
    return float(value)

def init_app():
    for asset in WORKING_FILES:
        month = asset['month'] # 1 - 12
        filename = asset['file_name']

        print(f"=> Reading file '{filename}'\n")

        data_frame = pd.read_csv(filename, delimiter=';')

        # Filter out PJ clients
        data_frame = data_frame[data_frame['cliente'] == 'PF']

        for _, row in data_frame.iterrows():
            ocupacao = row['ocupacao']      # Eg. PF - Aposentado/pensionista
            modalidade = row['modalidade']  # Eg. PF - Cartão de crédito

            if ocupacao not in metrics[month]['ocupacao']:
                metrics[month]['ocupacao'][ocupacao] = {}

            if modalidade not in metrics[month]['ocupacao'][ocupacao]:
                # metrics[12]['ocupacao']['PF - Aposentado/pensionista']['PF - Cartão de crédito']
                metrics[month]['ocupacao'][ocupacao][modalidade] = {
                    'ativo_problematico': 0,
                    'carteira_ativa': 0
                }


            to_sum_columns = [
                'ativo_problematico',
                'carteira_ativa'
            ]

            for column in to_sum_columns:
                if row[column]:
                    row[column] = convert_to_float(row[column])
                    metrics[month]['ocupacao'][ocupacao][modalidade][column] += row[column]

        print("Finished reading file\n")
        print("=" * 30, "\n")

        print("Credit amount contracted by occupation in each modality:")

        credit_by_occupation = {}

    for month in MONTH_RANGE:
        for occupation, modalities in metrics[month]['ocupacao'].items():
            if occupation not in credit_by_occupation:
                credit_by_occupation[occupation] = {}

            for modality, values in modalities.items():
                if modality not in credit_by_occupation[occupation]:
                    credit_by_occupation[occupation][modality] = 0

                credit_by_occupation[occupation][modality] += values['carteira_ativa']

    for occupation, modalities in credit_by_occupation.items():
        print(f"\nOccupation: {occupation}")

        for modality, amount in modalities.items():
            print(f"{modality}: Carteira Ativa = {format_number(amount)}")

init_app()
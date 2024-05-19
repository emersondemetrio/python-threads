import re
import time
import threading
import pandas as pd

start_time = time.time()

MONTH_RANGE = [*range(1, 13)]

WORKING_FILES = [
    {
        "month": 10,
        "file_name": 'file1.csv',
    },
    {
        "month": 11,
        "file_name": 'file2.csv',
    },
    {
        "month": 12,
        "file_name": 'file3.csv',
    },
]

def convert_to_float(value):
    pattern = r'\s|R\$'

    value = re.sub(pattern, '', value)

    if value == '-':
        return 0.0

    value = value.replace('.','')
    value = value.replace(',', '.')

    return float(value)

CHUNK_SIZE = 1000

def format_to_currency(value):
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

GLOBAL_METRICS = {
    month: {
        'sr': {},
        'uf': {},
        'porte': {},
        'ocupacao': {},
        'modalidade': {},
    } for month in MONTH_RANGE
}

SUM_UP_COLUMNS = [
    'ativo_problematico',
    'carteira_ativa'
]

global_lock = threading.Lock()

def process_chunk(data_frame, month):
    global GLOBAL_METRICS

    for _, row in data_frame.iterrows():
        ocupacao = row['ocupacao']
        modalidade = row['modalidade']

        if ocupacao not in GLOBAL_METRICS[month]['ocupacao']:
            GLOBAL_METRICS[month]['ocupacao'][ocupacao] = {}

        if modalidade not in GLOBAL_METRICS[month]['ocupacao'][ocupacao]:
            GLOBAL_METRICS[month]['ocupacao'][ocupacao][modalidade] = {
                'ativo_problematico': 0,
                'carteira_ativa': 0
            }

        with global_lock:
            for column in SUM_UP_COLUMNS:
                if row[column]:
                    row[column] = convert_to_float(row[column])
                    GLOBAL_METRICS[month]['ocupacao'][ocupacao][modalidade][column] += row[column]

def create_process_chunk_threads(file_path, month):
    chunk_threads = []

    for chunk in pd.read_csv(f"./data/{file_path}", sep=";", chunksize=CHUNK_SIZE):
        ct = threading.Thread(target=process_chunk, args=(chunk, month))
        chunk_threads.append(ct)
        ct.start()

    print(f"Processing thread chunks for {file_path}...")

    for ct in chunk_threads:
        ct.join()


file_threads = []

for asset in WORKING_FILES:
    t = threading.Thread(
        target=create_process_chunk_threads,
        args=(asset["file_name"], asset["amonth"])
    )

    file_threads.append(t)
    t.start()

print("Processing files...")

for ft in file_threads:
    ft.join()

def sumarize():
    credit_by_occupation = {}
    for month in MONTH_RANGE:
        for occupation, modalities in GLOBAL_METRICS[month]['ocupacao'].items():
            if occupation not in credit_by_occupation:
                credit_by_occupation[occupation] = {}

            for modality, values in modalities.items():
                if modality not in credit_by_occupation[occupation]:
                    credit_by_occupation[occupation][modality] = 0

                credit_by_occupation[occupation][modality] += values['carteira_ativa']

    for occupation, modalities in credit_by_occupation.items():
        print(f"\nOccupation: {occupation}")

        for modality, amount in modalities.items():
            print(f"{modality}: Carteira Ativa = {format_to_currency(amount)}")

sumarize()

elapsed_time = time.time() - start_time
hours, rem = divmod(elapsed_time, 3600)
minutes, seconds = divmod(rem, 60)
print("-----" * 40, "\n")
print("--- %d hours, %d minutes and %.2f seconds ---" % (hours, minutes, seconds))
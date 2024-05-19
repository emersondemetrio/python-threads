import time
import threading
import pandas as pd

from constants import CHUNK_SIZE, MONTH_RANGE, SUM_UP_COLUMNS, WORKING_FILES
from utils import show_elapsed_time, to_float, to_currency

start_time = time.time()
global_lock = threading.Lock()

GLOBAL_METRICS = {
    month: {
        'sr': {},
        'uf': {},
        'porte': {},
        'ocupacao': {},
        'modalidade': {},
    } for month in MONTH_RANGE
}

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
                    row[column] = to_float(row[column])
                    GLOBAL_METRICS[month]['ocupacao'][ocupacao][modalidade][column] += row[column]

def create_process_chunk_threads(file_path, month):
    chunk_threads = []

    tcount = 0
    for chunk in pd.read_csv(f"./data/{file_path}", sep=";", chunksize=CHUNK_SIZE):
        tcount += 1
        ct = threading.Thread(target=process_chunk, args=(chunk, month))
        chunk_threads.append(ct)
        ct.start()
        print(f"=> New thread for {file_path} [{tcount}]")

    print(f"==> {tcount} threads created for {file_path}. Processing...\n")

    for ct in chunk_threads:
        ct.join()

def process_files(assets):
    print(f"=> Processing files... ({len(WORKING_FILES)})", )

    file_threads = []

    for asset in assets:
        t = threading.Thread(
            target=create_process_chunk_threads,
            args=(asset["file_name"], asset["month"])
        )

        file_threads.append(t)
        t.start()


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
        print(f"\nOccupation: {'Outros' if occupation == '-' else occupation}")

        for modality, amount in modalities.items():
            print(f"{modality}: Carteira Ativa = {to_currency(amount)}")

process_files(WORKING_FILES)
sumarize()
show_elapsed_time(start_time)

import os, json, boto3
from functions import get_app_version, cnpj_fixing
from time import sleep
import requests, random
from io import StringIO
import pandas as pd

SLEEP_TIME_RANGE = (1, 3)

def load_cnpjs_from_txt(filename):
    s3 = boto3.client('s3')

    # Parâmetros do arquivo
    bucket_name = 'drivalake'
    key = f'perdcomp/src/{filename}'

    # Faz a leitura do arquivo no S3
    response = s3.get_object(Bucket=bucket_name, Key=key)
    content = response['Body'].read().decode('utf-8')

    # Simula arquivo com StringIO e lê linha a linha
    f = StringIO(content)
    return [line.strip() for line in f if line.strip()]

from datetime import date

def get_perdcomp_for_cnpjs(data_inicial, filename, data_final=None, save_json=True):
    # Define a data_final como hoje, se não for passada
    if not data_final:
        data_final = date.today().strftime("%Y-%m-%d")

    cnpj_list = load_cnpjs_from_txt(filename=filename)
    headers = {
        "versao_app": f'"{get_app_version()}"',
    }
    params = {
        "dataInicial": data_inicial,
        "dataFinal": data_final,
    }

    results = []
    cnpjs_com_dados = []
    cnpjs_sem_dados = []
    total = len(cnpj_list)

    for idx, cnpj in enumerate(cnpj_list, start=1):
        cnpj_fix = cnpj_fixing(cnpj)
        print(f"perdcomp cnpj: {cnpj_fix} ({idx} de {total})")
        try:
            response = requests.get(
                f"https://p-app-receita-federal.estaleiro.serpro.gov.br/servicos-rfb-apprfb-perdcomp/apprfb-perdcomp/consulta/ni/{cnpj_fix}",
                params=params,
                headers=headers,
                timeout=20,
            )
            response.raise_for_status()

            try:
                data = response.json()
                results.append(data)
                if data:
                    cnpjs_com_dados.append(cnpj_fix)
                else:
                    cnpjs_sem_dados.append(cnpj_fix)

            except Exception as e:
                print(f"get_perdcomp: {cnpj_fix} - resposta não é JSON: {response.text}")
                results.append(None)
                cnpjs_sem_dados.append(cnpj_fix)

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 412:
                print(f"get_perdcomp: {cnpj_fix} - 412 Client Error: Precondition Failed")
            else:
                print(f"get_perdcomp: {cnpj_fix} - {e}")
            results.append(None)
            cnpjs_sem_dados.append(cnpj_fix)

        except Exception as e:
            print(f"get_perdcomp: {cnpj_fix} - {e}")
            results.append(None)
            cnpjs_sem_dados.append(cnpj_fix)

        sleep_time = random.uniform(*SLEEP_TIME_RANGE)
        print(f"get_perdcomp: Sleeping for: {sleep_time:.2f} seconds")
        sleep(sleep_time)

    if save_json:
        saida_json = {
            "cnpjs_com_dados": cnpjs_com_dados,
            "cnpjs_sem_dados": cnpjs_sem_dados
        }
        with open("resultado_cnpjs.json", "w") as f:
            json.dump(saida_json, f, indent=2)
        print("Arquivo resultado_cnpjs.json salvo com sucesso.")
    
    # df_results = pd.DataFrame(results)
    # df_results["provedor"] = filename.upper()


    return results

# data_inicial = "2010-01-01"
# print(get_perdcomp_for_cnpjs(data_inicial, filename="cnpjs_test.txt"))
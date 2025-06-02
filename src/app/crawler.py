from functions import get_app_version
from time import sleep
import requests, random

def get_app_version():
    result = app('br.gov.economia.receita.rfb', lang='pt', country='br')
    return result['version']

SLEEP_TIME_RANGE = (1, 3) 

def get_perdcomp_for_cnpjs(cnpj_list, data_inicial, data_final):
    headers = {
        "versao_app": f'"{get_app_version()}"',
    }
    params = {
        "dataInicial": data_inicial,
        "dataFinal": data_final,
    }
    results = []
    total = len(cnpj_list)
    for idx, cnpj in enumerate(cnpj_list, start=1):
        cnpj_fix = str(cnpj).zfill(14)
        print(f"perdcomp cnpj: {cnpj_fix} ({idx} de {total})")
        try:
            response = requests.get(
                f"https://p-app-receita-federal.estaleiro.serpro.gov.br/servicos-rfb-apprfb-perdcomp/apprfb-perdcomp/consulta/ni/{cnpj_fix}",
                params=params,
                headers=headers,
                timeout=20,
            )
            response.raise_for_status()
            results.append(response.json())
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 412:
                print(f"get_perdcomp: {cnpj_fix} - 412 Client Error: Precondition Failed")
                results.append(None)
            else:
                print(f"get_perdcomp: {cnpj_fix} - {e}")
                results.append(None)
        except Exception as e:
            print(f"get_perdcomp: {cnpj_fix} - {e}")
            results.append(None)
        sleep_time = random.uniform(*SLEEP_TIME_RANGE)
        print(f"get_perdcomp: Sleeping for: {sleep_time:.2f} seconds")
        sleep(sleep_time)
    return results
from google_play_scraper import app

def get_app_version():
    result = app('br.gov.economia.receita.rfb', lang='pt', country='br')
    return result['version']

versao = get_app_version()
# print(versao)

def cnpj_fix(cnpj):
    return str(cnpj).zfill(14)

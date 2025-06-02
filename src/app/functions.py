from google_play_scraper import app

def get_app_version():
    result = app('br.gov.economia.receita.rfb', lang='pt', country='br')
    return result['version']

versao = get_app_version()
print(versao)

def cnpj_fix(cnpj):
    return str(cnpj).zfill(14)

def camel_to_snake(name):
    import re
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()

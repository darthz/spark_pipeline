# Spark Pipeline

## Objetivo

Esta pipeline tem como objetivo automatizar a extração, transformação, armazenamento e upload de dados de PERDCOMP (Pedido Eletrônico de Restituição, Ressarcimento ou Reembolso e Declaração de Compensação) para um banco de dados PostgreSQL. O fluxo foi desenvolvido para facilitar o processamento em larga escala de dados fiscais, garantindo organização em camadas (bronze, silver, gold) e integridade dos dados para análises e integrações futuras.

## Camadas da Pipeline

- **Bronze:** Armazena os dados brutos extraídos da API, em formato Parquet, com todos os campos disponíveis.
- **Silver:** Realiza limpeza, padronização de nomes de colunas e possíveis transformações intermediárias, salvando em formato Delta Lake.
- **Gold:** Seleciona e tipa as colunas finais, adiciona informações de processamento e prepara os dados para consumo e upload ao banco de dados.

## Tecnologias Utilizadas

- **Python 3.12+**
- **Pandas:** Manipulação de dados tabulares e leitura/escrita de arquivos Parquet.
- **PySpark:** Processamento distribuído, transformação de dados e integração com Delta Lake.
- **Delta Lake:** Formato de armazenamento transacional para dados analíticos (camadas silver e gold).
- **SQLAlchemy:** Integração e upload dos dados finais para o banco PostgreSQL.
- **PostgreSQL:** Banco de dados relacional para armazenamento dos dados finais.
- **dotenv:** Gerenciamento de variáveis de ambiente sensíveis (.env).
- **Git:** Controle de versão do projeto.

## Como usar

1. **Configure o arquivo `.env`** com as credenciais do banco PostgreSQL (veja `.env_example`).
2. **Execute a pipeline** a partir do script principal (`src/app/tests.py`) para processar os dados.
3. **Faça o upload dos dados gold** para o banco PostgreSQL usando o script `src/app/upload.py`.

## Observações

- As pastas `storage/bronze`, `storage/silver` e `storage/gold` são usadas para armazenar os dados intermediários e finais.
- O projeto utiliza arquivos `.gitkeep` para versionar as pastas vazias no Git.
- Arquivos sensíveis e grandes volumes de dados são ignorados pelo `.gitignore`.

---

Sinta-se à vontade para contribuir ou adaptar a pipeline conforme suas necessidades!
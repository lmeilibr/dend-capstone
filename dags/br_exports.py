from airflow import DAG
import logging
import os
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from datetime import timedelta
import requests
import json
from configparser import ConfigParser
from functions.create_tables import dal

config = ConfigParser()
config.read('credentials.cfg')

ROOT_PATH = 'data'

default_args = {
    'owner': 'meili',
    'depends_on_past': False,
    'start_date': datetime(1997, 1, 1),
    'end_date': datetime(2018, 12, 1),
    'retries': 1,
    'retries_delay': timedelta(minutes=1),
    'catchup': True
}

dag = DAG('brazil_exp_imp_etl_v5',
          default_args=default_args,
          description='Load and transform data in MySQL with Airflow',
          schedule_interval='@yearly'
          )


def begin():
    logging.info('it began!')
    paths = [
        ROOT_PATH,
        os.path.join(ROOT_PATH, 'EXP'),
        os.path.join(ROOT_PATH, 'IMP'),
        os.path.join(ROOT_PATH, 'AUX'),
        os.path.join(ROOT_PATH, 'TRUCKS'),
    ]
    [os.makedirs(path, exist_ok=True) for path in paths]
    logging.info('paths created')


def download_exports(**context):
    direction = 'EXP'
    root_path = os.path.join(ROOT_PATH, direction)
    abstract_download(direction, root_path, context)


def download_imports(**context):
    direction = 'IMP'
    root_path = os.path.join(ROOT_PATH, direction)
    abstract_download(direction, root_path, context)


def abstract_download(direction, root_path, context):
    year = context["execution_date"].year
    url = f'http://www.mdic.gov.br/balanca/bd/comexstat-bd/ncm/{direction}_{year}.csv'
    download_url(url, root_path)


def download_url(url, root_path):
    base_file = os.path.basename(url)
    download_path = os.path.join(root_path, base_file)
    if not os.path.exists(download_path):
        download = requests.get(url)
        content = download.content.decode('latin1')
        with open(download_path, 'w') as output:
            output.write(content)
            logging.info('file saved with success')


def aux_downloads():
    root_path = os.path.join(ROOT_PATH, 'AUX')
    aux = ['http://www.mdic.gov.br/balanca/bd/tabelas/NCM.csv',
           'http://www.mdic.gov.br/balanca/bd/tabelas/NCM_SH.csv',
           'http://www.mdic.gov.br/balanca/bd/tabelas/PAIS.csv',
           'http://www.mdic.gov.br/balanca/bd/tabelas/PAIS_BLOCO.csv',
           'http://www.mdic.gov.br/balanca/bd/tabelas/VIA.csv',
           'http://www.mdic.gov.br/balanca/bd/tabelas/URF.csv',
           ]

    [download_url(url, root_path) for url in aux]


def validation_downloads():
    root_path = os.path.join(ROOT_PATH, 'AUX')
    validation = ['http://www.mdic.gov.br/balanca/bd/comexstat-bd/ncm/EXP_TOTAIS_CONFERENCIA.csv',
                  'http://www.mdic.gov.br/balanca/bd/comexstat-bd/ncm/IMP_TOTAIS_CONFERENCIA.csv',
                  ]
    [download_url(url, root_path) for url in validation]


def truck_downloads():
    root_path = os.path.join(ROOT_PATH, 'TRUCKS')
    sales = 'http://api.bcb.gov.br/dados/serie/bcdata.sgs.7386/dados?formato=json'
    production = 'http://api.bcb.gov.br/dados/serie/bcdata.sgs.1375/dados?formato=json'

    download_json('sales', sales, root_path)
    download_json('production', production, root_path)


def download_json(name, url, root_path):
    download_path = os.path.join(root_path, name + '.json')
    if not os.path.exists(download_path):
        download = requests.get(url)
        content = download.json()
        with open(download_path, 'w') as output:
            json.dump(content, output)
            logging.info('file saved with success')


def init_db():
    # Database connection
    rdbms = config.get('DB', 'rdbms')
    connector = config.get('DB', 'connector')
    user = config.get('DB', 'user')
    password = config.get('DB', 'password')
    host = config.get('DB', 'host')
    port = config.get('DB', 'port')
    database = config.get('DB', 'database')
    conn_string = f'{rdbms}+{connector}://{user}:{password}@{host}:{port}/{database}'
    dal.db_init(conn_string)


def stg_imports_to_db(**context):
    year = context["execution_date"].year
    path = os.path.join(ROOT_PATH, 'IMP', f'IMP_{year}.csv')
    insert_into_db(path, dal.stg_import, prep_table)


def stg_exports_to_db(**context):
    year = context["execution_date"].year
    path = os.path.join(ROOT_PATH, 'EXP', f'EXP_{year}.csv')
    insert_into_db(path, dal.stg_export, prep_table)


def stg_trucks_to_db():
    prod = os.path.join(ROOT_PATH, 'TRUCKS', 'production.json')
    sales = os.path.join(ROOT_PATH, 'TRUCKS', 'sales.json')
    insert_into_db(prod, dal.stg_truck_production, prep_trucks)
    insert_into_db(sales, dal.stg_truck_sales, prep_trucks)


def insert_into_db(path, table, prep):
    data_dict = prep(path)
    insert_query = table.insert()
    init_db()
    batch = []
    for row in data_dict:
        batch.append(row)
        if len(batch) == 5000:
            dal.connection.execute(insert_query, batch)
            batch = []
            logging.info('5000 records inserted')
    dal.connection.execute(insert_query, batch)
    logging.info(len(batch), 'records inserted')


def prep_trucks(filepath):
    with open(filepath) as data:
        table = json.load(data)
        new_table = []
        for item in table:
            row = dict(date=datetime.strptime(item['data'], '%d/%m/%Y').date(),
                       value=item['valor'])
            new_table.append(row)
        return new_table


def prep_table(filepath):
    with open(filepath) as data:
        table = data.readlines()
        rows = []
        for line in table[1:]:
            fields = line.replace('"', '').split(";")
            row = {'co_ano': fields[0],
                   'co_mes': fields[1],
                   'co_ncm': fields[2],
                   'co_unid': fields[3],
                   'co_pais': fields[4],
                   'sg_uf_ncm': fields[5],
                   'co_via': fields[6],
                   'co_urf': fields[7],
                   'qt_estat': fields[8],
                   'kg_liquido': fields[9],
                   'vl_fob': fields[10]
                   }
            rows.append(row)
        return rows


def end_etl():
    logging.info('end etl!')


# start_operator = PythonOperator(
#     task_id='begin_execution',
#     dag=dag,
#     python_callable=begin
# )
# download_exports = PythonOperator(
#     task_id='download_exports',
#     dag=dag,
#     provide_context=True,
#     python_callable=download_exports
# )
#
# download_imports = PythonOperator(
#     task_id='download_imports',
#     dag=dag,
#     provide_context=True,
#     python_callable=download_imports
# )
#
# download_aux = PythonOperator(
#     task_id='download_aux',
#     dag=dag,
#     python_callable=aux_downloads
# )
#
# download_trucks = PythonOperator(
#     task_id='download_trucks',
#     dag=dag,
#     python_callable=truck_downloads
# )
#
# init_database = PythonOperator(
#     task_id='init_db',
#     dag=dag,
#     python_callable=init_db
# )
#
# stg_imports = PythonOperator(
#     task_id='stg_import',
#     dag=dag,
#     provide_context=True,
#     python_callable=stg_imports_to_db
# )
#
# stg_export = PythonOperator(
#     task_id='stg_export',
#     dag=dag,
#     provide_context=True,
#     python_callable=stg_exports_to_db
# )

stg_trucks = PythonOperator(
    task_id='stg_trucks',
    dag=dag,
    python_callable=stg_trucks_to_db
)
#
# end_operator = PythonOperator(
#     task_id='end_etl',
#     dag=dag,
#     python_callable=end_etl
# )

# start_operator >> download_exports
# start_operator >> download_imports
# start_operator >> download_aux
# start_operator >> download_trucks
# download_exports >> init_database
# download_imports >> init_database
# download_aux >> init_database
# download_trucks >> init_database
# init_database >> stg_imports
# stg_imports >> end_operator

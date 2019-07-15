""" ETL Dag for the Brazilian Commerce Statistics """
import json
import logging
import os
from configparser import ConfigParser
from datetime import datetime
from datetime import timedelta

import requests
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from dags.functions.create_tables import dal
from dags.functions.etl_queries import (load_dim_date, load_dim_country, load_dim_mode,
                                        load_dim_port, load_dim_product, load_dim_region,
                                        load_fact_truck, load_fact_trading, check_table_rows)

CONFIG = ConfigParser()
CONFIG.read('credentials.cfg')

ROOT_PATH = 'data'

DEFAULT_ARGS = {
    'owner': 'meili',
    'depends_on_past': False,
    'start_date': datetime(1997, 1, 1),
    'end_date': datetime(2000, 12, 1),
    'retries': 1,
    'retries_delay': timedelta(minutes=1),
    'catchup': True
}

dag = DAG('brazil_exp_imp_etl_v25',
          default_args=DEFAULT_ARGS,
          description='Load and transform data in MySQL with Airflow',
          schedule_interval='@yearly'
          )


def begin():
    """Setup the project download folders"""
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
    """Download export data for a given year (context)"""
    direction = 'EXP'
    root_path = os.path.join(ROOT_PATH, direction)
    abstract_download(direction, root_path, context)


def download_imports(**context):
    """Download import data for a given year (context)"""
    direction = 'IMP'
    root_path = os.path.join(ROOT_PATH, direction)
    abstract_download(direction, root_path, context)


def abstract_download(direction, root_path, context):
    """abstract download function to download imp/exp data"""
    year = context["execution_date"].year
    url = f'http://www.mdic.gov.br/balanca/bd/comexstat-bd/ncm/{direction}_{year}.csv'
    download_url(url, root_path)


def download_url(url, root_path):
    """given a url and a root path, download and saves a file"""
    base_file = os.path.basename(url)
    download_path = os.path.join(root_path, base_file)
    if not os.path.exists(download_path):
        download = requests.get(url)
        content = download.content.decode('latin1')
        with open(download_path, 'w') as output:
            output.write(content)
            logging.info('file saved with success')


def aux_downloads():
    """auxiliary table download list"""
    root_path = os.path.join(ROOT_PATH, 'AUX')
    aux = ['http://www.mdic.gov.br/balanca/bd/tabelas/NCM.csv',
           'http://www.mdic.gov.br/balanca/bd/tabelas/NCM_SH.csv',
           'http://www.mdic.gov.br/balanca/bd/tabelas/PAIS.csv',
           'http://www.mdic.gov.br/balanca/bd/tabelas/PAIS_BLOCO.csv',
           'http://www.mdic.gov.br/balanca/bd/tabelas/VIA.csv',
           'http://www.mdic.gov.br/balanca/bd/tabelas/URF.csv',
           ]

    [download_url(url, root_path) for url in aux]


def truck_downloads():
    """download truck production/sales data"""
    root_path = os.path.join(ROOT_PATH, 'TRUCKS')
    sales = 'http://api.bcb.gov.br/dados/serie/bcdata.sgs.7386/dados?formato=json'
    production = 'http://api.bcb.gov.br/dados/serie/bcdata.sgs.1375/dados?formato=json'

    download_json('sales', sales, root_path)
    download_json('production', production, root_path)


def download_json(name, url, root_path):
    """abstract function to download a json file"""
    download_path = os.path.join(root_path, name + '.json')
    if not os.path.exists(download_path):
        download = requests.get(url)
        content = download.json()
        with open(download_path, 'w') as output:
            json.dump(content, output)
            logging.info('file saved with success')


def init_db():
    """initialize a database"""
    # Database connection
    rdbms = CONFIG.get('DB', 'rdbms')
    connector = CONFIG.get('DB', 'connector')
    user = CONFIG.get('DB', 'user')
    password = CONFIG.get('DB', 'password')
    host = CONFIG.get('DB', 'host')
    port = CONFIG.get('DB', 'port')
    database = CONFIG.get('DB', 'database')
    conn_string = f'{rdbms}+{connector}://{user}:{password}@{host}:{port}/{database}'
    dal.db_init(conn_string)


def stg_imports_to_db(**context):
    year = context["execution_date"].year
    path = os.path.join(ROOT_PATH, 'IMP', f'IMP_{year}.csv')
    insert_into_db(path, dal.stg_import, prep_imp_exp, get_imp_exp_row, truncate=False)


def stg_exports_to_db(**context):
    year = context["execution_date"].year
    path = os.path.join(ROOT_PATH, 'EXP', f'EXP_{year}.csv')
    insert_into_db(path, dal.stg_export, prep_imp_exp, get_imp_exp_row, truncate=False)


def stg_ncm_to_db():
    path = os.path.join(ROOT_PATH, 'AUX', 'NCM.csv')
    insert_into_db(path, dal.stg_ncm, prep_table, get_ncm_row)


def stg_ncm_sh_to_db():
    path = os.path.join(ROOT_PATH, 'AUX', 'NCM_SH.csv')
    insert_into_db(path, dal.stg_ncm_sh, prep_table, get_ncm_sh_row)


def stg_trucks_to_db():
    prod = os.path.join(ROOT_PATH, 'TRUCKS', 'production.json')
    sales = os.path.join(ROOT_PATH, 'TRUCKS', 'sales.json')
    insert_into_db(prod, dal.stg_truck_production, prep_trucks)
    insert_into_db(sales, dal.stg_truck_sales, prep_trucks)


def stg_pais_to_db():
    path = os.path.join(ROOT_PATH, 'AUX', 'PAIS.csv')
    insert_into_db(path, dal.stg_pais, prep_table, get_pais_row)


def stg_pais_bloco_to_db():
    path = os.path.join(ROOT_PATH, 'AUX', 'PAIS_BLOCO.csv')
    insert_into_db(path, dal.stg_pais_bloco, prep_table, get_pais_bloco_row)


def stg_urf_to_db():
    path = os.path.join(ROOT_PATH, 'AUX', 'URF.csv')
    insert_into_db(path, dal.stg_urf, prep_table, get_urf_row)


def stg_via_to_db():
    path = os.path.join(ROOT_PATH, 'AUX', 'VIA.csv')
    insert_into_db(path, dal.stg_via, prep_table, get_via_row)


def insert_into_db(path, table, prep, row_getter=None, truncate=True):
    data_dict = prep(path, row_getter)
    insert_query = table.insert()
    init_db()
    # truncate table
    if truncate:
        dal.connection.execute(f"TRUNCATE {table.name}")
    batch = []
    for row in data_dict:
        batch.append(row)
        if len(batch) == 2500:
            dal.connection.execute(insert_query, batch)
            batch = []
            logging.info('5000 records inserted')
    dal.connection.execute(insert_query, batch)
    logging.info(len(batch), 'records inserted')


def prep_trucks(filepath, row_getter=None):
    with open(filepath) as data:
        table = json.load(data)
        new_table = []
        for item in table:
            row = dict(date=datetime.strptime(item['data'], '%d/%m/%Y').date(),
                       year=int(item['data'].split('/')[2]),
                       month=int(item['data'].split('/')[1]),
                       units=item['valor'])
            new_table.append(row)
        return new_table


def prep_imp_exp(filepath, row_func):
    with open(filepath, encoding='latin1') as data:
        table = data.readlines()
        rows = []
        for line in table[1:]:
            line = line.encode("ascii", "ignore").decode('utf-8')
            fields = [ln.replace('"', '') for ln in line.split(';')]
            row = row_func(fields)
            if row:
                rows.append(row)
        return rows


def prep_table(filepath, row_func):
    with open(filepath, encoding='latin1') as data:
        table = data.readlines()
        rows = []
        for line in table[1:]:
            line = line.encode("ascii", "ignore").decode('utf-8')
            fields = [ln.replace('"', '') for ln in line.split(';"')]
            row = row_func(fields)
            if row:
                rows.append(row)
        return rows


def get_imp_exp_row(fields):
    if len(fields) != 11:
        return None
    row = {'co_ano': fields[0],
           'co_mes': fields[1],
           'co_ncm': fields[2],
           'co_unid': fields[3],
           'co_pais': fields[4],
           'sg_uf_ncm': fields[5],
           'co_via': fields[6],
           'co_urf': fields[7],
           'kg_liquido': fields[9],
           'vl_fob': fields[10]
           }
    return row


def get_ncm_row(fields):
    if len(fields) != 14:
        return None
    row = {
        "co_ncm": fields[0],
        "co_unid": fields[1],
        "co_sh6": fields[2],
        "co_ppe": fields[3],
        "co_ppi": fields[4],
        "co_fat_agreg": fields[5],
        "co_cgce_n3": fields[7],
        "co_siit": fields[8],
        "co_exp_subset": fields[10],
        "no_ncm_por": fields[11],
        "no_ncm_esp": fields[12],
        "no_ncm_ing": fields[13],
    }
    return row


def get_ncm_sh_row(fields):
    if len(fields[8]) > 15:
        logging.info(fields[8])
        return None
    row = {
        "co_sh6": fields[0],
        "no_sh6_esp": fields[2],
        "no_sh6_ing": fields[3],
        "co_sh4": fields[4],
        "no_sh4_por": fields[5],
        "no_sh4_esp": fields[6],
        "no_sh4_ing": fields[7],
        "co_sh2": fields[8],
        "no_sh2_por": fields[9],
        "no_sh2_esp": fields[10],
        "no_sh2_ing": fields[11],
        "co_ncm_secrom": fields[12],
        "no_sec_por": fields[13],
        "no_sec_esp": fields[14],
        "no_sec_ing": fields[15],
    }
    return row


def get_pais_row(fields):
    if len(fields) != 6:
        logging.info(fields)
        return None
    row = {
        "co_pais": fields[0],
        "co_pais_ison3": fields[1],
        "co_pais_isoa3": fields[2],
        "no_pais": fields[3],
        "no_pais_ing": fields[4],
        "no_pais_esp": fields[5]
    }
    return row


def get_pais_bloco_row(fields):
    if len(fields) != 5:
        logging.info(fields)
        return None
    row = {
        "co_pais": fields[0],
        "co_bloco": fields[1],
        "no_bloco": fields[2],
        "no_bloco_ing": fields[3],
        "no_bloco_esp": fields[4],
    }
    return row


def get_urf_row(fields: list):
    row = {
        "co_urf": fields[0],
        "no_urf": fields[1].split('-')[1].strip()
    }
    return row


def get_via_row(fields):
    row = {
        "co_via": fields[0],
        "no_via": fields[1]
    }
    return row


def load_date():
    init_db()
    load_dim_date(dal)


def load_mode():
    init_db()
    load_dim_mode(dal)


def load_port():
    init_db()
    load_dim_port(dal)


def load_region():
    init_db()
    load_dim_region(dal)


def load_country():
    init_db()
    load_dim_country(dal)


def load_product():
    init_db()
    load_dim_product(dal)


def load_truck():
    init_db()
    load_fact_truck(dal)


def load_trading(**context):
    init_db()
    load_fact_trading(dal, context['execution_date'].year)


def check_trading():
    init_db()
    check_table_rows(dal, dal.fact_trading)


def check_trucks():
    init_db()
    check_table_rows(dal, dal.fact_truck)


def end_etl():
    logging.info('end etl!')


start_operator = PythonOperator(
    task_id='begin_execution',
    dag=dag,
    python_callable=begin
)

download_exports = PythonOperator(
    task_id='download_exports',
    dag=dag,
    provide_context=True,
    python_callable=download_exports
)

download_imports = PythonOperator(
    task_id='download_imports',
    dag=dag,
    provide_context=True,
    python_callable=download_imports
)

download_aux = PythonOperator(
    task_id='download_aux',
    dag=dag,
    python_callable=aux_downloads
)

download_trucks = PythonOperator(
    task_id='download_trucks',
    dag=dag,
    python_callable=truck_downloads
)

stg_imports = PythonOperator(
    task_id='stg_import',
    dag=dag,
    provide_context=True,
    python_callable=stg_imports_to_db
)

stg_exports = PythonOperator(
    task_id='stg_export',
    dag=dag,
    provide_context=True,
    python_callable=stg_exports_to_db
)

stg_trucks = PythonOperator(
    task_id='stg_trucks',
    dag=dag,
    python_callable=stg_trucks_to_db
)

stg_ncm = PythonOperator(
    task_id='stg_ncm',
    dag=dag,
    python_callable=stg_ncm_to_db
)

stg_ncm_sh = PythonOperator(
    task_id='stg_ncm_sh',
    dag=dag,
    python_callable=stg_ncm_sh_to_db
)

stg_pais = PythonOperator(
    task_id='stg_pais',
    dag=dag,
    python_callable=stg_pais_to_db
)

stg_urf = PythonOperator(
    task_id='stg_urf',
    dag=dag,
    python_callable=stg_urf_to_db
)
stg_via = PythonOperator(
    task_id='stg_via',
    dag=dag,
    python_callable=stg_via_to_db
)

dim_date = PythonOperator(
    task_id='load_dim_date',
    dag=dag,
    python_callable=load_date
)

dim_mode = PythonOperator(
    task_id='load_dim_mode',
    dag=dag,
    python_callable=load_mode
)

dim_port = PythonOperator(
    task_id='load_dim_port',
    dag=dag,
    python_callable=load_port
)

dim_region = PythonOperator(
    task_id='load_dim_region',
    dag=dag,
    python_callable=load_region
)

dim_country = PythonOperator(
    task_id='load_dim_country',
    dag=dag,
    python_callable=load_country
)

dim_product = PythonOperator(
    task_id='load_dim_product',
    dag=dag,
    python_callable=load_product
)

fact_truck = PythonOperator(
    task_id='load_fact_truck',
    dag=dag,
    python_callable=load_truck
)

fact_trading = PythonOperator(
    task_id='load_fact_trading',
    dag=dag,
    provide_context=True,
    python_callable=load_trading
)

check_fact_trading = PythonOperator(
    task_id='check_fact_trading',
    dag=dag,
    python_callable=check_trading
)

check_fact_truck = PythonOperator(
    task_id='check_fact_truck',
    dag=dag,
    python_callable=check_trucks
)

end_operator = PythonOperator(
    task_id='end_etl',
    dag=dag,
    python_callable=end_etl
)

start_operator >> download_exports >> stg_exports >> fact_trading
start_operator >> download_imports >> stg_imports
stg_imports >> dim_region >> fact_trading
start_operator >> download_trucks >> stg_trucks
start_operator >> download_aux
download_aux >> stg_ncm
download_aux >> stg_ncm_sh
download_aux >> stg_pais >> dim_country >> fact_trading
download_aux >> stg_urf >> dim_port >> fact_trading
download_aux >> stg_via >> dim_mode >> fact_trading
stg_trucks >> dim_date >> fact_truck
stg_ncm >> dim_product
stg_ncm_sh >> dim_product
dim_product >> fact_trading
fact_trading >> check_fact_trading >> end_operator
fact_truck >> check_fact_truck >> end_operator

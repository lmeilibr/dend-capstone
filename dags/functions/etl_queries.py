from dags.functions.create_tables import DataAccessLayer
from sqlalchemy import select, and_


def execute(dal, query, trg):
    dal.connection.execute(f"TRUNCATE {trg.name}")
    dal.connection.execute(query)


def load_dim_date(dal: DataAccessLayer):
    src = dal.stg_truck_sales
    trg = dal.dim_date

    query = trg.insert().from_select(
        [trg.c.year, trg.c.month],
        select([src.c.year, src.c.month])
    )
    execute(dal, query, trg)


def load_dim_mode(dal: DataAccessLayer):
    src = dal.stg_via
    trg = dal.dim_mode

    query = trg.insert().from_select(
        [trg.c.mode_nk, trg.c.mode_name],
        select([src.c.co_via, src.c.no_via])
    )
    execute(dal, query, trg)


def load_dim_port(dal: DataAccessLayer):
    src = dal.stg_urf
    trg = dal.dim_port

    query = trg.insert().from_select(
        [trg.c.port_nk, trg.c.port_name],
        select([src.c.co_urf, src.c.no_urf])
    )
    execute(dal, query, trg)


def load_dim_region(dal: DataAccessLayer):
    src = dal.stg_import
    trg = dal.dim_region

    query = trg.insert().from_select(
        [trg.c.region_nk],
        select([src.c.sg_uf_ncm]).distinct()
    )
    execute(dal, query, trg)


def load_dim_country(dal: DataAccessLayer):
    src = dal.stg_pais
    trg = dal.dim_country

    query = trg.insert().from_select(
        [trg.c.country_nk, trg.c.iso3, trg.c.country_name],
        select([src.c.co_pais, src.c.co_pais_isoa3, src.c.no_pais_ing])
    )
    execute(dal, query, trg)


def load_dim_product(dal: DataAccessLayer):
    ncm = dal.stg_ncm
    sh = dal.stg_ncm_sh
    trg = dal.dim_product

    query = trg.insert().from_select(
        [trg.c.product_nk, trg.c.ncm_name, trg.c.sh6_name, trg.c.sh4_name,
         trg.c.sh2_name, trg.c.section],
        select([ncm.c.co_ncm, ncm.c.no_ncm_ing, sh.c.no_sh6_ing,
                sh.c.no_sh4_ing, sh.c.no_sh2_ing, sh.c.no_sec_ing]).where(
            ncm.c.co_sh6 == sh.c.co_sh6
        )
    )
    execute(dal, query, trg)


def load_fact_truck(dal: DataAccessLayer):
    prod = dal.stg_truck_production
    date = dal.dim_date
    trg = dal.fact_truck
    sales_query = trg.insert().from_select(
        [trg.c.date_sk, trg.c.operation, trg.c.truck_units],
        select([date.c.date_sk, dal.stg_truck_sales.c.operation, dal.stg_truck_sales.c.units])
            .where(and_(date.c.year == dal.stg_truck_sales.c.year,
                        date.c.month == dal.stg_truck_sales.c.month)
                   )
    )

    production_query = trg.insert().from_select(
        [trg.c.date_sk, trg.c.operation, trg.c.truck_units],
        select(
            [date.c.date_sk, prod.c.operation, prod.c.units])
            .where(and_(date.c.year == prod.c.year,
                        date.c.month == prod.c.month)
                   )
    )

    dal.connection.execute(f'TRUNCATE {trg.name}')
    dal.connection.execute(sales_query)
    dal.connection.execute(production_query)


def load_fact_trading(dal: DataAccessLayer, year):
    dat = dal.dim_date
    mod = dal.dim_mode
    por = dal.dim_port
    reg = dal.dim_region
    cnt = dal.dim_country
    prd = dal.dim_product
    imp = dal.stg_import
    exp = dal.stg_export
    trg = dal.fact_trading

    import_query = trading_query(cnt, dat, imp, mod, por, prd, reg, trg, year)
    export_query = trading_query(cnt, dat, exp, mod, por, prd, reg, trg, year)
    dal.connection.execute(import_query)
    dal.connection.execute(export_query)


def trading_query(cnt, dat, trd, mod, por, prd, reg, trg, year):
    query = trg.insert().from_select(
        [trg.c.date_sk, trg.c.product_sk, trg.c.country_sk,
         trg.c.region_sk, trg.c.port_sk, trg.c.mode_sk,
         trg.c.net_kilogram, trg.c.fob_value_usd,
         trg.c.direction],
        select([
            dat.c.date_sk, prd.c.product_sk, cnt.c.country_sk,
            reg.c.region_sk, por.c.port_sk, mod.c.mode_sk,
            trd.c.kg_liquido, trd.c.vl_fob,
            trd.c.direction]).where(
            and_(trd.c.co_ano == dat.c.year,
                 trd.c.co_mes == dat.c.month,
                 trd.c.co_ncm == prd.c.product_nk,
                 trd.c.co_pais == cnt.c.country_nk,
                 trd.c.sg_uf_ncm == reg.c.region_nk,
                 trd.c.co_via == mod.c.mode_nk,
                 trd.c.co_urf == por.c.port_nk,
                 trd.c.co_ano == year
                 )
        )
    )
    return query


def check_table_rows(dal, table):
    query = table.select()
    result = dal.connection.execute(query).fetchone()
    if not result:
        raise ValueError(f"Data quality check failed. {table.name} returned no results")

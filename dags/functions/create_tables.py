from sqlalchemy import Table, MetaData, create_engine, Integer, \
    Column, String, Date, UniqueConstraint


class DataAccessLayer:
    connection = None
    engine = None
    conn_string = None
    metadata = MetaData()

    def db_init(self, conn_string):
        self.engine = create_engine(conn_string or self.conn_string)
        self.metadata.create_all(self.engine)
        self.connection = self.engine.connect()

    stg_truck_production = Table('stg_truck_production',
                                 metadata,
                                 Column('truck_production_id', Integer(), primary_key=True),
                                 Column('date', Date()),
                                 Column('year', Integer()),
                                 Column('month', Integer()),
                                 Column('units', Integer()),
                                 Column('operation', String(20), default='production'),
                                 UniqueConstraint('date', 'units')
                                 )

    stg_truck_sales = Table('stg_truck_sales',
                            metadata,
                            Column('truck_sales_id', Integer(), primary_key=True),
                            Column('date', Date()),
                            Column('year', Integer()),
                            Column('month', Integer()),
                            Column('units', Integer()),
                            Column('operation', String(20), default='sales'),
                            UniqueConstraint('date', 'units')
                            )

    stg_export = Table('stg_export',
                       metadata,
                       Column('export_id', Integer(), primary_key=True),
                       Column('direction', String(6), default='export'),
                       Column('co_ano', Integer(), index=True),
                       Column('co_mes', Integer()),
                       Column('co_ncm', String(8)),
                       Column('co_unid', Integer()),
                       Column('co_pais', String(3)),
                       Column('sg_uf_ncm', String(2)),
                       Column('co_via', String(2)),
                       Column('co_urf', String(7)),
                       Column('qt_estat', Integer()),
                       Column('kg_liquido', Integer()),
                       Column('vl_fob', Integer())
                       )

    stg_import = Table('stg_import',
                       metadata,
                       Column('import_id', Integer(), primary_key=True),
                       Column('direction', String(6), default='import'),
                       Column('co_ano', Integer(), index=True),
                       Column('co_mes', Integer()),
                       Column('co_ncm', String(8)),
                       Column('co_unid', Integer()),
                       Column('co_pais', String(3)),
                       Column('sg_uf_ncm', String(2), index=True),
                       Column('co_via', String(2)),
                       Column('co_urf', String(7)),
                       Column('qt_estat', Integer()),
                       Column('kg_liquido', Integer()),
                       Column('vl_fob', Integer())
                       )

    stg_ncm = Table('stg_ncm',
                    metadata,
                    Column('ncm_id', Integer(), primary_key=True),
                    Column('co_ncm', String(8)),
                    Column('co_unid', Integer()),
                    Column('co_sh6', String(6), index=True),
                    Column('co_ppe', Integer()),
                    Column('co_ppi', Integer()),
                    Column('co_fat_agreg', String(2)),
                    Column('co_cuci_item', Integer()),
                    Column('co_cgce_n3', Integer()),
                    Column('co_siit', Integer()),
                    Column('co_isic4', Integer()),
                    Column('co_exp_subset', Integer()),
                    Column('no_ncm_por', String(255)),
                    Column('no_ncm_esp', String(255)),
                    Column('no_ncm_ing', String(255))
                    )

    stg_ncm_sh = Table('stg_ncm_sh',
                       metadata,
                       Column('ncm_sh_id', Integer(), primary_key=True),
                       Column('co_sh6', String(6), unique=True),
                       Column('no_sh6_por', String(300)),
                       Column('no_sh6_esp', String(300)),
                       Column('no_sh6_ing', String(300)),
                       Column('co_sh4', String(4)),
                       Column('no_sh4_por', String(400)),
                       Column('no_sh4_esp', String(300)),
                       Column('no_sh4_ing', String(300)),
                       Column('co_sh2', String(15)),
                       Column('no_sh2_por', String(255)),
                       Column('no_sh2_esp', String(255)),
                       Column('no_sh2_ing', String(255)),
                       Column('co_ncm_secrom', String(5)),
                       Column('no_sec_por', String(255)),
                       Column('no_sec_esp', String(255)),
                       Column('no_sec_ing', String(255))
                       )

    stg_pais = Table('stg_pais',
                     metadata,
                     Column('pais_id', Integer(), primary_key=True),
                     Column('co_pais', String(3), unique=True),
                     Column('co_pais_ison3', String(3)),
                     Column('co_pais_isoa3', String(3)),
                     Column('no_pais', String(150)),
                     Column('no_pais_ing', String(150)),
                     Column('no_pais_esp', String(150))
                     )

    stg_pais_bloco = Table('stg_pais_bloco',
                           metadata,
                           Column('pais_bloco_id', Integer(), primary_key=True),
                           Column('co_pais', String(3)),
                           Column('co_bloco', Integer()),
                           Column('no_bloco', String(100)),
                           Column('no_bloco_ing', String(100)),
                           Column('no_bloco_esp', String(100))
                           )

    stg_urf = Table('stg_urf',
                    metadata,
                    Column('urf_id', Integer, primary_key=True),
                    Column('co_urf', String(7), unique=True),
                    Column('no_urf', String(100))
                    )
    stg_via = Table('stg_via',
                    metadata,
                    Column('via_id', Integer(), primary_key=True),
                    Column('co_via', String(2), unique=True),
                    Column('no_via', String(32))
                    )

    dim_date = Table('dim_date',
                     metadata,
                     Column('date_sk', Integer(), primary_key=True),
                     Column('year', Integer()),
                     Column('month', Integer()),
                     UniqueConstraint('year', 'month')
                     )

    dim_mode = Table('dim_mode',
                     metadata,
                     Column('mode_sk', Integer(), primary_key=True),
                     Column('mode_nk', String(2), unique=True),
                     Column('mode_name', String(255)),
                     UniqueConstraint('mode_name')
                     )

    dim_product = Table('dim_product',
                        metadata,
                        Column('product_sk', Integer(), primary_key=True),
                        Column('product_nk', Integer(), unique=True),
                        Column('ncm_name', String(400)),
                        Column('sh6_name', String(400)),
                        Column('sh4_name', String(400)),
                        Column('sh2_name', String(400)),
                        Column('section', String(400))
                        )
    dim_country = Table('dim_country',
                        metadata,
                        Column('country_sk', Integer(), primary_key=True),
                        Column('country_nk', String(3), unique=True),
                        Column('iso3', String(3)),
                        Column('country_name', String(255))
                        )
    dim_region = Table('dim_region',
                       metadata,
                       Column('region_sk', Integer(), primary_key=True),
                       Column('region_nk', String(2), unique=True),
                       Column('region_name', String(100))
                       )
    dim_port = Table('dim_port',
                     metadata,
                     Column('port_sk', Integer(), primary_key=True),
                     Column('port_nk', String(7), unique=True),
                     Column('port_name', String(255))
                     )

    dim_operation = Table('dim_operation',
                          metadata,
                          Column('operation_sk', Integer(), primary_key=True),
                          Column('operation_type', String(50), unique=True)
                          )

    fact_truck = Table('fact_truck',
                       metadata,
                       Column('fact_truck_sk', Integer(), primary_key=True),
                       Column('date_sk', Integer()),
                       Column('operation', String(20)),
                       Column('truck_units', Integer())
                       )

    fact_trading = Table('fact_trading',
                         metadata,
                         Column('fact_trading_sk', Integer(), primary_key=True),
                         Column('date_sk', Integer()),
                         Column('product_sk', Integer()),
                         Column('country_sk', Integer()),
                         Column('region_sk', Integer()),
                         Column('port_sk', Integer()),
                         Column('mode_sk', Integer()),
                         Column('direction', String(6)),
                         Column('net_kilogram', Integer()),
                         Column('fob_value_usd', Integer())
                         )


dal = DataAccessLayer()

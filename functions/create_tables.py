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
                                 Column('value', Integer()),
                                 UniqueConstraint('date', 'value')
                                 )

    stg_truck_sales = Table('stg_truck_sales',
                            metadata,
                            Column('truck_sales_id', Integer(), primary_key=True),
                            Column('date', Date()),
                            Column('value', Integer()),
                            UniqueConstraint('date', 'value')
                            )

    stg_export = Table('stg_export',
                       metadata,
                       Column('export_id', Integer(), primary_key=True),
                       Column('co_ano', Integer()),
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
                       Column('co_ano', Integer()),
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

    stg_ncm = Table('stg_ncm',
                    metadata,
                    Column('ncm_id', Integer(), primary_key=True),
                    Column('co_ncm', String(8)),
                    Column('co_unid', Integer()),
                    Column('co_sh6', String(6)),
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
                       Column('ncm_sh_id', Integer()),
                       Column('co_sh6', String(6), unique=True),
                       Column('no_sh6_por', String(255)),
                       Column('no_sh6_esp', String(255)),
                       Column('no_sh6_ing', String(255)),
                       Column('co_sh4', String(4)),
                       Column('no_sh4_por', String(255)),
                       Column('no_sh4_esp', String(255)),
                       Column('no_sh4_ing', String(255)),
                       Column('co_sh2', String(2)),
                       Column('no_sh2_por', String(255)),
                       Column('no_sh2_esp', String(255)),
                       Column('no_sh2_ing', String(255)),
                       Column('co_ncm_secrom', String(1)),
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
                           Column('co_pais', String(3), unique=True),
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


dal = DataAccessLayer()

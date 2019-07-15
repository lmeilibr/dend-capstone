# dend-capstone
Capstone Project for the Udacity Data Enginner Nanodegree

## Objective
The goal of this project was to create a data model for the Brazilian Commerce and Industry statistics. 
This initial version includes data for imported and exported goods on a monthly basis, 
with the series initiating in 1997. Another dataset is the truck production and sales statistics, also on a monthly basis,
with sales beginning in 1990 and production in 1993.<br>
The data is public available.
### Queries that this Data Model could answer:
- Trends in exports/imports over the years by products
- Who are the main Brazilian importers and exporters?
- How imports of a specific product(s) could help predict the production of trucks? 

## Technologies
For this project the technologies chosen were:
- Airflow: For ETL orchestration of many many tasks
- SQLAlchemy: A framework to move data from Python to any other RDBMS. This lowers the dependency of the project to a 
single RDBMS, making it easy to change between difference RDBMS vendors in the future. 
- MySQL: The Relational Database Management System of choice. Open source, simple to configure and 
powerful enough for our needs

## Project Setup
*`It's assumed that AIRFLOW is already configured in your environment`<br>*

First, install the project dependecies<br>

`pip install -r requirements.txt` <br>

Second, configure a user and database into mysql according to the variables in the credentials.cfg file

Third, run the dag through the Ariflow GUI

## Steps
The steps taken into this ETL can be summarize below:
![ETL](https://github.com/lmeilibr/dend-capstone/blob/master/etl_flow.png "ETL Flow diagram")


## Data Sources:
For the import and export data, the link to the csv files can be found here:<br>
[comexstat](http://www.mdic.gov.br/balanca/bd/comexstat-bd)<br>

The files of interest are:
 - Truck units produced per month
 - Truck units sales per month
 - Export (Goods exported per month)
 - Import (Goods imported per month)
 - Auxiliary:
    - NCM (Mercosul product nomenclature)
    - NCM_SH (Harmonized System)
    - PAIS (Country)
    - URF (Fiscal ports of entry/exit)
    - VIA (Transport mode)
    
The biggest tables in this data set are the export/import. With more than 1 million rows for each year of data.
Considering a range from 1997 to 2018, this is over 20 million rows.
    
## Data Model
The final data model is composed of 6 Dimensions and 2 Fact Tables:
### Dimensions:

dim_date

|field  |type|description       |
|-------|----|------------------|
|date_sk|int |surrogate key     |
|year   |int |4 digit year value|
|month  |int |1=jan..12=dec     |

dim_country

|field       |type  |description             |
|------------|------|------------------------|
|country_sk  |int   |surrogate key           |
|country_nk  |string| 3 char code from source|
|iso3        |string| ISO 3 char code        |
|country_name|string|country name in english |

dim_mode

|field       |type  |description             |
|------------|------|------------------------|
|mode_sk     |int   |surrogate key           |
|mode_nk     |string|code from source        | 
|mode_name   |string|mode name               |

dim_port

|field       |type  |description             |
|------------|------|------------------------|
|port_sk     |int   |surrogate key           |
|port_nk     |string|code from source        |
|port_name   |string|port name               |

dim_product

|field       |type  |description             |
|------------|------|------------------------|
|product_sk  |int   |surrogate key           |
|product_nk  |int   |code from source        |
|ncm_name    |string|ncm name in english     |
|sh6_name    |string|sh6 name in english     |
|sh4_name    |string|sh4 name in english     |
|sh2_name    |string|sh2 name in english     |
|section     |string|section name in english |

dim_region

|field       |type  |description             |
|------------|------|------------------------|
|region_sk   |int   |surrogate key           |
|region_nk   |string|State 2 char code       |
|region_name |string|State name              |

### Facts

fact_truck

|field        |type  |description              |
|-------------|------|-------------------------|
|fact_truck_sk|int   |surrogate key            |
|date_sk      |int   |reference key to dim_date|
|operation    |string|sales or production      |
|truck_units  |int   |number of vehicles       |

fact_trading

|field          |type  |description                 |
|---------------|------|----------------------------|
|fact_trading_sk|int   |surrogate key               |
|date_sk        |int   |reference key to dim_date   |
|product_sk     |int   |reference key to dim_product|
|country_sk     |int   |reference key to dim_country|
|region_sk      |int   |reference key to dim_region |
|port_sk        |int   |reference key to dim_port   |
|mode_sk        |int   |reference keu to dim_mode   |
|direction      |string|import or export            |
|net_kilogram   |int   |unit in net kilogram        |
|fob_value_usd  |int   |unit in US Dollars (FOB)    |

## Addressing Other Scenarios:

### The data was increased by 100x.

With data increasing by 100x, performance and storage space would be affected. In this case, 
moving the source data to an S3 bucket on AWS would help in this scalability.

### The pipelines would be run on a daily basis by 7 am every day.

With pipelines running on a daily basis, some smart choices should be made. Truncating tables shall be 
substituted by an incremental load. This would save some time and a lot of bandwith.

### The database needed to be accessed by 100+ people.
With data needing to be accessed by 100+ people, a clustered Database would be an ideal solution, 
as replication (read-only) nodes would distributed the demand for data. For this case, moving from MySQL to Redshift 
would be an alternative, but costs calculation would be helpful to support this solution.



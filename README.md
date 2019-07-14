# dend-capstone
Capstone Project for the Udacity Data Enginner Nanodegree

## Objective
The goal of this project was to create a data model for the Brazilian Commerce and Industry statistics. 
This initial version includes data for imported and exported goods on a monthly basis, 
with the series initiating in 1997. Another dataset is the truck production and sales statistics, also on a monthly basis,
with sales beginning in 1990 and production in 1993.<br>
The data is public available.

## Steps
The steps taken into this ETL can be summarize below:
![ETL](https://github.com/lmeilibr/dend-capstone/blob/master/etl_flow.png "ETL Flow diagram")

## Technologies
For this project the technologies chosen were:
- Airflow: For ETL orchestration
- SQLAlchemy: A framework to move data from Python to any other RDBMS. This lowers the dependency of the project to a 
single RDBMS, making it easy to change between difference RDBMS vendors in the future. 
- MySQL: The Relational Database Management System of choice.


Data Source:
- Import and Export Statistics
- Truck Production Statistics
- Truck Sales Statistics


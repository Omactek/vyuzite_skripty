import os
import pandas as pd
import psycopg2
from sqlalchemy import create_engine

#directory containing your CSV files
csv_directory = r'D:\School\bakalarka\data\data_wip\redo\02-1hod_checked'
metadata_file = r'D:\School\bakalarka\data\data_wip\redo\Metadata_by_stations.csv'
station_metadata = r'D:\School\bakalarka\data\data_wip\redo\station_metadata.xlsx'

#set up PostgreSQL connection parameters
db_params = {
    'user': 'user',
    'password': 'pass',
    'host': 'host',
    'port': 'port',
    'database': 'database',
}

#create PostgreSQL engine
engine = create_engine(f'postgresql+psycopg2://{db_params["user"]}:{db_params["password"]}@{db_params["host"]}:{db_params["port"]}/{db_params["database"]}')

#function to sanitize column names
def sanitize_column_name(column_name):
    #check if value is NaN
    if pd.isna(column_name):
        return None 
    
    return column_name.replace('[', '').replace(']', '').replace('%', 'pct').replace(' ', '')

def sanitize_metadata_col_name(column_name):
    return column_name.replace('[', '').replace(']', '').replace('%', 'pct').replace(' ', '_').replace('(', '').replace(')', '')

def create_django_metadata_col_name(column_name):
    return column_name.replace('[', '').replace(']', '').replace('%', 'pct').replace(' ', '_').replace('(', '').replace(')', '').replace('/', '_').replace('-', '').rstrip('_').lower()


#function to create tables from CSV files
def create_table_from_csv(csv_file):
    #extract table name from the CSV file name by removing "_hour_final.csv"
    original_table_name = os.path.splitext(os.path.basename(csv_file))[0].replace('_hour_final', '')
    table_name = original_table_name.lower()

    #load CSV into a DataFrame
    df = pd.read_csv(csv_file, sep = ';')

    #combine 'Year', 'Month', 'Day', and 'Hour' into a single datetime column
    df['date_time'] = pd.to_datetime(df[['Year', 'Month', 'Day', 'Hour']])

    #sanitize column names
    df.columns = [sanitize_column_name(col) for col in df.columns]

    #remove redundant columns
    df.drop(['Year', 'Month', 'Day', 'Hour', 'Date', 'date'], axis=1, inplace=True, errors='ignore')

    #write DataFrame to PostgreSQL
    df.to_sql(table_name, engine, index=False, if_exists='replace')

    #set 'DateTime' column as the primary key
    with engine.connect() as connection:
        connection.execute(f'ALTER TABLE "{table_name}" ADD PRIMARY KEY (date_time);')

#function to create metadata of values
def create_value_metadata_table (csv_metadata):

    #creates value with name of the table
    table_name = 'values_metadata'

    #load metadata csv file into DataFrame
    df = pd.read_csv(csv_metadata, sep = ';')

    #drop duplicates based on specified columns
    df = df.drop_duplicates(subset=['Parameter', 'Parameter abreviation in data file'])

    #drop rows not included in hourly csv files
    df = df[df['Notes'] != 'Not included in hourly csv files']

    #select specific columns
    df = df[['Parameter', 'Parameter abreviation in data file', 'Unit']]

    df['django_field_name'] = [create_django_metadata_col_name(col) for col in df['Parameter abreviation in data file']]

    #create database table
    df.to_sql(table_name, engine, index = False, if_exists='replace')

    #add a composite primary key
    with engine.connect() as connection:
        connection.execute(f"ALTER TABLE {table_name} ADD PRIMARY KEY (\"Parameter\", \"Parameter abreviation in data file\")")

#function to create metadata of stations
def create_station_metadata(csv_file):

    #creates value with name of the table
    table_name = 'station_metadata'

    #load station metadata excel file into DataFrame
    df = pd.read_excel(csv_file)

    #create database table
    df.to_sql(table_name, engine, index=False, if_exists='replace')

    #add column with geometry type and create postgis point geometry 
    with engine.connect() as connection:
        connection.execute(f"ALTER TABLE {table_name} ADD COLUMN geom geometry(Point, 4326);")
        connection.execute(f"UPDATE {table_name} SET geom = ST_SetSRID(ST_MakePoint(long, lat), 4326);")

for csv_file in os.listdir(csv_directory):
    if csv_file.endswith('_hour_final.csv'):
        csv_path = os.path.join(csv_directory, csv_file)
        create_table_from_csv(csv_path)


create_value_metadata_table(metadata_file)
create_station_metadata(station_metadata)

#dispose the engine connection
engine.dispose()

print('Tables created successfully.')

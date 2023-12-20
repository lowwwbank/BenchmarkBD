import configparser
import sqlite3
import pandas
import sqlalchemy
import testDuckDB
import testPsycorg2
import testSqlite3

config = configparser.ConfigParser()  #  создаём объекта парсера
config.read("code\\config.ini")  #  читаем конфиг
print(config['postgres'].values())
postgres = f"postgresql://{config['postgres']['user']}:{config['postgres']['password']}@{config['postgres']['host']}:{config['postgres']['port']}/{config['postgres']['dbname']}"
refactor_column1 = "tpep_pickup_datetime"
refactor_column2 = "tpep_dropoff_datetime"

if config['modules']['CreateDataBase'] == 'Y':
    sqlite = sqlite3.connect(f"data\\{config['local']['db']}")
    db = pandas.read_csv(f"data\\{config['local']['csv']}")
    db[refactor_column1] = pandas.to_datetime(db[refactor_column1])
    db[refactor_column2] = pandas.to_datetime(db[refactor_column2])
    db.to_sql(config['local']['table'], sqlite, if_exists="replace", chunksize=1000, index=False)
    sqlite.close()
    
if config['modules']['CreatePostgres'] == 'Y':
    engine = sqlalchemy.create_engine(postgres)
    db = pandas.read_csv(f"data\\{config['local']['csv']}")
    db[refactor_column1] = pandas.to_datetime(db[refactor_column1])
    db[refactor_column2] = pandas.to_datetime(db[refactor_column2])
    db.to_sql(config['local']['table'], engine, if_exists="replace", chunksize=1000, index=False)
    engine.dispose()

if config['modules']['TestSQLite'] == 'Y':
    print("Result for SQlite3")
    print(testSqlite3.test())
if config['modules']['TestPsycorg'] == 'Y':
    print("Result for Psycorg2")
    print(testPsycorg2.test())
if config['modules']['TestDuckDB'] == 'Y':
    print("Result for DuckDB")
    print(testDuckDB.test())

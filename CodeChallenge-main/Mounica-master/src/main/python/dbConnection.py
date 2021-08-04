import psycopg2 as p
import configparser as cp
from sqlalchemy import create_engine


confPath = '../../../resources/config/application.properties'
env = "dev"

#get variables from config File
props = cp.RawConfigParser()
props.read(confPath)

def read_config(paramtr : str):
    return props.get(env, paramtr)

def db_connection_open(conn_type):
    try:
        host = read_config('host')
        dbname = read_config('dbname')
        username= read_config('usernme')
        password= read_config('passwrd')
        if conn_type == "regular":
            conn = p.connect(f"host={host} dbname={dbname} user={username} password={password}")
            conn.set_session(autocommit=True)
            cur = conn.cursor()
            return conn, cur
        elif conn_type == "alchemy":
            alchemyEngine = create_engine(f'postgresql+psycopg2://{username}:{password}@{host}/{dbname}', pool_recycle=3600)
            conn = alchemyEngine.connect()
            cur = ''
            return conn, cur
        elif conn_type == "jdbc":
            conn = f"jdbc:postgresql://{host}:5432/{dbname}"
            properties = {"user": username, "password": password, "driver": "org.postgresql.Driver", "batchsize": "100000"}
            return conn, properties
    except p.Error as e:
        print("connection failed")
        print (e)



def db_connection_close(conn_type, conn, cur):
    try:
        if conn_type == "regular":
            cur.close()
            conn.close()
        else:
            conn.close()
    except p.Error as e:
        print("connection close failed")
        print (e)


def create_table(cur):
    try:
        cur.execute(
            """create table if not exists Covid_Data_Analysis (submission_date date,
                                state varchar,
                                total_cases int,
                                new_case int,
                                total_deaths int,
                                new_death int,
                                covid_case_rate varchar,
                                covid_death_rate varchar)
             """)
    except p.Error as e:
        print("Error while creating table songs")
        print(e)

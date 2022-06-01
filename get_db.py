from sqlalchemy import create_engine
import os



def get_db(config_dict):
    try:
        host=config_dict['db_host']
        port=config_dict['db_port']
        database=config_dict['db_database']
        user=config_dict['db_user']
        password=config_dict['db_pass']

        engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}", 
                                executemany_mode='batch')

        connection = engine.connect()
        return connection
        print(f'[PID:{os.getpid()}] DB is connected')                        

    except Exception as ex:
        raise ex    



config_dict= {
        "db_host":"localhost",
        "db_port":"5432",
        "db_database":"suri",
        "db_user":"postgres",
        "db_pass":"admin"
}        


con=get_db(config_dict)

# result=con.execute("select * from accounts")
# for x in result:
#     print(x)


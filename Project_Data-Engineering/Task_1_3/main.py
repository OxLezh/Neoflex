from sqlalchemy import create_engine, text
import pandas as pd
import time
from os import getenv
from dotenv import load_dotenv, find_dotenv #(python-dotenv)
import sys
from pathlib import Path

# Для использования переменных окружения.
load_dotenv(find_dotenv())

# Переменные окружения. Создайте в корне папки файл .env и укажите значения переменных для 
# подключения к базе PostgreSQL (шаблон в env _example.txt). Можно заполнить вручную, например "DB_NAME = postgres".

DB_NAME = getenv("DB_NAME")
DB_HOST=getenv("DB_HOST")
DB_PORT=getenv("DB_PORT")
DB_USER=getenv("DB_USER")
DB_PASSWORD=getenv("DB_PASSWORD")
 
# Название таблиц и схемы в PostgreSQL.
schema = 'dm'
table_name = 'dm_f101_round_f'
copy_table_name = 'dm_f101_round_f_2'

# Путь к файлу .csv
file_to_open = Path("Project_Data-Engineering/Task_1_3/data")/ f"{table_name}.csv" 
  
def logging(status, engine, description='', error=''):

    """ Загрузка данных в таблицу логов. """   
    with engine.connect() as conn:       
        conn.execute(text(f"""insert into logs.extract_load_logs (status, description, error)
                          values ('{status}', '{description}', '{error}');"""))
        conn.commit()         

def exist_table(engine):

    """ Проверка подключения к PostgreSQL, существования таблиц, создание таблицы логов. """   
    try:        
        with engine.connect() as conn:           
            #Проверка существования таблицы с  данными     
            exist_table =  conn.execute(text(f"""select exists(
                                            select * 
                                            from information_schema.tables
                                            where table_schema = '{schema}' 
                                            and table_name = '{table_name}')""")).first()[0]    
            #Создание таблицы логов         
            conn.execute(text(f"""create table if not exists logs.extract_load_logs(
                                    action_date timestamp not null default now(),
                                    status varchar(40),
                                    description text,
                                    error text);"""))   
            conn.commit()                          
            if exist_table is False:
                logging('error_exist_table', engine, f'Таблицы {schema}.{table_name} не существует.')
                print(f"\nТаблицы c данными {schema}.{table_name} в PostgreSQL не существует.\n")                          
            return exist_table
    except:       
        print(f'ОШИБКА соединения c PostgreSQL: \n {sys.exc_info()}\n')
 

def extract_PostgreSQL(engine):

    """ Извлечение данных из PostgreSQL. """   
    try:
        df = pd.read_sql_query(f'select * from {schema}.{table_name}', con=engine) 
        if not df.empty:                 
            logging('extract_PostgreSQL', engine,  f'Извлечение данных из таблицы {schema}.{table_name} PostgreSQL прошло успешно.', '')
            print(f'Извлечение данных из таблицы {schema}.{table_name} PostgreSQL прошло успешно.')                
            return df
        else:
            logging('error_extract_PostgreSQL', engine,  f'ОШИБКА. В таблице {schema}.{table_name} отстутсвуют данные.', '') 
            print(f'ОШИБКА. В таблице {schema}.{table_name} отстутсвуют данные.')              
    except:
        logging('error_extract_PostgreSQL', engine,  f'ОШИБКА извлечения данных из таблицы {schema}.{table_name} PostgreSQL',\
                str(sys.exc_info()).replace("'", '')) 
        print(f'Ошибка извлечения данных из таблицы {schema}.{table_name} PostgreSQL: \n {sys.exc_info()}')
  

def upload_to_csv(engine, df):
     
    """ Загрузка данных в csv файл. """   
    try:
        df.to_csv(file_to_open, sep=';', encoding='utf-8', index=False)          
        logging('upload_to_csv', engine, f'Данные успешно загружены в файл {table_name}.csv.')
        print(f'Данные успешно загружены в файл {table_name}.csv.\n')         
    except:
        logging('error_upload_to_csv', engine, f'ОШИБКА загрузки данных в файл {table_name}.csv.',\
                str(sys.exc_info()).replace("'", '')) 
        print(f"Ошибка загрузки данных в файл {table_name}.csv.", str(sys.exc_info()).replace("'", '') ) 


def upload_PostgreSQL(engine):

    """ Загрузка данных из .csv в PostgreSQL. """        
    try:
        df = pd.read_csv(file_to_open, delimiter=';', encoding='utf-8')          
        if not df.empty:    
            with engine.connect() as conn:       
                conn.execute(text(f"""drop table if exists {schema}.{copy_table_name};"""))
                conn.execute(text(f"""create table if not exists {schema}.{copy_table_name} as table {schema}.{table_name} with no data;"""))
                conn.commit()     
            df.to_sql(copy_table_name, engine, if_exists='append', schema=schema, index=False)   
            logging('upload_PostgreSQL', engine, f'Данные в таблицу {schema}{copy_table_name} успешно загружены.')
            print(f'Данные в таблицу {schema}.{copy_table_name} загружены.\n') 
        else:
            logging('error_upload_PostgreSQL', engine,  f'ОШИБКА. В файле {file_to_open}.csv отстутсвуют данные', '') 
            print(f'ОШИБКА. В файле {file_to_open}.csv отстутсвуют данные.')  
    except:       
        logging('error_upload_PostgreSQL', engine, f'ОШИБКА загрузки данных в таблицу {schema}.{copy_table_name}',\
                str(sys.exc_info()).replace("'", '')) 
        print(f"Ошибка загрузки данных в таблицу {schema}.{copy_table_name}.", str(sys.exc_info()).replace("'", '') ) 


def main():

    start_time = time.time()       
    engine = create_engine(f'postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
    exis_table = exist_table(engine)        
    if exis_table is True: 
        choice = 0
        while choice != 1 or choice != 2: 
            try:    
                choice = int(input(f'\nЕсли вы хотите загрузить данные в файл {table_name}.csv - введите цифру 1.'
                        f'\nЕсли необходимо загрузить данные в таблицу {copy_table_name} в PostreSQL - введите цифру 2.\n'
                        f'Для выхода введите цифру 3: \n'))                   
            except:  
                pass             
            if choice == 1:
                df = extract_PostgreSQL(engine)                                                         
                if df is not None:   
                    upload_to_csv(engine, df)                    
                    break 
                else:             
                    break                                     
            elif choice == 2:
                upload_PostgreSQL(engine)                
                break  
            elif choice == 3:
                print('Выход из программы.')               
                break                                                                             
            else:
                print("ВВедите, пожалуйста, цифру 1, 2 или 3 для выхода.") 
    
    print("--- %s seconds ---" % (time.time() - start_time))


if __name__ == '__main__':
    main()
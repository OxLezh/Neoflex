from sqlalchemy import create_engine, text
import pandas as pd
import time
from os import getenv
from dotenv import load_dotenv, find_dotenv #(python-dotenv)
import sys
from pathlib import Path
load_dotenv(find_dotenv()) # Для использования переменных окружения.


def connect_postgreSQL():
    """
    Провека подключения к PosgreSQL.
    """
    #Переменные окружения
    DB_NAME = getenv("DB_NAME")
    DB_HOST=getenv("DB_HOST")
    DB_PORT=getenv("DB_PORT")
    DB_USER=getenv("DB_USER")
    DB_PASSWORD=getenv("DB_PASSWORD")
    engine = create_engine(f'postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
    try:
        engine.connect()
        print("\nПодключение к PostgreSQL успешно.\n") 
        return engine  
    except:        
        print(f'\nОШИБКА соединения c PostgreSQL: \n {sys.exc_info()}\n')


def сreate_table_logging(engine):    
    """
    Создание таблицы логов.
    """
    with engine.connect() as conn:           
        conn.execute(text(f"""create table if not exists logs.extract_load_logs(
                                action_date timestamp not null default now(),
                                status varchar(40),
                                description text,
                                error text);"""))         
        conn.commit() 
        conn.close() 


def logging(status, engine, description='', error=''):
    """
    Загрузка данных в таблицу логов.
    """   
    with engine.connect() as conn:              
        conn.execute(text(f"""insert into logs.extract_load_logs (status, description, error)
                          values ('{status}', '{description}', '{error}');"""))        
        conn.commit()
        conn.close()       
           

def exist_table(engine, schema, table_name):
    """
    Проверка существования витрины в PostgreSQL и данных в ней.
    """            
    with engine.connect() as conn:        
        exist_table =  conn.execute(text(f"""select exists(
                                        select * 
                                        from information_schema.tables
                                        where table_schema = '{schema}' 
                                        and table_name = '{table_name}')""")).first()[0]                
        if exist_table:
            data = conn.execute(text(f"""select * from {schema}.{table_name};""")).first()   
            if data != None:
                return True  
            else:
                logging('error_exist_table', engine,  f'ОШИБКА. В витрине {schema}.{table_name} отстутсвуют данные.', '') 
                print(f'ОШИБКА. В витрине {schema}.{table_name} отстутсвуют данные.\n')  
        conn.close()                             
        if not exist_table:
            logging('error_exist_table', engine, f'ОШИБКА. Витрины {schema}.{table_name} не существует.')
            print(f"ОШИБКА. Витрины {schema}.{table_name} в PostgreSQL не существует.\n")              
        

def extract_PostgreSQL(engine, schema, table_name):
    """
    Извлечение данных из PostgreSQL.
    """      
    df = pd.read_sql_query(f'select * from {schema}.{table_name}', con=engine)                     
    logging('extract_PostgreSQL', engine,  f'Извлечение данных из витрины {schema}.{table_name} PostgreSQL прошло успешно.', '')
    print(f'Извлечение данных из витрины {schema}.{table_name} PostgreSQL прошло успешно.\n')                
    return df          
    
 
def upload_to_csv(engine, df, table_name, file_to_open):  
    """
    Загрузка данных в csv файл.
    """            
    df.to_csv(file_to_open, sep=';', encoding='utf-8', index=False)          
    logging('upload_to_csv', engine, f'Данные загружены в файл {table_name}.csv.')
    print(f'Данные загружены в файл {table_name}.csv.\n')         
    

def upload_PostgreSQL(engine, schema, table_name, copy_table_name, file_to_open):
    """
    Выгрузка данных из .csv в PostgreSQL.
    """  
    try:
        df = pd.read_csv(file_to_open, delimiter=';', encoding='utf-8')
        with engine.connect() as conn:       
            conn.execute(text(f"""drop table if exists {schema}.{copy_table_name};"""))
            conn.execute(text(f"""create table if not exists {schema}.{copy_table_name} as table {schema}.{table_name} with no data;"""))
            conn.commit()     
            conn.close() 
        df.to_sql(copy_table_name, engine, if_exists='append', schema=schema, index=False)   
        logging('upload_PostgreSQL', engine, f'Данные в таблицу {schema}{copy_table_name} успешно загружены.')
        print(f'Данные в таблицу {schema}.{copy_table_name} загружены.\n') 
    except:
        logging('error_upload_PostgreSQL', engine, f'ОШИБКА выгрузки данных из файла .csv.')
        print(f"ОШИБКА выгрузки данных из файла .csv.\n") 


def main():
    """
    Запуск программы.
    """
    start_time = time.time()    
    # Название таблиц, схем в PostgreSQL.
    schema = 'dm'
    table_name = 'dm_f101_round_f'
    copy_table_name = 'dm_f101_round_f_2'
    # Путь к файлу.
    file_to_open = Path("Project_Data-Engineering/Task_1_3/data")/ f"{table_name}.csv"    
    keyboard_input = 0
    while keyboard_input not in [1,2,3]: 
        try:
            keyboard_input = int(input(
            f'Введите 1 - загрузка данных в файл {table_name}.csv.\n'
            f'Введите 2 - загрузка данных в таблицу {copy_table_name} в PostreSQL.\n'
            f'Введите 3  - выход: \n'))
        except:
            pass        
        if keyboard_input ==3:
            print('Конец программы.')
            break 
        if keyboard_input not in [1,2,3]:
            print('\nНеверный ввод.\n')
            continue
        engine = connect_postgreSQL() 
        if engine:
            сreate_table_logging(engine)                   
            if exist_table(engine, schema, table_name):                                  
                if keyboard_input ==1:                
                    df = extract_PostgreSQL(engine, schema, table_name)
                    upload_to_csv(engine, df, table_name, file_to_open)
                    print("--- %s seconds ---" % (time.time() - start_time)) 
                if keyboard_input ==2:    
                    upload_PostgreSQL(engine, schema, table_name, copy_table_name, file_to_open)
                    print("--- %s seconds ---" % (time.time() - start_time)) 
                  

if __name__ == '__main__':
    main()
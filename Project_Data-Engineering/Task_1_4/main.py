from sqlalchemy import create_engine, text
import pandas as pd
import time
from os import getenv
from dotenv import load_dotenv, find_dotenv #(python-dotenv)
import sys
from pathlib import Path
import datetime
load_dotenv(find_dotenv()) # Для использования переменных окружения.


def connect_postgreSQL():
    """
    Провека подключения к PosgreSQL
    """
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
        print(f'ОШИБКА соединения c PostgreSQL: \n {sys.exc_info()}\n')


def сreate_table_logging(engine):
    """
    Создание таблицы логов.
    """
    with engine.connect() as conn:          
        conn.execute(text(f"""create table if not exists logs.extract_function_logs(
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
        conn.execute(text(f"""insert into logs.extract_function_logs (status, description, error)
                          values ('{status}', '{description}', '{error}');"""))
        conn.commit()
        conn.close()         


def exist_table(engine, schema, table_name):
    """
    Проверка существования таблицы в PostgreSQL и данных в ней.
    """   
    #Проверка существования таблицы с данными.       
    with engine.connect() as conn:            
        exist_table =  conn.execute(text(f"""select exists
                                        (select * 
                                        from information_schema.tables
                                        where table_schema = '{schema}' 
                                        and table_name = '{table_name}')""")).first()[0] 
        print((exist_table))
        #Проверка наличия данных в таблице.
        if exist_table:
            data = conn.execute(text(f"""select * from {schema}.{table_name};""")).first()   
            if data != None:
                return True
            else:
                logging('error_exist_object', engine, f'Нет данных в таблице  {schema}.{table_name}.')
                print(f'Нет данных в таблице  {schema}.{table_name}.\n')                           
        conn.close()    
        if not exist_table:
            logging('error_exist_object', engine, f'Таблицы {schema}.{table_name} не существует.')
            print(f"Таблицы c данными {schema}.{table_name} в PostgreSQL не существует.\n")         
         
      
def exist_function(engine, schema2, function_name):
    """
    Проверка cуществования функции в PostgreSQL.
    """    
    #Проверка существования таблицы с данными.       
    with engine.connect() as conn:           
        exist_function =  conn.execute(text(f"""select exists
                                        (select * 
                                        from pg_proc 
                                        where proname = '{function_name}')""")).first()[0]          
        conn.close() 
        if exist_function:
            return True            
        if not exist_function:
            logging('error_exist_object', engine, f'Функции {schema2}.{function_name} не существует.')
            print(f"\nФункции {schema2}.{function_name} не существует.\n")      


def start_function(engine, schema, table_name, schema2, function_name, date):
    """
    Функция в PostgreSQL - принимает дату и возвращает дату, информацию о максимальной и минимальной сумме проводки 
    по кредиту и по дебету за эту дату.
    """     
    #Создание датафрейма и извлечение данных с использованием функции в PostgreSQL.
    df = pd.read_sql_query(f"""select * from {schema2}.{function_name}('{date}')""", con=engine)
    #Проверка, что в датафрейме есть данные.          
    if not df.empty:                 
        logging('start_function', engine,  f'ФУнкция {schema2}.{function_name} отработала успешно.', '')
        print(f'\nФУнкция {schema2}.{function_name} отработала успешно.\n')                
        return df
    else:
        logging('error_start_function', engine,  f'ОШИБКА. В таблице {schema}.{table_name} отстутсвуют данные за указанную дату.', '') 
        print(f'\nОШИБКА. В таблице {schema}.{table_name} отстутсвуют данные за указанную дату.\n') 


def upload_to_csv(engine, df, date, function_name):
    """
    Загрузка данных в csv файл.
    """           
    # Путь к файлу .csv и его название
    file_to_open = Path("Project_Data-Engineering/Task_1_4/data")/ f"{date}_{function_name}.csv" 
    #Запись данных в файл .csv
    df.to_csv(file_to_open, sep=';', encoding='utf-8', index=False)          
    logging('upload_to_csv', engine, f'Данные успешно загружены в файл {function_name}.csv.')
    print(f'Данные загружены в файл {function_name}.csv.\n')      


def date_validation():
    """
    Проверка корректности даты.
    """
    date = input(f'Введите дату проводки в формате 2018-01-01: ')
    try:
        datetime.datetime.strptime(date, '%Y-%m-%d')       
        return date
    except ValueError:
        print("Некорректный формат даты.")


def main():
    """
    Запуск программы.
    """
    start_time = time.time()    
    # Название таблиц, схем в PostgreSQL.
    schema = 'ds'
    table_name = 'ft_posting_f'
    schema2 = 'dm'
    function_name = 'func_ds_ft_posting_f'

    engine = connect_postgreSQL()   

    if engine:                   
        if exist_table(engine, schema, table_name):  
            if exist_function(engine, schema2, function_name):
                date = date_validation()    
                if date:       
                    df = start_function(engine, schema, table_name, schema2, function_name, date)
                    if df is not None:         
                        upload_to_csv(engine, df, date, function_name) 
                        print("--- %s seconds ---" % (time.time() - start_time))     


if __name__ == '__main__':
    main()
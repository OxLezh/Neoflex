from sqlalchemy import create_engine, text
import pandas as pd
import time
from os import getenv
from dotenv import load_dotenv, find_dotenv #(python-dotenv)
import sys
from pathlib import Path
load_dotenv(find_dotenv()) # Для использования переменных окружения.


def logging(status, engine, description='', error=''):

    """ Загрузка данных в таблицу логов. """   

    with engine.connect() as conn:   
        #Создание таблицы для логов.
        conn.execute(text(f"""create table if not exists logs.extract_function_logs(
                                action_date timestamp not null default now(),
                                status varchar(40),
                                description text,
                                error text);"""))    
        conn.execute(text(f"""insert into logs.extract_function_logs (status, description, error)
                          values ('{status}', '{description}', '{error}');"""))
        conn.commit()         

def exist_table_function(engine, schema, table_name, schema2, function_name):

    """ Проверка подключения к PostgreSQL, существования таблицы с данными, функции. """   
    try: 
        #Проверка существования таблицы с данными.       
        with engine.connect() as conn:            
            exist_table =  conn.execute(text(f"""select exists
                                            (select * 
                                            from information_schema.tables
                                            where table_schema = '{schema}' 
                                            and table_name = '{table_name}')""")).first()[0] 
            #Проверка наличия данных в таблице.
            data = conn.execute(text(f"""select * from ds.ft_posting_f;""")).first()                      
            #Проверка существования функции.   
            exist_function =  conn.execute(text(f"""select exists
                                            (select * 
                                            from pg_proc 
                                            where proname = '{function_name}')""")).first()[0]          
            conn.commit()                          
            if exist_table is False:
                logging('error_exist_table', engine, f'Таблицы {schema}.{table_name} не существует.')
                print(f"\nТаблицы c данными {schema}.{table_name} в PostgreSQL не существует.\n")                          
           
            elif exist_function is False:
                logging('error_exist_table', engine, f'Функции {schema2}.{function_name} не существует.')
                print(f"\nФункции {schema2}.{function_name} не существует.\n")

            elif data == None:
                logging('error_exist_table', engine, f'Нет данных в таблице  {schema}.{table_name}.')
                print(f'\nНет данных в таблице  {schema}.{table_name}.\n')  
            
            return exist_table, exist_function, data
    except:       
        print(f'ОШИБКА соединения c PostgreSQL: \n {sys.exc_info()}\n')

 
def start_function(engine, schema, table_name, schema2, function_name, date):

    """ Функция в PostgreSQL принимает дату и возвращает эту дату и информацию о максимальной и минимальной сумме проводки 
        по кредиту и по дебету за переданную дату """   
    try:
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
    except:
        logging('error_start_function', engine,  f'ОШИБКА извлечения данных из таблицы {schema}.{table_name} PostgreSQL',\
                 str(sys.exc_info()).replace("'", '')) 
        print(f'Ошибка работы функции {schema2}.{function_name} PostgreSQL: \n {sys.exc_info()}')        


def upload_to_csv(engine, df, date, function_name):
     
    """ Загрузка данных в csv файл. """   
    try:          
        # Путь к файлу .csv и его название
        file_to_open = Path("Project_Data-Engineering/Task_1_4/data")/ f"{date}_{function_name}.csv" 
        #Запись данных в файл .csv
        df.to_csv(file_to_open, sep=';', encoding='utf-8', index=False)          
        logging('upload_to_csv', engine, f'Данные успешно загружены в файл {function_name}.csv.')
        print(f'Данные загружены в файл {function_name}.csv.\n')         
    except:
        logging('error_upload_to_csv', engine, f'ОШИБКА загрузки данных в файл {function_name}.csv.',\
                str(sys.exc_info()).replace("'", '')) 
        print(f"Ошибка загрузки данных в файл {function_name}.csv.", str(sys.exc_info()).replace("'", '') ) 


def main(date):

    start_time = time.time()    
    # Название таблиц, схем в PostgreSQL.
    schema = 'ds'
    table_name = 'ft_posting_f'
    schema2 = 'dm'
    function_name = 'func_ds_ft_posting_f'

    #Переменные окружения для PostgreSQL:
    DB_NAME = getenv("DB_NAME")
    DB_HOST=getenv("DB_HOST")
    DB_PORT=getenv("DB_PORT")
    DB_USER=getenv("DB_USER")
    DB_PASSWORD=getenv("DB_PASSWORD")

    try:
        engine = create_engine(f'postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
        exis_table, exis_function, data_empty = exist_table_function(engine, schema, table_name, schema2, function_name)         
        if exis_table is True and exis_function is True and data_empty != None: 
            df = start_function(engine, schema, table_name, schema2, function_name, date)
            if df is not None:         
                upload_to_csv(engine, df, date, function_name)    
    except:
        pass
        
    print("--- %s seconds ---" % (time.time() - start_time))


if __name__ == '__main__':
    main( '2018-01-09')
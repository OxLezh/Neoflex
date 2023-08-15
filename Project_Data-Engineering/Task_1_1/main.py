from sqlalchemy import create_engine, text
import pandas as pd
import time
from os import getenv
from dotenv import load_dotenv, find_dotenv #(python-dotenv)
import sys
from pathlib import Path
# Для использования переменных окружения.
load_dotenv(find_dotenv())


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
        conn.execute(text(f"""create table if not exists logs.load_logs(
                                sources varchar(40),
	                            action_date timestamp not null default now(),
	                            status varchar(20),
	                            description text,
	                            error text);"""))         
        conn.commit() 
        conn.close() 


def logging(table_name, status, engine, description='', error=''):
    """ 
    Загрузка данных в таблицу логов.
    """   
    with engine.connect() as conn:       
        conn.execute(text(f"""insert into logs.load_logs (sources, status, description, error)
                            values ('{table_name}', '{status}', '{description}', '{error}');"""))
        conn.commit()
        conn.close()          


def exist_table(table_name, engine, schema):
    """
    Проверка существования таблиц в PostgreSQL.
    """   
     #Проверка существования таблицы с данными.       
    with engine.connect() as conn:            
        exist_table = (conn.execute(text(f"""select exists
                                        (select * 
                                        from information_schema.tables
                                        where table_schema = '{schema}' 
                                        and table_name = '{table_name}')"""))).first()[0] 
        conn.close()     
        if not exist_table:            
            logging(table_name,'error_exist_object', engine, f'Таблицы {schema}.{table_name} не существует.')
            print(f"Таблицы c данными {schema}.{table_name} в PostgreSQL не существует.\n")  
    return exist_table


def extract_data(table_name, engine, path):
    """
    Извлечение данных из csv файлов.
    """   
    try:        
        df = pd.read_csv(f'{path}/{table_name}.csv', delimiter=';', encoding='cp866', keep_default_na=False)
        df.columns = df.columns.str.lower()
        df = df.iloc[:, 1:len(df.axes[1])]  # Выбираем только нужные колонки.
        if table_name == 'ft_posting_f':
            df = df.groupby(['oper_date', 'credit_account_rk','debet_account_rk'])[['credit_amount', 'debet_amount']].sum().reset_index()
        elif table_name == 'md_exchange_rate_d':
            df = df.drop_duplicates()
        elif table_name == 'ft_balance_f':
            df['on_date'] = pd.to_datetime(df['on_date'], format='%d.%m.%Y')                    
        return df
    except:
        logging(table_name, 'error_extract', engine,  f'ОШИБКА извлечения данных из файла {table_name}.csv', str(sys.exc_info()).replace("'", '')) 
        print(f'Ошибка извлечения данных из файла {table_name}.csv: \n {sys.exc_info()}')
    
      
def upload_data(table_name, engine, df):
    """
    Загрузка данных из датафрейма в PostgreSQL.
    """    
    try:        
        logging(table_name, 'start_upload', engine, f'Старт загрузки данных в таблицу {table_name}')
        time.sleep(2)        
        with engine.connect() as conn:
            # Получаем список первичных ключей:
            column_pk_q =  conn.execute(text(f"""SELECT c.column_name
                FROM information_schema.table_constraints tc 
                JOIN information_schema.constraint_column_usage AS ccu USING (constraint_schema, constraint_name) 
                JOIN information_schema.columns AS c ON c.table_schema = tc.constraint_schema
                AND tc.table_name = c.table_name AND ccu.column_name = c.column_name
                WHERE constraint_type = 'PRIMARY KEY' and tc.table_name = '{table_name}';"""))
            # Извлекаем из датафрейма необходимые колонки, значения и т.д.
            column_pk_list = ([f'{i[0]}' for i in column_pk_q])         
            column_pk = ", ".join(column_pk_list)
            column_list = [f'{i}' for i in df.columns]
            columns = ", ".join(column_list)            
            values = ','.join([str(i) for i in list(df.to_records(index=False))])                          
            columns_no_pk_list = ([item for item in column_list if item not in column_pk_list])
            columns_no_pk = ", ".join(columns_no_pk_list)          
            columns_excluded = ", ".join(['excluded.' + direction for direction in columns_no_pk_list])              
            # Загрузка данных в PostgreSQL.
            conn.execute(text(f"""insert into ds.{table_name} ({columns})  values {values} 
                ON CONFLICT ({column_pk}) DO UPDATE SET ({columns_no_pk})  = ({columns_excluded});"""))
            conn.commit()
            conn.close()
            logging(table_name, 'end_upload', engine, f'Данные в таблицу {table_name} загружены.')
            print(f'Данные в таблицу {table_name} загружены.\n') 
    except:       
        logging(table_name, 'error_upload', engine, f'ОШИБКА загрузки данных в таблицу {table_name}', str(sys.exc_info()).replace("'", '')) 
        print("Ошибка загрузки данных в таблицу", table_name,".", str(sys.exc_info()).replace("'", '') ) 

def main():  

    # Список с названиями таблиц для загрузки данных. 
    tables_name = [
            "ft_balance_f",
            "ft_posting_f",
            "md_account_d",
            "md_currency_d",
            "md_exchange_rate_d",
            "md_ledger_account_s"
             ]
    schema = 'ds'
    path = Path("Project_Data-Engineering/Task_1_1/data")
    start_time = time.time()
    engine = connect_postgreSQL()   

    if engine:
        сreate_table_logging(engine)      
        for name in tables_name:   
           if exist_table(name, engine, schema):                           
                df = extract_data(name, engine, path)
                if df is not None:
                    upload_data(name, engine, df)   
    print("--- %s seconds ---" % (time.time() - start_time))                 
          
   
    

if __name__ == '__main__':
    main()




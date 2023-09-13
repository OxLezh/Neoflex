from pathlib import Path
import os
import time
import pyspark
from delta import * #(delta-spark)
from dotenv import load_dotenv, find_dotenv #(python-dotenv)
from os import getenv
from sqlalchemy import create_engine, text
import sys
import pandas as pd


def connect_postgreSQL():
    """
    Провека подключения к PosgreSQL
    """
    try:
        # Для использования переменных окружения.
        load_dotenv(find_dotenv()) 
        # Переменные окружения
        DB_NAME = getenv("DB_NAME")
        DB_HOST=getenv("DB_HOST")
        DB_PORT=getenv("DB_PORT")
        DB_USER=getenv("DB_USER")
        DB_PASSWORD=getenv("DB_PASSWORD")
        engine = create_engine(f'postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
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
        conn.execute(text(f"""create table if not exists logs.etl_spark(
                                action_date timestamp not null default now(),
                                status varchar(40),
                                delta_id int,
                                table_name  varchar(40));"""))         
        conn.commit() 
        conn.close() 


def logging(status, engine, delta_id, table_name):
    """
    Загрузка данных в таблицу логов.
    """   
    with engine.connect() as conn:             
        conn.execute(text(f"""insert into logs.etl_spark(status, delta_id, table_name)
                          values ('{status}', {delta_id}, '{table_name}');"""))
        conn.commit()
        conn.close()    


def Spark_Session():
    """
    Cоздает объект SparkSession для работы co Spark.
    """
    builder = (
        pyspark.sql.SparkSession.builder.appName("MyApp").enableHiveSupport()
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog",))
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    return spark


def input_path():
    """
    Получает от пользователя путь к папке data_deltas и проверяет его корректность
    """
    path = ''
    while path is not True:
        path = input(
            f"Введите путь к директории с дельтами:\n"
            f"Для выхода нажмите 4.\n").strip()    
        if path == "4":
            return
        else:
            try:
                delta_list = sorted(os.listdir(path))                            
                if delta_list == []:
                    print("В папке отсутвуют данные.")                
                else:
                    return path, delta_list
            except:
                print("\nВведен некорректный путь.\n")
                
        
def input_table_name():
    """
    Получает от пользователя название таблицы.
    """
    table_name = ''
    while table_name is not True:
        table_name = input(
            f"\nВведите название таблицы:\n"
            f"Для выхода нажмите 4.\n").strip()    
        if table_name == "4":
            return
        else:
            if table_name == '':
                pass
            else:
                return table_name

def input_pk(spark):
    """
    Получает от пользователя первичные ключи и проверяет их корректнность.   
    """
    pk = ''
    while pk is not True:
        pk = (input(
            f'\nВведите название полей, которые являются первичными ключами через запятую:\n'
            f'Для выхода нажмите 4.\n')) 
        if pk == "4":
            return        
        pk = pk.upper().replace(" ",'').split(",")                 
        df = spark.read.options(header='True', delimiter=';', inferSchema='True')\
            .csv(f"{Path(sys.path[0],'mirr_md_account_d')}").columns  
        my_list = []       
        for value_pk in pk:                              
            if value_pk in df:                    
                my_list.append(value_pk) 
            else:
                print("\nОШИБКА. В зеркале нет поля", f"'{value_pk}'\n")
        if len(my_list) == len(pk):
            return pk                    
                    

def create_empty_mirror(spark, path, delta_list):    
    """
    Если зеркала нет в mirr_md_account_d,
    то создается пустое зеркало в файле csv.   
    """
    # Получаем список файлов в папке mirr_md_account_d:
    del_list = sorted(os.listdir(f"{Path(sys.path[0],'mirr_md_account_d')}"))    
    if del_list == [] or del_list ==['.gitignore']: 
        # Копируем структуру таблицы из первой дельты:           
        file_name = "".join(os.listdir(f"{path}/{delta_list[0]}/"))         
        df_mirror = spark.read.options(header='True', delimiter=';', inferSchema='True')\
            .csv(f"{Path(path,delta_list[0], file_name)}")
        df_mirror = df_mirror.limit(0)  
        df_mirror.repartition(1).write.mode("overwrite")\
    .options(header='True', delimiter=';').csv(f"{Path(sys.path[0],'mirr_md_account_d')}")      
   
   
def mirror_to_parquet(spark):    
    """
    Сохраняет зеркало в формате parquet. 
    """
    df_mirror = spark.read.options(header='True', delimiter=';', inferSchema='True')\
            .csv(f"{Path(sys.path[0],'mirr_md_account_d')}")  
    # Сохраняем в формате parquet:    
    df_mirror.repartition(1).write.mode("overwrite").format("parquet").save(f"{Path(sys.path[0],'data_out')}.parquet") 
   


def log_delta_id_last(engine, schema, table_name_log):
    """
    Возвращает id последней загруженной дельты из таблицы логов.
    """   
    max_delta_id = pd.read_sql_query(f'select max(delta_id) from {schema}.{table_name_log}', con=engine).values[0][0]    
    if max_delta_id is None:
        return 0
    else:
        return max_delta_id  


def merge_mirror_delta(max_delta_id, spark, path, engine, table_name, pk, delta_list):
    """
    Обновляет зеркало данными из дельт.
    """ 
    deltaTable = DeltaTable.convertToDelta(spark, f"parquet.`{Path(sys.path[0],'data_out')}.parquet`")         
    for delta_id in delta_list:   
        # Проверяем загружались ли дельты ранее.         
        if int(delta_id) > max_delta_id:             
            file_name = "".join(os.listdir(f"{path}/{delta_id}/"))    
            logging('start', engine,  delta_id, table_name)
            # Генерируем Dataframe c данными из конкретной дельты.
            df_delta = spark.read.options(header='True', delimiter=';', inferSchema='True')\
                    .csv(f"{Path(path,delta_id,file_name)}")                     
            condition = ''
            for i in range(len(pk)):    
                condition = f"""mirror.{pk[i]} = delta.{pk[i]} and {condition}"""                    
            conditions = condition[:-5]   
            #merge:          
            deltaTable.alias("mirror").merge(
            source = df_delta.alias("delta"),
            condition = conditions) \
                .whenMatchedUpdateAll() \
                .whenNotMatchedInsertAll() \
                .execute()
            df = deltaTable.toDF()          
            print(delta_id)
            df.show(100)
            logging('end', engine, delta_id, table_name)            
    try:
        return df
    except:
        print("\nВсе дельты уже обработаны.\n")              
   
        
def upload_to_csv(df):
    """
    Загружает зеркало в csv файл.
    """       
    df.repartition(1).write.mode("overwrite")\
    .options(header='True', delimiter=';').csv(f"{Path(sys.path[0],'mirr_md_account_d')}")   

     
def read_to_csv(spark):
    """
    Отображает данные из зеркала.
    """       
    df = spark.read.options(header='True', delimiter=';', inferSchema='True')\
            .csv(f"{Path(sys.path[0],'mirr_md_account_d')}")
    df.show(50)



def show_logs_PostgreSQL(engine, schema, table_name_log, spark):
    """
    Отображает данные из таблицы логов в PostgreSQL.
    """       
    table_log = pd.read_sql_query(f'select * from {schema}.{table_name_log}', con=engine)
    if not table_log.empty:   
        table_log = spark.createDataFrame(table_log)   
        table_log.show(50, False) 
    else:
        print("\nДанные в таблице логов отсутствуют.\n")  


def main():
    """
    Запуск программы
    """
    start_time = time.time() 
    schema = 'logs'
    table_name_log = 'etl_spark' 
    keyboard_input = 0 
    spark = Spark_Session()    
    engine = connect_postgreSQL() 
    if engine:         
        try:
            path, delta_list = input_path()          
        except:
            return
        create_empty_mirror(spark, path, delta_list)  
        mirror_to_parquet(spark) 
        table_name = input_table_name()   
        if table_name:        
            pk = input_pk(spark)
            if pk:            
                while keyboard_input !=4: 
                    try:
                        keyboard_input = int(input(
                        f'\nВведите 1 - если хотите обновить зеркало таблицы {table_name} данными из дельт.\n'
                        f'Введите 2 - если хотите открыть файл с зеркалом таблицы {table_name}.\n'
                        f'Введите 3 - если хотите получить данные из таблицы логов.  \n'
                        f'Введите 4  - выход: \n'))
                    except:
                        pass  
                    if keyboard_input ==4:
                            print('Конец программы.')
                            break 
                    if keyboard_input not in [1,2,3,4]:
                            print('\nНеверный ввод.\n') 
                    if keyboard_input ==1:                        
                        сreate_table_logging(engine)                                          
                        max_delta_id = log_delta_id_last(engine, schema, table_name_log)   
                        df = merge_mirror_delta(max_delta_id, spark, path, engine, table_name, pk, delta_list)
                        if df is not None:  
                            upload_to_csv(df)                                                         
                    if keyboard_input ==2:           
                        read_to_csv(spark)               
                    if keyboard_input ==3:  
                        show_logs_PostgreSQL(engine, schema, table_name_log, spark)                                    
                print("--- %s seconds ---" % (time.time() - start_time))


if __name__ == '__main__':
    main() 



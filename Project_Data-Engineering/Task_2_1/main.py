from pathlib import Path
from pyspark.sql.functions import initcap
from pyspark.sql import SparkSession 
import sys
from pathlib import Path


def sparksession():
    """
    Cоздает объект SparkSession для работы co Spark SQL.
    """
    spark = (SparkSession
    .builder
    .appName('pyspark_lezhneva')
    .enableHiveSupport()
    .getOrCreate())
    return spark


def create_df_discipline(spark):
    """
    Генерирует DataFrame из трёх колонок (row_id, discipline, season) - олимпийские дисциплины по сезонам.
        * row_id - число порядкового номера строки;
        * discipline - наименование олимпиский дисциплины на английском (полностью маленькими буквами);
        * season - сезон дисциплины (summer / winter);
    """
    rows = [
    (1,"football", "summer"),
    (2,"boxing", "summer"),
    (3,"badminton", "summer"), 
    (4,"swimming", "summer"),
    (5,"judo", "summer"), 
    (6,"hockey", "winter"),
    (7,"curling", "winter"),
    (8,"bobsleigh", "winter"),
    (9,"biathlon", "winter"),
    (10,"snowboard", "winter")]
    schema = "row_id BIGINT, discipline STRING, season STRING"
    df_discipline = spark.createDataFrame(rows, schema)
    return df_discipline


def write_to_csv(df_discipline, file_path):
    """
    Сохраняет DataFrame в один csv-файл:
        * разделитель колонок табуляция
        * первая строка содержит название колонок.
    """
    df_discipline.coalesce(1).write.mode("overwrite").options(header='True', delimiter='\t').csv(f"{file_path}.csv")
    print("write_to_csv - success")
    

def read_from_csv(spark, file_path):
    """
    Читает данные из файла .csv и создает DataFrame.
    """
    try:
        df_athletes = spark.read.options(header='True', delimiter=';', inferSchema='True')\
        .csv(f"{file_path}.csv")    
        return df_athletes
    except:
        print(f"ERROR. {sys.exc_info()[1]}")


def athletes_count(df_athletes):
    """
    Считает в разрезе дисциплин сколько всего спортсменов в каждой из дисциплин принимало участие
    """
    df_athletes_count = df_athletes.groupBy("discipline").count()
    return df_athletes_count


def write_to_parquet(df_name, file_path):
    """
    Cохраняет данные в формате parquet.
    """
    try:
        df_name.write.mode("overwrite").parquet(f"{file_path}.parquet")
        print("write_to_parquet - success")
    except:
        print(f"ERROR. {sys.exc_info()[1]}")


def join(df_discipline, df_athletes_count):
    """
    Объединяет со сгенерированным DataFrame и выводит количество участников,
    только по тем дисциплинам, что есть в сгенерированном DataFrame.
    """
    #Исправим названия в колонке discipline - названия будут начинаться с заглавной буквы:
    df_discipline = df_discipline.select(initcap('discipline').alias('discipline'))
    #объединяем датафреймы:
    df_join = df_discipline.join(df_athletes_count,'discipline','left').na.drop(subset=['count'])
    return df_join


def check(spark):
    spark = sparksession()
    print("Task 1:")
    spark.read.options(header='True', delimiter='\t', inferSchema='True')\
        .csv(f"{Path(sys.path[0],'data','df_discipline')}.csv").show() 
    print("Task 2:")
    spark.read.parquet(f"{Path(sys.path[0],'data','df_athletes_count')}.parquet").show() 
    print("Task 3:")
    spark.read.parquet(f"{Path(sys.path[0],'data','df_join')}.parquet").show() 


def main():
    """
    Запуск программы.
    """    
    spark = sparksession()
    df_discipline = create_df_discipline(spark)
    write_to_csv(df_discipline, Path(sys.path[0],'data','df_discipline'))    
    df_athletes = read_from_csv(spark, Path(sys.path[0],'data','Athletes'))
    if df_athletes:
        df_athletes_count = athletes_count(df_athletes)       
        write_to_parquet(df_athletes_count, Path(sys.path[0],'data','df_athletes_count'))        
        df_join = join(df_discipline, df_athletes_count)        
        write_to_parquet(df_join, Path(sys.path[0],'data','df_join'))    
        check(spark)
    
    
if __name__ == '__main__':
    main()


    

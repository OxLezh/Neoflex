# Курсовой проект для студентов учебного центра Neoflex по направлению «Data-Engineering (BDS)»

# Проектная работа “Реализация процедуры ETL”

## Задание №1

В некотором банке внедрили новую frontend-систему для работы с клиентами, а так же обновили и саму базу данных. Большую часть данных успешно были перенесены из старых БД в одну новую централизованную БД.  Но в момент переключения со старой системы на новую возникли непредвиденные проблемы в ETL-процессе, небольшой период (конец 2017 начало 2018 года) так и остался в старой базе. Старую базу отключили, а не выгруженные данные сохранили в csv-файлы. Недавно банку потребовалось построить отчёт по 101 форме. Те данные что остались в csv-файлах тоже нужны. Загрузить их в новую БД не получиться из-за архитектурных и управленческих сложностей, нужно рассчитать витрину отдельно. Но для этого сначала нужно загрузить исходные данные из csv-файлов в детальный слой (DS) хранилища в СУБД Oracle / PostgreSQL.

### Задача 1.2.

Описание задачи в документации.

### <u>Файлы репозитория</u>:

#### SQL-cкрипты:

* _PW_create_table_1_2_Lezhneva.sql_ - скрипт создания витрин оборотов и 101-й отчётной формы, таблицы с логами, sequence, расширения pg_cron.

* _PW_procedure_writeog_1_2_Lezhneva.sql_ - скрипт создания процедуры ля записи данных в таблицу логов.

* _PW_procedure_account_turnover_f_1_2_1_Lezhneva.sql_ - скрипт создания процедур расчета данных для витрины оборотов dm.dm_account_turnover_f за выбранный день и за месяц.

* _PW_procedure_f101_round_f_1_2_2_Lezhneva.sql_ - скрипт создания процедуры расчета данных для витрины 101-й отчётной формы dm.dm_f101_round_f.

* _PW_pg_cron_1_2_job_Lezhneva.sql_ - скрипт для создания заданий pg_cron по запуску расчетов витрин оборотов и 101-й отчётной формы.


#### Документация по проекту:

* _Курсовой_проект_DataEngineer_Задача_1.2.md_
* _Структура таблиц.md_
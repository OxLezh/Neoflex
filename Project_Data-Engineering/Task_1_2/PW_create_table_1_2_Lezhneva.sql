create schema if not exists dm;

--Создание витрины оборотов.
create table if not exists dm.dm_account_turnover_f (
	on_date date,
	account_rk numeric,
	credit_amount numeric(23,8),
	credit_amount_rub numeric(23,8),
	debet_amount numeric(23,8),
	debet_amount_rub numeric(23,8)
);

--Создание витрины 101-й отчётной формы.
create table if not exists dm.dm_f101_round_f (
	from_date date,
	to_date date,
	chapter char(1),
	ledger_account char(5),
	characteristic char(1),
	balance_in_rub numeric(23,8),
	r_balance_in_rub numeric(23,8),
	balance_in_val numeric(23,8),
	r_balance_in_val numeric(23,8),
	balance_in_total numeric(23,8),
	r_balance_in_total numeric(23,8),
	turn_deb_rub numeric(23,8),
	r_turn_deb_rub numeric(23,8),
	turn_deb_val numeric(23,8),
	r_turn_deb_val numeric(23,8),
	turn_deb_total numeric(23,8),
	r_turn_deb_total numeric(23,8),
	turn_cre_rub numeric(23,8),
	r_turn_cre_rub numeric(23,8),
	turn_cre_val numeric(23,8),
	r_turn_cre_val numeric(23,8),
	turn_cre_total numeric(23,8),
	r_turn_cre_total numeric(23,8),
	balance_out_rub numeric(23,8),
	r_balance_out_rub numeric(23,8),
	balance_out_val numeric(23,8),
	r_balance_out_val numeric(23,8),
	balance_out_total numeric(23,8),
	r_balance_out_total numeric(23,8)
);

--Создание таблицы для записи логов.
create table if not exists logs.lg_messages ( 
	record_id numeric not null,
	date_time timestamp not null,
	pid numeric not null,
	message varchar(4000) not null,
	message_type numeric(1) not null,
	usename text, 
	datname text, 
	client_addr text, 
	application_name text,
	backend_start text
	);

create sequence if not exists dm.seq_lg_messages start 1;

--Cозадние расширения pg_cron.
--create extension if not exists pg_cron;
--
--alter system set shared_preload_libraries='pg_cron';

--drop extension pg_cron;



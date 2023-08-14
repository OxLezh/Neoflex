
create schema if not exists ds;

create schema if not exists logs;

--остатки средств на счетах
create table if not exists ds.ft_balance_f (
	on_date date,
	account_rk numeric, 
	currency_rk numeric,
	balance_out numeric,
	primary key (on_date, account_rk)
);

--проводки (движения средств) по счетам
create table if not exists ds.ft_posting_f (
	oper_date date, 
	credit_account_rk numeric, 
	debet_account_rk numeric, 
	credit_amount numeric,
	debet_amount numeric,
	primary key (oper_date, credit_account_rk, debet_account_rk)
);

--информация о счетах клиентов
create table if not exists ds.md_account_d (
	data_actual_date date, 
	data_actual_end_date date not null,
	account_rk numeric, 
	account_number varchar(20) not null,
	char_type char(1) not null,
	currency_rk numeric not null,
	currency_code char(3) not null,
	primary key (data_actual_date, account_rk)
);

--справочник валют
create table if not exists ds.md_currency_d (
	currency_rk numeric, 
	data_actual_date date, 
	data_actual_end_date date,
	currency_code char(3),
	code_iso_char char(3),
	primary key (currency_rk, data_actual_date)
);

--курсы валют
create table if not exists ds.md_exchange_rate_d (
	data_actual_date date, 
	data_actual_end_date date,
	currency_rk numeric,
	reduced_cource numeric,
	code_iso_num char(3),
	primary key (data_actual_date, currency_rk)
);

--справочник балансовых счётов
create table if not exists ds.md_ledger_account_s (
	chapter char(1),
	chapter_name varchar(16),
	section_number int,
	section_name varchar(22),
	subsection_name varchar(21),
	ledger1_account int,
	ledger1_account_name varchar(47),
	ledger_account int, 
	ledger_account_name varchar(153),
	characteristic char(1),
	is_resident char(1) ,
	is_reserve char(1),
	is_reserved char(1) ,
	is_loan char(1),
	is_reserved_assets char(1),
	is_overdue char(1),
	is_interest char(1),
	pair_account varchar(5),
	start_date date, 
	end_date date,
	is_rub_only char(1),
	min_term varchar(1),
	min_term_measure varchar(1),
	max_term varchar(1),
	max_term_measure varchar(1),
	ledger_acc_full_name_translit varchar(1),
	is_revaluation varchar(1),
	is_correct varchar(1),
	primary key (ledger_account, start_date)
);
--таблица логов
create table if not exists logs.load_logs (
	sources varchar(40),
	action_date timestamp not null default now(),
	status varchar(20),
	description text,
	error text
);

-----------------------------------------------------------------------------------------------
-- Дополнительные запросы:

--truncate ds.ft_balance_f, ds.ft_posting_f, ds.md_account_d, ds.md_currency_d,ds.md_exchange_rate_d, ds.md_ledger_account_s,logs.load_logs;

--drop table ds.ft_balance_f, ds.ft_posting_f, ds.md_account_d, ds.md_currency_d,ds.md_exchange_rate_d, ds.md_ledger_account_s, logs.load_logs;

--drop table logs.load_logs;

--truncate  logs.load_logs;

-- select * from ds.ft_balance_f;

-- select * from ds.ft_posting_f;

-- select * from ds.md_account_d;

-- select * from ds.md_currency_d;

-- select * from ds.md_exchange_rate_d;

-- select * from ds.md_ledger_account_s;

-- select * from logs.load_logs;
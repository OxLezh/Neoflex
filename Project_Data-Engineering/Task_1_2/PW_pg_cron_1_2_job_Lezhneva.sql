--Запуск расчета витрины оборотов dm.dm_account_turnover_f за месяц.
insert
	into cron.job(jobname, schedule, command, nodename)
	values ('dm.fill_account_turnover_f','20 seconds','call dm.month_account_turnover_f(''2018-01-03'')','');

--Запуск расчета витрины 101-й отчётной формы dm.dm_f101_round_f.
insert
	into cron.job (jobname, schedule, command, nodename)
	values ('dm.fill_f101_round_f','20 seconds','call dm.fill_f101_round_f(''2018-01-03'')','');

select * from cron.job;

select * from cron.job_run_details;

select * from dm.dm_account_turnover_f;

select * from dm.dm_f101_round_f;

select * from dm.lg_messages;


/*Создайте функцию (в Oracle или PostgreSQL), которая будет принимать дату, 
а возвращать эту дату и информацию о максимальной и минимальной сумме проводки 
 по кредиту и по дебету за переданную дату. То есть эти значения надо вычислять на
 основании данных в таблице «ds.ft_posting_f».*/

create or replace function dm.func_ds_ft_posting_f(i_OnDate date) returns table
	(oper_dates date,
	max_credit_amount numeric,
	min_credit_amount numeric,
	max_debet_amount numeric,
	min_debet_amount numeric ) AS $$ 
begin
	if i_OnDate in (
		select
			oper_date
		from ds.ft_posting_f)
	then return query
		select 
			oper_date, 
			max(credit_amount),
			min(credit_amount), 
			max(debet_amount),
			min(debet_amount) 
			from ds.ft_posting_f
			where oper_date = i_OnDate
			group by oper_date;	
	end if;
end;
$$ language plpgsql;
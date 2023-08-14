--Процедура расчета данных для витрины оборотов dm.dm_account_turnover_f за выбранный день.
create or replace procedure dm.fill_account_turnover_f(i_OnDate date)
language plpgsql    
as $$
declare
	v_RowCount int;
begin	
	call dm.writelog( '[BEGIN] fill dm.dm_account_turnover_f (i_OnDate => date ''' 
	     || to_char(i_OnDate, 'yyyy-mm-dd') 
	     || ''');', 1);
	
	delete
		from dm.dm_account_turnover_f f
		where f.on_date = i_OnDate;
	   
	call dm.writelog( 'delete on_date = ' 
	     || to_char(i_OnDate, 'yyyy-mm-dd'), 1);
	
	insert
	  into dm.dm_account_turnover_f
		(on_date,
		account_rk,
		credit_amount,
		credit_amount_rub,
		debet_amount,
		debet_amount_rub)
	with wt_turn as
		(select 
			p.credit_account_rk                  				as account_rk,
			p.credit_amount                      				as credit_amount,
			p.credit_amount * coalesce(er.reduced_cource, 1)	as credit_amount_rub,
			cast(null as numeric)                 				as debet_amount,
			cast(null as numeric)                 				as debet_amount_rub
	    from ds.ft_posting_f p
	    join ds.md_account_d a
	      on a.account_rk = p.credit_account_rk
	    left
	    join ds.md_exchange_rate_d er
	      on er.currency_rk = a.currency_rk
	     and i_OnDate 
	     	between er.data_actual_date 
	     	and er.data_actual_end_date
	   where p.oper_date = i_OnDate
	     and i_OnDate 
	     	between a.data_actual_date 
	     	and a.data_actual_end_date
	     and a.data_actual_date 
	     	between date_trunc('month', i_OnDate) 
	     	and (date_trunc('MONTH', i_OnDate) + INTERVAL '1 MONTH - 1 day')
		union all
		select 
			p.debet_account_rk                  				as account_rk,
			cast(null as numeric)								as credit_amount,
			cast(null as numeric)                 				as credit_amount_rub,
			p.debet_amount                       				as debet_amount,
			p.debet_amount * coalesce(er.reduced_cource, 1)		as debet_amount_rub
	    from ds.ft_posting_f p
	    join ds.md_account_d a
	      on a.account_rk = p.debet_account_rk
	    left 
	    join ds.md_exchange_rate_d er
	      on er.currency_rk = a.currency_rk
	     and i_OnDate between er.data_actual_date 
	     	 and er.data_actual_end_date
	   where p.oper_date = i_OnDate
	     and i_OnDate between a.data_actual_date 
	     	and a.data_actual_end_date
	     and a.data_actual_date between date_trunc('month', i_OnDate) 
	     	 and (date_trunc('MONTH', i_OnDate) + INTERVAL '1 MONTH - 1 day'))
	select 
		i_OnDate    				                        	as on_date,
		t.account_rk,
		sum(coalesce(t.credit_amount, 0))                   	as credit_amount,
		sum(coalesce(t.credit_amount_rub, 0))               	as credit_amount_rub,
		sum(coalesce(t.debet_amount, 0))                    	as debet_amount,
		sum(coalesce(t.debet_amount_rub, 0))                	as debet_amount_rub
	from wt_turn t
	group by t.account_rk;
 
	GET DIAGNOSTICS v_RowCount = ROW_COUNT;

	call dm.writelog('insert', 1);

	call dm.writelog('[END] inserted dm.dm_account_turnover_f ' || to_char(v_RowCount,'FM99999999') || ' rows.', 1);

commit;	
end;$$;

--Расчет витрины оборотов dm.dm_account_turnover_f за месяц.
create or replace procedure dm.month_account_turnover_f(i_OnDate date)
language plpgsql    
as $$
declare
	start_date date := date_trunc('month',i_OnDate);
	end_date date := date_trunc('month',i_OnDate) + interval '1 MONTH - 1 day';
begin
	while start_date <= end_date loop
		call dm.fill_account_turnover_f(start_date);
		start_date := start_date + interval '1 day';
	end loop;
commit;  	
end;$$;

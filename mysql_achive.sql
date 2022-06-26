DELIMITER $$
CREATE DEFINER=`root`@`localhost` PROCEDURE `up_hk_history_data`(
	IN _keep_days int,IN _batch_size int
)
MainLabel:BEGIN
    DECLARE _start date;
    declare _start_date date;
    declare _end_date date;
    declare _affected_rows int;
    declare _fnname varchar(50); 
    declare _id int;
    declare _last_id int;
    SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
    if time(CURRENT_TIMESTAMP) not between time('02:00:00') and time('06:00:00') then
        leave MainLabel;
    end if;
    set _batch_size=ifnull(_batch_size,50000);
    set _end_date=DATE_SUB(CURDATE(), interval IFNULL(_keep_days,100) day);
    select max(bdate) into _start from process_data_log where bdate <=_end_date and fnname='delete cnf_balance_log';
    if _start is null then
		select CAST(from_unixtime(min(create_time)) as date) into _start from cnf_balance_log where create_time <=UNIX_TIMESTAMP(_end_date);
        if _start is null then
          leave MainLabel;
		end if;
    end if;    
    set _start=DATE_add(_start, interval 1 day); 
    select _start,_end_date,_batch_size;
    if _start>_end_date then
      leave MainLabel;
    end if;        
   set _last_id=null;
   select max(id) into _last_id from cnf_balance_log where create_time<=UNIX_TIMESTAMP(_start);
   if _last_id is null then
      select _start,_end_date,_batch_size;
      leave MainLabel;
   end if;
   
   set _fnname='delete cnf_balance_log';  
   insert into process_data_log (bdate,fnname,rows_affected,starttime) values (_start,_fnname,0,SYSDATE());
   set _id=LAST_INSERT_ID();
   
   set _affected_rows=_batch_size;
   
   while _affected_rows>0
	 do 
        delete from cnf_balance_log where id<=_last_id limit _batch_size;
		set _affected_rows=ROW_COUNT();
		update process_data_log set rows_affected=rows_affected+_affected_rows where id=_id;     
		DO SLEEP(1);
   end while;  
   update process_data_log set endtime=SYSDATE() where id=_id;     
   set _last_id=null;
   select max(id) into _last_id from sk_order where create_time<=UNIX_TIMESTAMP(_start);
   if _last_id is null then
      leave MainLabel;
   end if;
   set _fnname='delete sk_order';  
   insert into process_data_log (bdate,fnname,rows_affected,starttime) values (_start,_fnname,0,SYSDATE());
   set _id=LAST_INSERT_ID();
   set _affected_rows=_batch_size;
   while _affected_rows>0
	 do 
        delete from sk_order where id<=_last_id limit _batch_size;
		set _affected_rows=ROW_COUNT();
		update process_data_log set rows_affected=rows_affected+_affected_rows where id=_id;     
		DO SLEEP(1);
   end while;  
   update process_data_log set endtime=SYSDATE() where id=_id;     
       
END$$
DELIMITER ;

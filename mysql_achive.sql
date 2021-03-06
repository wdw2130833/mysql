use master_all;
DROP table IF EXISTS process_data_log ;
CREATE TABLE `process_data_log` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `bdate` date NOT NULL,
  `fnname` varchar(50) NOT NULL,
  `rows_affected` int(11) DEFAULT NULL,
  `starttime` datetime DEFAULT NULL,
  `endtime` datetime DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=192 DEFAULT CHARSET=utf8mb4;

DELIMITER //
DROP PROCEDURE IF EXISTS up_hk_history_data //
create PROCEDURE up_hk_history_data(
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
    set _batch_size=ifnull(_batch_size,50000);
    set _end_date=DATE_SUB(CURDATE(), interval IFNULL(_keep_days,100) day);
    select max(bdate) into _start from process_data_log where bdate <=_end_date and fnname='delete cnf_balance_log';
    if _start is null then
		select CAST(from_unixtime(min(create_time)) as date) into _start from cnf_balance_log where create_time <=UNIX_TIMESTAMP(_end_date);
        if _start is null then
          leave MainLabel;
		end if;
    end if;    
    set _start=DATE_add(_start, interval 1 day); --  clean one day for each run
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
       
END //
DELIMITER ;
drop EVENT IF EXISTS archive_data_daily;
DELIMITER //
CREATE EVENT archive_data_daily
ON SCHEDULE EVERY 5 minute STARTS (TIMESTAMP(CURRENT_DATE) + INTERVAL 120 minute)
ends (TIMESTAMP(CURRENT_DATE) + INTERVAL 360 minute)
ON COMPLETION PRESERVE
DO begin
	SET sql_log_bin=off;
	call up_hk_history_data(null,null);
	SET sql_log_bin=on;
 end //
 DELIMITER ;
 

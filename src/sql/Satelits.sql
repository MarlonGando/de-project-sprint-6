--Администраторы
drop table if exists STV2023081266__DWH.s_admins;
create table IF NOT EXISTS STV2023081266__DWH.s_admins
(
    hk_admin_id bigint not null CONSTRAINT fk_s_admins_l_admins REFERENCES STV2023081266__DWH.l_admins (hk_l_admin_id),
    is_admin boolean,
    admin_from datetime,
    load_dt datetime,
    load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_admin_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);
-------------------------------------------------------------------------------------------------------------------------
--Наименование группы
drop table if exists STV2023081266__DWH.s_group_name;
create table IF NOT EXISTS STV2023081266__DWH.s_group_name
(
    hk_group_id bigint not null CONSTRAINT fk_s_group_name
    REFERENCES STV2023081266__DWH.h_groups(hk_group_id),
    group_name varchar(100),
    load_dt datetime,
    load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_group_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);
-------------------------------------------------------------------------------------------------------------------------
--Является ли группа приватной
drop table if exists STV2023081266__DWH.s_group_private_status;
create table IF NOT EXISTS STV2023081266__DWH.s_group_private_status
(
    hk_group_id bigint not null CONSTRAINT fk_s_group_status
    REFERENCES STV2023081266__DWH.h_groups(hk_group_id),
    is_private int,
    load_dt datetime,
    load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_group_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);
-------------------------------------------------------------------------------------------------------------------------
--Имя пользователя в чате
drop table if exists STV2023081266__DWH.s_user_chatinfo;
create table IF NOT EXISTS STV2023081266__DWH.s_user_chatinfo
(
    hk_user_id bigint not null CONSTRAINT fk_s_user_chatinfo
    REFERENCES STV2023081266__DWH.h_users(hk_user_id),
    chat_name varchar (200),
    load_dt datetime,
    load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_user_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);
-------------------------------------------------------------------------------------------------------------------------
--Соц дем
drop table if exists STV2023081266__DWH.s_user_socdem;
create table IF NOT EXISTS STV2023081266__DWH.s_user_socdem
(
    hk_user_id bigint not null CONSTRAINT fk_s_user_socdem
    REFERENCES STV2023081266__DWH.h_users(hk_user_id),
    country varchar(200),
    age int,
    load_dt datetime,
    load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_user_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);
-------------------------------------------------------------------------------------------------------------------------
--Информация о диалоге
drop table if exists STV2023081266__DWH.s_dialog_info;
create table STV2023081266__DWH.s_dialog_info
(
hk_message_id bigint not null CONSTRAINT fk_s_dialog_info_h_dialogs REFERENCES  STV2023081266__DWH.h_dialogs(hk_message_id),
message varchar(1000),
message_from integer,
message_to integer,
load_dt datetime,
load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_message_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);
-------------------------------------------------------------------------------------------------------------------------
--Лог событий
drop table if exists STV2023081266__DWH.s_auth_history;
create table IF NOT EXISTS STV2023081266__DWH.s_auth_history
(
    hk_l_user_group_activity bigint not null CONSTRAINT fk_l_user_group_activity
    REFERENCES STV2023081266__DWH.l_user_group_activity(hk_l_user_group_activity),
    user_id_from integer,
    event varchar(15),
    event_dt datetime,
    load_dt datetime,
    load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_l_user_group_activity all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);
-------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------INSERT-----------------------------------------------------------------

--Администраторы
INSERT INTO STV2023081266__DWH.s_admins
(hk_admin_id, is_admin,admin_from,load_dt,load_src)
select la.hk_l_admin_id,
	True as is_admin,
	hg.registration_dt,
	now() as load_dt,
	's3' as load_src
from STV2023081266__DWH.l_admins as la
left join STV2023081266__DWH.h_groups as hg on la.hk_group_id = hg.hk_group_id;
-------------------------------------------------------------------------------------------------------------------------
--Наименование группы
INSERT INTO STV2023081266__DWH.s_group_name
(hk_group_id, group_name,load_dt,load_src)
SELECT
	hg.hk_group_id,
	sg.group_name,
	now() as load_dt,
	's3' as load_src
FROM STV2023081266__DWH.h_groups as hg
LEFT JOIN STV2023081266__STAGING.groups as sg
on sg.id = hg.group_id;
-------------------------------------------------------------------------------------------------------------------------
--Является ли группа приватной
INSERT INTO STV2023081266__DWH.s_group_private_status
(hk_group_id, is_private, load_dt, load_src)
SELECT
	hg.hk_group_id,
	sg.is_private,
	now() as load_dt,
	's3' as load_src
FROM STV2023081266__DWH.h_groups as hg
LEFT JOIN STV2023081266__STAGING.groups as sg
on sg.id = hg.group_id;
-------------------------------------------------------------------------------------------------------------------------
--Информация о диалоге
INSERT INTO STV2023081266__DWH.s_dialog_info
(hk_message_id, message, message_from,message_to,load_dt,load_src)
SELECT
	hd.hk_message_id,
	std.message,
	std.message_from,
	std.message_to,
	now() as load_dt,
	's3' as load_src
FROM STV2023081266__DWH.h_dialogs hd
LEFT JOIN STV2023081266__STAGING.dialogs std
ON hd.message_id = std.message_id;
-------------------------------------------------------------------------------------------------------------------------
--Соц дем
INSERT INTO STV2023081266__DWH.s_user_socdem
(hk_user_id,country,age,load_dt,load_src)
SELECT
	hu.hk_user_id,
	stu.country,
	stu.age,
	now() as load_dt,
	's3' as load_src
FROM STV2023081266__DWH.h_users hu
LEFT JOIN STV2023081266__STAGING.users stu
ON hu.user_id = stu.id;
-------------------------------------------------------------------------------------------------------------------------
--Имя пользователя в чате
INSERT INTO STV2023081266__DWH.s_user_chatinfo(hk_user_id, chat_name,load_dt,load_src)
SELECT
	hu.hk_user_id,
	stu.chat_name,
	now() as load_dt,
	's3' as load_src
FROM STV2023081266__DWH.h_users hu
LEFT JOIN STV2023081266__STAGING.users stu
ON hu.user_id = stu.id;
-------------------------------------------------------------------------------------------------------------------------
--Информация о диалоге
INSERT INTO STV2023081266__DWH.s_dialog_info
(hk_message_id, message, message_from, message_to, load_dt, load_src)
select hd.hk_message_id,
d.message,
d.message_from,
d.message_to,
now() as load_dt,
's3' as load_src
from STV2023081266__DWH.h_dialogs as hd
left join STV2023081266__STAGING.dialogs as d
on hd.message_id = d.message_id;
-------------------------------------------------------------------------------------------------------------------------
--Лог событий
INSERT INTO STV2023081266__DWH.s_auth_history
(hk_l_user_group_activity, user_id_from,
event, event_dt, load_dt,load_src)
SELECT
	luga.hk_l_user_group_activity,
	gl.user_id_from,
	gl.event,
	gl.datetime_ts,
	now() as load_dt,
	's3' as load_src
FROM STV2023081266__STAGING.group_log gl
left join STV2023081266__DWH.h_groups as hg on gl.group_id = hg.group_id
left join STV2023081266__DWH.h_users as hu on gl.user_id = hu.user_id
left join STV2023081266__DWH.l_user_group_activity as luga on hg.hk_group_id = luga.hk_group_id and hu.hk_user_id = luga.hk_user_id
;
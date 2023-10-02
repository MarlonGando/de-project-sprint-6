--------------------------------------------------------HUBS------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------
drop table if exists STV2023081266__DWH.h_users;
create table IF NOT EXISTS STV2023081266__DWH.h_users
(
    hk_user_id bigint primary key,
    user_id      integer,
    registration_dt datetime,
    load_dt datetime,
    load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_user_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

------------------------------------------------------------------------------------------------------------------------
--Группы
drop table if exists STV2023081266__DWH.h_groups;
create table IF NOT EXISTS STV2023081266__DWH.h_groups
(
    hk_group_id bigint primary key,
    group_id      integer,
    registration_dt datetime,
	load_dt datetime,
    load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_group_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);
------------------------------------------------------------------------------------------------------------------------
-- Диалоги
drop table if exists STV2023081266__DWH.h_dialogs;
create table IF NOT EXISTS STV2023081266__DWH.h_dialogs
(
    hk_message_id bigint primary key,
    message_id      integer,
    datetime_ts datetime,
	load_dt datetime,
    load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_message_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);
-------------------------------------------------------LINKS------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------
--Связь пользователей и сообщений
drop table if exists STV2023081266__DWH.l_user_message;
create table IF NOT EXISTS STV2023081266__DWH.l_user_message
(
    hk_l_user_message bigint primary key,
    hk_user_id bigint not null CONSTRAINT fk_l_user_message_user REFERENCES STV2023081266__DWH.h_users (hk_user_id),
    hk_message_id bigint not null CONSTRAINT fk_l_user_message_message REFERENCES STV2023081266__DWH.h_dialogs (hk_message_id),
    load_dt datetime,
    load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_user_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);
------------------------------------------------------------------------------------------------------------------------------
--Админы
drop table if exists STV2023081266__DWH.l_admins;
create table IF NOT EXISTS STV2023081266__DWH.l_admins
(
    hk_l_admin_id bigint primary key,
    hk_user_id bigint not null CONSTRAINT fk_l_admin_group_user REFERENCES STV2023081266__DWH.h_users (hk_user_id),
    hk_group_id bigint not null CONSTRAINT fk_l_admin_user_group REFERENCES STV2023081266__DWH.h_groups (hk_group_id),
    load_dt datetime,
    load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_l_admin_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);
------------------------------------------------------------------------------------------------------------------------------
--Связь пользователя, группы и диалога
drop table if exists STV2023081266__DWH.l_groups_dialogs;
create table IF NOT EXISTS STV2023081266__DWH.l_groups_dialogs
(
    hk_l_groups_dialogs bigint primary key,
    hk_message_id bigint not null CONSTRAINT fk_l_group_dialog REFERENCES STV2023081266__DWH.h_dialogs (hk_message_id),
    hk_group_id bigint not null CONSTRAINT fk_l_dialog_group REFERENCES STV2023081266__DWH.h_groups (hk_group_id),
    load_dt datetime,
    load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_l_groups_dialogs all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);
------------------------------------------------------------------------------------------------------------------------------
--Связь активности пользователя и группы
drop table if exists STV2023081266__DWH.l_user_group_activitys;
create table IF NOT EXISTS STV2023081266__DWH.l_user_group_activity
(
    hk_l_user_group_activity integer primary key,
    hk_user_id bigint not null CONSTRAINT fk_l_group_dialog REFERENCES STV2023081266__DWH.h_dialogs (hk_message_id),
    hk_group_id bigint not null CONSTRAINT fk_l_dialog_group REFERENCES STV2023081266__DWH.h_groups (hk_group_id),
    load_dt datetime,
    load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_l_user_group_activity all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);
--------------------------------------------------SATELLITES------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------
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
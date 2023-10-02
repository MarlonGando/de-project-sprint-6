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
------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------INSERT------------------------------------------------------------
--Админы
INSERT INTO STV2023081266__DWH.l_admins(hk_l_admin_id, hk_group_id,hk_user_id,load_dt,load_src)
select
    hash(hg.hk_group_id,hu.hk_user_id),
    hg.hk_group_id,
    hu.hk_user_id,
    now() as load_dt,
    's3' as load_src
from STV2023081266__STAGING.groups as g
left join STV2023081266__DWH.h_users as hu on g.admin_id = hu.user_id
left join STV2023081266__DWH.h_groups as hg on g.id = hg.group_id
where hash(hg.hk_group_id,hu.hk_user_id) not in (select hk_l_admin_id from STV2023081266__DWH.l_admins);
------------------------------------------------------------------------------------------------------------------------------

INSERT INTO STV2023081266__DWH.l_user_message
(hk_l_user_message, hk_user_id, hk_message_id, load_dt, load_src)
select
    hash(hu.hk_user_id,hd.hk_message_id),
    hu.hk_user_id,
    hd.hk_message_id,
    now() as load_dt,
    's3' as load_src
from STV2023081266__STAGING.dialogs as g
left join STV2023081266__DWH.h_dialogs as hd on g.message_id = hd.message_id
left join STV2023081266__DWH.h_users as hu on g.message_from  = hu.user_id
where hash(hu.hk_user_id,hd.hk_message_id)
not in (select hk_l_user_message from STV2023081266__DWH.l_user_message);
------------------------------------------------------------------------------------------------------------------------------

INSERT INTO STV2023081266__DWH.l_groups_dialogs
(hk_l_groups_dialogs,hk_message_id, hk_group_id, load_dt, load_src)
select
    hash(hd.hk_message_id,hg.hk_group_id),
    hd.hk_message_id,
    hg.hk_group_id,
    now() as load_dt,
    's3' as load_src
from STV2023081266__STAGING.dialogs as g
left join STV2023081266__DWH.h_dialogs as hd on g.message_id = hd.message_id
INNER join STV2023081266__DWH.h_groups as hg on g.message_group  = hg.group_id --Сделаю left - получу ошибку
where hash(hd.hk_message_id,hg.hk_group_id)
not in (select hk_l_groups_dialogs from STV2023081266__DWH.l_groups_dialogs);
------------------------------------------------------------------------------------------------------------------------------
INSERT INTO STV2023081266__DWH.l_user_group_activity
(hk_l_user_group_activity, hk_user_id, hk_group_id, load_dt, load_src)
select
    hash(hu.hk_user_id,hg.hk_group_id),
    hu.hk_user_id,
    hg.hk_group_id,
    now() as load_dt,
    's3' as load_src
from STV2023081266__STAGING.group_log as g
left join STV2023081266__DWH.h_users as hu on g.user_id = hu.user_id
left join STV2023081266__DWH.h_groups as hg on g.group_id  = hg.group_id
where hash(hu.hk_user_id,hg.hk_group_id)
not in (select hk_l_user_group_activity from STV2023081266__DWH.l_user_group_activity);

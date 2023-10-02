--------------------------------------------------------HUBS------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------
--Пользователи
INSERT INTO STV2023081266__DWH.h_users
(hk_user_id, user_id,registration_dt,load_dt,load_src)
select
       hash(id) as  hk_user_id,
       id as user_id,
       registration_dt,
       now() as load_dt,
       's3' as load_src
       from STV2023081266__STAGING.users
where hash(id) not in (select hk_user_id from STV2023081266__DWH.h_users);
------------------------------------------------------------------------------------------------------------------------
--Группы
INSERT INTO STV2023081266__DWH.h_groups
(hk_group_id, group_id,registration_dt,load_dt,load_src)
select
       hash(id) as hk_group_id,
       id as group_id,
       registration_dt,
       now() as load_dt,
       's3' as load_src
       from STV2023081266__STAGING.groups
where hash(id) not in (select hk_group_id from STV2023081266__DWH.h_groups);
------------------------------------------------------------------------------------------------------------------------
--Диалоги
INSERT INTO STV2023081266__DWH.h_dialogs
(hk_message_id, message_id,datetime_ts,load_dt,load_src)
select
       hash(message_id) as hk_group_id,
       message_id,
       message_ts,
       now() as load_dt,
       's3' as load_src
       from STV2023081266__STAGING.dialogs
where hash(message_id) not in (select hk_message_id from STV2023081266__DWH.h_dialogs);
-------------------------------------------------------LINKS------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------
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
--Связь пользователей и сообщений
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
--Связь пользователя, группы и диалога
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
--Связь активности пользователя и группы
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
-----------------------------------------------------SATELLITES---------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------
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
left join STV2023081266__DWH.l_user_group_activity as luga on hg.hk_group_id = luga.hk_group_id and hu.hk_user_id = luga.hk_user_id;
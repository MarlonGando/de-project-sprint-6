---------------------------------------------------------STG-------------------------------------------------
drop table if exists STV2023081266__STAGING.users;
drop table if exists STV2023081266__STAGING.users_rej;
drop table if exists STV2023081266__STAGING.groups;
drop table if exists STV2023081266__STAGING.groups_rej;
drop table if exists STV2023081266__STAGING.dialogs;
drop table if exists STV2023081266__STAGING.dialogs_rej;
drop table if exists STV2023081266__STAGING.group_log;
drop table if exists STV2023081266__STAGING.group_log_rej;

CREATE TABLE STV2023081266__STAGING.users
(
	id integer PRIMARY KEY,
	chat_name varchar(200),
	registration_dt datetime,
	country varchar(200),
	age integer
)
ORDER BY id
SEGMENTED BY HASH(id) ALL NODES;
-------------------------------------------------------------------------------------------------------------

CREATE TABLE STV2023081266__STAGING.groups
(
    id integer PRIMARY KEY,
    admin_id integer NOT NULL REFERENCES STV2023081266__STAGING.users(id) ,
    group_name varchar(100),
    registration_dt datetime,
    is_private boolean
)
ORDER BY id, admin_id
SEGMENTED BY HASH(id) ALL NODES
PARTITION BY registration_dt::date
GROUP BY calendar_hierarchy_day(registration_dt::date, 3, 2);
-------------------------------------------------------------------------------------------------------------

CREATE TABLE STV2023081266__STAGING.dialogs
(
    message_id integer NOT NULL,
    message_ts datetime NOT NULL,
    message_from integer NOT NULL REFERENCES STV2023081266__STAGING.users(id),
    message_to integer NOT NULL REFERENCES STV2023081266__STAGING.users(id),
    message varchar(1000),
    message_group integer REFERENCES STV2023081266__STAGING.groups(id)

)
ORDER BY message_id
PARTITION BY message_ts::date
GROUP BY calendar_hierarchy_day(message_ts::date, 3, 2);
-------------------------------------------------------------------------------------------------------------

CREATE TABLE STV2023081266__STAGING.group_log
(
    group_id integer NOT NULL REFERENCES STV2023081266__STAGING.groups(id),
    user_id integer NOT NULL REFERENCES STV2023081266__STAGING.users(id),
    user_id_from integer REFERENCES STV2023081266__STAGING.users(id),
    event varchar(20),
    event_dt datetime

)
ORDER BY group_id
SEGMENTED BY HASH(group_id) ALL NODES
PARTITION BY event_dt::date
GROUP BY calendar_hierarchy_day(event_dt::date, 3, 2);                      


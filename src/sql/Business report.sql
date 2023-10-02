WITH user_group_messages AS (
    						SELECT 
    							lgd.hk_group_id AS hk_group_id, 
    							count(DISTINCT sdi.message_from) AS cnt_users_in_group_with_messages
							FROM STV2023081266__DWH.s_dialog_info sdi
							INNER JOIN STV2023081266__DWH.l_groups_dialogs lgd
							ON sdi.hk_message_id = lgd.hk_message_id
							GROUP BY lgd.hk_group_id
),
user_group_log AS (
					SELECT 
						ugm.hk_group_id, 
						count(DISTINCT luga.hk_user_id) AS cnt_added_users
					FROM STV2023081266__DWH.l_user_group_activity luga
					INNER JOIN user_group_messages ugm
					ON luga.hk_group_id = ugm.hk_group_id
					WHERE luga.hk_l_user_group_activity IN (
														SELECT hk_l_user_group_activity 
														FROM STV2023081266__DWH.s_auth_history
														WHERE event = 'add')
					GROUP BY ugm.hk_group_id)
SELECT
	ugl.hk_group_id,
	ugl.cnt_added_users,
	ugm.cnt_users_in_group_with_messages,
	ugm.cnt_users_in_group_with_messages/ugl.cnt_added_users as group_conversion
FROM user_group_log AS ugl
INNER JOIN user_group_messages AS ugm
ON ugm.hk_group_id=ugl.hk_group_id
WHERE ugl.hk_group_id IN (SELECT hk_group_id
                    FROM STV2023081266__DWH.h_groups
                    ORDER BY registration_dt 
                    LIMIT 10)
ORDER BY ugm.cnt_users_in_group_with_messages/ugl.cnt_added_users DESC;
   
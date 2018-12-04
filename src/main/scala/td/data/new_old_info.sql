create database waimai DEFAULT CHARSET=utf8;
USE waimai;
CREATE TABLE `td_new_old_info` (
  `new_old_id` int(11) DEFAULT NULL,
  `new_old_name` varchar(10) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

insert into td_new_old_info values(1,'新用户');
insert into td_new_old_info values(0,'老用户');
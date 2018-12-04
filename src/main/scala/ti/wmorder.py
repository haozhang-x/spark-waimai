# -*- encoding: utf-8 -*-
import random
import time

users = (1001, 1002, 1003, 1004)
pois = (101, 102, 103, 104, 105, 106)
deliverys = (1, 2, 3, 4)
citys = (
    1101, 1201, 1301, 1302, 1303, 1304, 1305, 1306, 1307, 1308, 1309, 1310, 1311, 1401, 1402, 1403, 1404, 1405, 1406,
    1407, 1408, 1409)
status = (0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
terminals = (1001, 1002, 1101, 1102, 1201, 1202, 1301, 1302)
# 日期，自己根据需要修改
day_id = '20180501'
starttime = (2018, 5, 1, 0, 0, 0, 0, 0, 0)
endtime = (2018, 5, 1, 23, 59, 59, 0, 0, 0)
platacts = (1, 2, 3, 4, 5, 6)
poiacts = (7, 8, 9)
# print(users[random.randint(0,3)]) #随机整数
# print(pois[random.randint(0,5)])

# print(round(random.uniform(0,50),2)) #随机数，带小数点
# print(str(citys[1])[0:2])截取字符串前2位
t = random.uniform(time.mktime(starttime), time.mktime(endtime))
# print(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(t)))
# print(time.strftime('%Y-%m-%d %H:%M:%S',starttime))
# print(time.strftime('%Y-%m-%d %H:%M:%S',endtime))

# 循环生成sql，条数自己根据需要修改
for i in range(1, 338):
    city = str(citys[random.randint(0, 21)])
    user = str(users[random.randint(0, 3)])
    poi = str(pois[random.randint(0, 5)])
    dly = str(deliverys[random.randint(0, 3)])
    tml = str(terminals[random.randint(0, 7)])
    sts = str(status[random.randint(0, 9)])
    platact = str(platacts[random.randint(0, 5)])
    poiact = str(poiacts[random.randint(0, 2)])
    # day_id
    # order_id
    # city_id
    # poi_id
    # user_id
    # terminal_id
    # fee
    # order_time
    # status
    # remark
    print(
        'insert into ti_order_delta_d(day_id,city_id,poi_id,user_id,terminal_id,fee,order_time,status,remark) values(' + day_id + ',' + city + ',' + poi + ',' + user + ',' + tml + ',' + str(
            random.randint(20, 50)) + ',' + '\'' + str(
            time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(t))) + '\',' + sts + ',\'\');')
    # day_id
    # order_id
    # shipping_fee
    # rider
    # dilivery_type_id
    # dilivery_time
    # status
    print(
        'insert into ti_order_delivery_delta_d(day_id,shipping_fee,rider,delivery_type_id,delivery_time,status) values(' + day_id + ',' + str(
            round(random.uniform(0, 5), 2)) + ',\'' + 'zhangsan' + '\',' + dly + ',' + str(
            random.randint(1000, 3000)) + ',' + sts + ');')
    # day_id
    # order_id
    # act_id
    # charge_fee
    print(
        'insert into ti_order_plat_act_d(day_id,act_id,charge_fee) values(' + day_id + ',' + platact + ',' + str(
            round(random.uniform(2, 10), 2)) + ');')
    # day_id
    # order_id
    # act_id
    # charge_fee
    print(
        'insert into ti_order_poi_act_d(day_id,act_id,charge_fee) values(' + day_id + ',' + poiact + ',' + str(
            round(random.uniform(2, 20), 2)) + ');')

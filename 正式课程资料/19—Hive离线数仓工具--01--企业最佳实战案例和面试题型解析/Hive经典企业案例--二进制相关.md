[TOC]



# 二进制换算和统计

假如你是一个 移动用户！你每天，可能打电话，也可能不打电话，只要打了电话就收一块钱，不打电话就不收钱。

移动需要给所有的用户，来统计月费

```
BitMap 和  BloomFilter
```

统计每个月每天出现的情况，比如下面两串数字

```
0100010101000101010100101010101 这串数字一共31位的，每一位代表某个月某一天，如电信的号码某一天有通话记录就置成1，没有为0
1101010101010101010100101010100 这串数字一共31位的，每一位代表某个月某一天，如电信的号码某一天有流量记录就置成1，没有为0
```

现在是想把这两串数字拼起来，比如第一天，通话标识为0，流量标识为1，我需要的数字是两个标识同一天有1的就置为1，两个1也置为1，oracle 我知道怎么写，但是hive不知道怎么写，得出的结果最好不要有科学计数的出现，我想要结果是这样的 1101010101010101010100101010101，怎么写？

假设：现在有一张表 exercise_dianxin：

```
字段名称：name,month,phone,message
字段意义：用户，月份，当月电话记录，当月短信记录
```

样例数据：

```
zhangsan,2020-01,1010010100101010,101100101010101010101
zhangsan,2020-02,1010101101000101010,1010111011101111100000
lisi,2020-01,0010101111101100,101010101010001010101010
lisi,2020-02,0101111100100101110101010,100010110010011110001
wangwu,2020-01,1010000101111011010101,10110001010101001001110
wangwu,2020-02,0101011101101010101,1010101010101100001111
```

实际需求：

```
假设，每个用户收费标准都是只要当天有通话记录或者发送短信记录，就收取一块钱。帮我求每个用户每个月的应缴费用
```

二进制转换SQL实例：

```sql
-- 左补齐
select lpad("010001010100010101", 32, "0");

-- 进制转换
select conv(lpad("010001010100010101", 32, "0"), 2, 10);

-- 类型转换
select cast(conv(lpad("010001010100010101", 32, "0"), 2, 10) as bigint);

-- 两个 int 按位求与
select cast(conv(lpad("010001010100010101", 32, "0"), 2, 10) as bigint) | cast(conv(lpad("1010111", 32, "0"), 2, 10) as bigint);

-- 转换成二进制
select bin(cast(conv(lpad("010001010100010101", 32, "0"), 2, 10) as bigint));

-- 两个二进制序列做 |  作
select bin(cast(conv(lpad("010001010100010101", 32, "0"), 2, 10) as bigint) |  cast(conv(lpad("1010111", 32, "0"), 2, 10) as bigint));

-- 最终语句
select lpad(cast(bin(cast(conv(lpad("010001010100010101", 32, "0"), 2, 10) as bigint) | cast(conv(lpad("1010111", 32, "0"), 2, 10) as bigint)) as string), 31, "0");

-- 求出结果
select size(split("1000000000000000000000000010101", "1")) - 1;
```

解题思路：

辅助理解题意和提供解题思路：

第一个辅助理解SQL：

```sql
select 
cast(conv(lpad("010001010100010101", 32, "0"), 2, 10) as bigint) 
| 
cast(conv(lpad("101011110101111010", 32, "0"), 2, 10) as bigint);
```

第二个辅助SQL：

```sql
select lpad(
        cast(
            bin(
                cast(conv(lpad("010001010100010101", 32, "0"), 2, 10) as int) 
                | 
                cast(conv(lpad("101011110101111010", 32, "0"), 2, 10) as int)
        ) as string
    ), 31, "0"
);
```

第三个辅助SQL：	

```
select size(
    split(lpad(
        cast(
            bin(
            cast(conv(lpad("010001010100010101", 32, "0"), 2, 10) as int) 
            | 
            cast(conv(lpad("101011110101111010", 32, "0"), 2, 10) as int)
            ) as string
        ), 31, "0"
    ), "1")
) - 1;
```


解决实际需求：

第一步：建表导入数据等准备操作

```sql
create database if not exists exercise_db;
use exercise_db;
drop table if exists exercise_dianxin;
create table if not exists exercise_dianxin(name string, month string, phone string, message string) row format delimited fields terminated by ",";
load data local inpath '/home/bigdata/exercise_dianxin.txt' into table exercise_dianxin;
select name, month, phone, message from exercise_dianxin;
```

第二步：最终SQL:

```SQL
select a.name, a.month, 
size(split(lpad(cast(bin(
    cast(conv(lpad(a.phone, 32, "0"), 2, 10) as int) 
    | 
    cast(conv(lpad(a.message, 32, "0"), 2, 10) as int)
) as string), 31, "0"), "1")) - 1 as cost 
from exercise_dianxin a;
```

最终结果：

```sql
zhangsan 	2020-01 16
zhangsan  	2020-02 17
lisi		2020-01 15
lisi		2020-02 20
wangwu		2020-01 18
wangwu		2020-02 15
```


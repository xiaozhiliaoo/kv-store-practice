# Redis cluster 热点key和大key统计

### 热点key和大key统计重在思路！！！
参考文档：https://blog.csdn.net/cjfeii/article/details/77069778

https://cachecloud.github.io/2017/02/20/Redis%E7%83%AD%E7%82%B9key%E5%AF%BB%E6%89%BE%E4%B8%8E%E4%BC%98%E5%8C%96/

## 1. 热key安装工具

git clone https://github.com/facebookarchive/redis-faina.git

## 2. 热点key统计
redis-cli -c -h 127.0.0.1 -p 6383 -a xxxxxx monitor > 6383.sql

redis-cli -c -h 127.0.0.1 -p 6384 -a xxxxxx monitor > 6384.sql

redis-cli -c -h 127.0.0.1 -p 6385 -a xxxxxx monitor > 6385.sql

redis-faina.py 6383.sql >6383.hotkey.sql

redis-faina.py 6384.sql >6384.hotkey.sql

redis-faina.py 6385.sql >6385.hotkey.sql

## 3. 大key统计
参考文档：https://www.jianshu.com/p/91d15876b713

https://github.com/sripathikrishnan/redis-rdb-tools


# 占用 1M内存的大key
核心代码：rdb -c memory /var/redis/6379/dump.rdb --bytes 1024 -f memory.csv

生成csv报告

生成报表字段有database（key在redis的db）、type（key类型）、key（key值）、size_in_bytes（key的内存大小）、encoding（value的存储编码形式）、num_elements（key中的value的个数）、len_largest_element（key中的value的长度），可以创建个表的导入到关系型库用SQL语句分析。
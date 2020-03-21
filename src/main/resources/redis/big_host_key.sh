#!/usr/bin/env bash

redis_bin="/usr/local/bin/redis-cli "

redis_nodes=`/usr/local/chainup/redis/bin/redis-cli -c -h aaaaaa -p 6383 -a xxxxxx cluster nodes|grep master|awk -F " " '{print $2}'`

emails='772654204@qq.com 871894141@qq.com'


function send_email(){
    for name in $emails
    do
       echo "1" | mail -v -s "saas2 redis 节点内存监控" -a /data/dba_tmp/6383.memory.csv $name
       echo "1" | mail -v -s "saas2 redis 节点内存监控" -a /data/dba_tmp/6384.memory.csv $name
       echo "1" | mail -v -s "saas2 redis 节点内存监控" -a /data/dba_tmp/6385.memory.csv $name
    done
}



# 监控redis 占用内存超过1M的大key
function genreal_redis_memory_analyze(){

    for node in $redis_nodes
    do
        echo $node
        node_ip=`echo $node |awk -F ":" '{print $1}'`
        node_port=`echo $node |awk -F ":" '{print $2}'`
        $redis_bin -c -h $node_ip -p $node_port -a xxxxxx bgsave
        sleep 60
        redis_rdb_path="$node_ip:/data/redis_$node_port/dump.rdb"
        local_rdb_path="/data/dba_tmp/$node_port.rdb"
        local_memory_path="/data/dba_tmp/$node_port.memory.csv"
        if [ -f $local_rdb_path ]; then
            rm -rf $local_rdb_path
        fi
        if [ -f $local_memory_path ]; then
            rm -rf $local_memory_path
        fi
        scp $redis_rdb_path $local_rdb_path
        rdb -c memory $local_rdb_path --bytes 1024 -f $local_memory_path
    done
}

main(){
    genreal_redis_memory_analyze
    send_email
}

main
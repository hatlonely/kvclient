#!/usr/bin/env bash

node_ports="
7000
7001
7002
7003
7004
7005
"

redis_conf="
port 6379
cluster-enabled yes
cluster-config-file nodes.conf
cluster-node-timeout 5000
appendonly yes
"

function deploy_redis_node() {
    node=$1

    mkdir -p ${node}
    cd ${node}
    echo "${redis_conf}" | sed s/6379/${node}/g > redis.conf
    redis-server redis.conf &
    cd -
}

function clean() {
    for node in ${node_ports}; do
        rm -rf ${node}
    done
}

function start() {
    for node in ${node_ports}; do
        deploy_redis_node ${node}
    done
    redis-trib.rb create --replicas 1 127.0.0.1:7000 127.0.0.1:7001 127.0.0.1:7002 127.0.0.1:7003 127.0.0.1:7004 127.0.0.1:7005 <<EOF
yes
EOF
}

function stop() {
    ps aux | grep redis-server | grep cluster | awk '{print $2}' | xargs kill
}

function usage() {
    echo "usage:"
    echo "  sh op_redis_cluster <start | stop | clean>"
    echo "  start 开始"
    echo "  stop  结束"
    echo "  clean 清理"
}

function main() {
    case $1 in
        "start") start;;
        "clean") clean;;
        "stop") stop;;
        *) usage;;
    esac
}

main "$@"

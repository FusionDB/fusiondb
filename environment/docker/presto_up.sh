#!/usr/bin/env bash

start_docker_containers(){
    while [ ! -f "docker-compose.yml" ]; do
      sleep 1
    done

    docker-compose up -d  > /dev/null 2>&1
}

wait_for_docker() {
  while ! docker ps | grep "root_presto-master_1"  > /dev/null 2>&1; do
    sleep 1
  done
}

log_into_presto_node() {
  docker container exec -it "root_presto-master_1" presto
}

start_docker_containers
wait_for_docker
log_into_presto_node
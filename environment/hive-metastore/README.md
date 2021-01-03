# hive-metastore

### Overview
Docker image used to create a minimal unsecured hive metastore for dev fusiondb. DO NOT use for production.

ENV variables:
 - METASTORE_DB_HOSTNAME - hostname used to ping metastore db during init
 
Hive standalone metastore mode running.

Support hive-metastore version:
* 3.0.0
* 3.1.2

### Check service

```
cd environment/hive-metastore
docker-compose up -d # start
docker-compose ps -a # check
docker-compose stop # stop
docker-compose start # start
docker-compose restart # restart
docker-compose down # delete all

telnet localhost 9083
```

### Build
```
sh build.sh
```
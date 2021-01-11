# FusionDB

### Overview

lease use the included docker-compose.yaml to test it:

```
docker-compose up -d --scale fdb-worker=2
```

Create S3 bucket:

```
aws s3api --endpoint http://localhost:9878 create-bucket --bucket=tiny
aws s3api --endpoint http://localhost:9878 create-bucket --bucket=ozone
aws s3 --endpoint http://localhost:9878 cp --storage-class REDUCED_REDUNDANCY /tmp/adobegc.log s3://ozone/adobegc.log
aws s3 ls --endpoint http://localhost:9878
aws s3 ls --endpoint http://localhost:9878 s3://ozone
```

FusionDB on S3 (minio or ozone s3):

```
mysql -u root@ozone -h $hostname -P 8306 -p -D tiny # root/root123

or

mysql -u root@minio -h $hostname -P 8306 -p -D tiny # admin/admin123

CREATE SCHEMA ozone.tiny WITH (location = 's3a://tiny/'); # catalog = ozone or minio

use tiny;

create table customer as select * from tpcds.tiny.customer;
```
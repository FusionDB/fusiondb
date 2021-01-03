<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

Please use the included docker-compose.yaml to test it:

```
docker-compose build
docker-compose up -d --scale datanode=3  # fusiondb write ozone, at least 3 nodes, single node write error：https://issues.apache.org/jira/browse/HDDS-1936 
```

Create S3 bucket:

```
aws s3api --endpoint http://localhost:9878 create-bucket --bucket=tiny
aws s3api --endpoint http://localhost:9878 create-bucket --bucket=ozone
aws s3 --endpoint http://localhost:9878 cp --storage-class REDUCED_REDUNDANCY /tmp/adobegc.log s3://ozone/adobegc.log
aws s3 ls --endpoint http://localhost:9878
aws s3 ls --endpoint http://localhost:9878 s3://ozone
```

FusionDB on S3:

```
mysql -u root -h hostname -P 8306 -p -D tiny # root/root123

CREATE SCHEMA ozone.tiny WITH (location = 's3a://tiny/');

use tiny;

create table customer as select * from tpcds.tiny.customer;
```
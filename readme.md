增量测试：
1. cd /Users/mozhenghua/Downloads/flink-1.20.1
2. ./bin/start-cluster.sh
3. cd /Users/mozhenghua/Downloads/flink-cdc-3.4.0/bin
4. ./flink-cdc.sh --flink-home=/Users/mozhenghua/Downloads/flink-1.20.1 --jar=/opt/misc/tis-plugins-commercial/tis-datax/tis-ds-mysql-v8-plugin/target/tis-ds-mysql-v8-plugin/WEB-INF/lib/mysql-connector-j-8.0.31.jar  mysql-to-paimon.yaml
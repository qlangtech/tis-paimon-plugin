
# PaimonWriter 插件文档


___



## 1 快速介绍

PaimonWriter插件实现了向数据湖Paimon中写入数据，在底层实现上，通过调用paimon的batch write和stream write的相关方法来讲数据写入到paimon中

## 2 实现原理

通过读取paimon的文件catalog或者hive catalog的路径，以及相关hadoop配置，hive配置等信息来写入数据 元数据文件等信息到文件系统中

## 3 功能说明

### 3.1 配置样例

* 配置一个从mysql到paimon导入的作业:

```
{
    "job": {
        "setting": {
            "speed": {
                "channel": 2
            }
        },
        "content": [
            {
                "reader": {
                    "name": "mysqlreader",
                    "parameter": {
                        "column": [
                            "id",
                            "name",
                            "age",
                            "score",
                            "create_at",
                            "update_at",
                            "dt"
                        ],
                        "connection": [
                            {
                                "jdbcUrl": [
                                    "jdbc:mysql://127.0.0.1:3306/demo?useSSL=false&useUnicode=true&characterEncoding=UTF-8&serverTimezone=Asia/Shanghai"
                                ],
                                "table": [
                                    "user"
                                ]
                            }
                        ],
                        "password": "root1234",
                        "username": "root",
                        "where": ""
                    }
                },
                "writer": {
                    "name": "paimonwriter",
                    "parameter": {
                        "tableName": "test",
                        "databaseName": "paimon",
                        "catalogPath": "/app/hive/warehouse",
                        "metastoreUri": "thrift://127.0.0.1:9083",
                        "hiveConfDir": "/your/path",
                        "catalogType": "hive",
                        "hiveConfDir": "/your/path",
                        "hadoopConfDir": "/your/path",
                        "tableBucket": 2,
                        "primaryKey": "dt,id",
                        "partitionFields": "dt",
                        "writeOption": "stream_insert",
                        "batchSize": 100,
                        "hadoopConfig": {
                            "hdfsUser": "hdfs",
                            "coreSitePath": "/your/path/core-site.xml",
                            "hdfsSitePath": "/your/path/hdfs-site.xml"
                        },
                        "paimonConfig": {
                            "compaction.min.file-num": "3",
                            "compaction.max.file-num": "6",
                            "snapshot.time-retained": "2h",
                            "snapshot.num-retained.min": "5",
                            "hive.table.owner": "zhangsan",
                            "hive.storage.format": "ORC"
                        },
                        "column": [
                            {
                                "name": "id",
                                "type": "int"
                            },
                            {
                                "name": "name",
                                "type": "string"
                            },
                            {
                                "name": "age",
                                "type": "int"
                            },
                            {
                                "name": "score",
                                "type": "double"
                            },
                            {
                                "name": "create_at",
                                "type": "string"
                            },
                            {
                                "name": "update_at",
                                "type": "string"
                            },{
                                "name": "dt",
                                "type": "string"
                            }
                        ]
                    }
                }
            }
        ]
    }
}

```


### 3.2 参数说明

* **metastoreUri**

	* 描述：需要配置hive的metastore地址:thrift://127.0.0.1:9083,注意:当设置了metastoreUri,则不需要设置hiveConfDir。 <br />

	* 必选：metastoreUri和hiveConfDir配置二选一 <br />

	* 默认值：无 <br />
	
* **hiveConfDir**

	* 描述：如果没有设置hive的metastoreUri,则需要设置hiveConfDir路径，注意：路径中必须要包含hive-site.xml文件。 <br />

	* 必选：metastoreUri和hiveConfDir配置二选一 <br />

	* 默认值：无 <br />

* **catalogPath**

	* 描述：catalogPath是paimon创建的catalog路径，可以包含文件系统的和hdfs系统的路径。 <br />

	* 必选：是 <br />

	* 默认值：无 <br />

* **catalogType**

	* 描述：paimon的catalog类型，支持两种选项，1.file,2.hive <br />

	* 必选：是 <br />

	* 默认值：无 <br />

* **hadoopConfDir**

	* 描述：paimon依赖的hadoop文件配置路径，注意：路径下面要包含两个文件:hdfs-site.xml,core-site.xml <br />

	* 必选：hadoopConfDir和hadoopConfig下的coreSitePath,hdfsSitePath配置二选一 <br />

	* 默认值：无 <br />

* **writeOption**

	* 描述：paimon写入数据的方式，目前支持2种方式：1.batch_insert(按照官方的定义模式,每次只能有一次提交)，2.stream_insert(支持多次提交) <br />

	* 必选：是 <br />

	* 默认值：false <br />

* **hadoopConfig**

	* 描述：设置hadoop的配置参数，可以以设置配置文件core-site.xml和hdfs-site.xml以及可配置kerberos和s3相关参数。<br />

	* 必选：否 <br />

	* 默认值：无 <br />

* **paimonConfig**

	* 描述：paimon的相关配置信息都可以加入。<br />

	* 必选：否 <br />

	* 默认值：无 <br />

* **keyspace**

	* 描述：需要同步的表所在的keyspace。<br />

	* 必选：是 <br />

	* 默认值：无 <br />

* **table**

	* 描述：所选取的需要同步的表。<br />

	* 必选：是 <br />

	* 默认值：无 <br />

* **column**

	* 描述：所配置的表中需要同步的列集合。<br />
	  内容可以是列的名称或"writetime()"。如果将列名配置为writetime()，会将这一列的内容作为时间戳。

	* 必选：是 <br />

	* 默认值：无 <br />
	
* **bucket**

	* 描述：paimon设置bucket大小，注意如果设置为-1则会出现，无法动态的写入分区错误：<br />

	* 必选：否 <br />

	* 默认值：2 <br />

* **batchSize**

	* 描述：一次批量提交(BATCH)的记录条数，注意：次配置是配合在stream_insert模式下使用的，其他模式无效：<br />

	* 必选：否 <br />

	* 默认值：10 <br />

在 Apache Paimon 中，`org.apache.paimon.schema.Schema.Builder` 的 `option` 方法支持配置丰富的表属性（Table Options），涵盖文件管理、合并策略、元数据存储、Hive 集成等。以下是常见参数分类整理：

---

### **1. Compaction 文件合并策略**
| **参数**                          | **说明**                                                                 | **默认值**      |
|-----------------------------------|-------------------------------------------------------------------------|----------------|
| `compaction.min.file-num`         | 触发合并的最小文件数量（小文件积累到该值时触发合并）                         | `3`            |
| `compaction.max.file-num`         | 合并时一次性处理的最大文件数量                                             | `6`            |
| `compaction.size-amplification-threshold` | 文件大小放大阈值（触发合并的条件，如总大小/最大文件大小 > 阈值）               | `1.5`          |
| `compaction.async.enabled`        | 是否启用异步合并（流式写入时建议开启）                                       | `false`        |
| `compaction.early-max.file-num`   | 提前合并的阈值（避免写入延迟过高）                                           | `30`           |

---

### **2. Snapshot 管理**
| **参数**                          | **说明**                                                                 | **默认值**      |
|-----------------------------------|-------------------------------------------------------------------------|----------------|
| `snapshot.time-retained`          | 快照保留时间（如 `2h` 表示保留最近2小时内的快照）                            | `1h`           |
| `snapshot.num-retained.min`       | 最小保留快照数量（即使超过时间保留，至少保留该数量的快照）                     | `5`            |
| `snapshot.num-retained.max`       | 最大保留快照数量（防止存储爆炸）                                             | `2147483647`   |

---

### **3. Hive 集成**
| **参数**                          | **说明**                                                                 | **默认值**      |
|-----------------------------------|-------------------------------------------------------------------------|----------------|
| `hive.table.owner`                | Hive 表的所有者（用于 Hive 元数据同步）                                     | 无             |
| `hive.storage.format`             | Hive 表存储格式（如 `ORC`、`Parquet`）                                     | `ORC`          |
| `hive.partition.extractor`        | 分区字段提取方式（如 `default` 或自定义类）                                 | `default`      |

---

### **4. 通用存储格式**
| **参数**                          | **说明**                                                                 |
|-----------------------------------|-------------------------------------------------------------------------|
| `file.format`                     | 底层文件格式（如 `orc`、`parquet`、`avro`）                              |
| `write.buffer-size`               | 写入缓冲区大小（控制内存使用，如 `256mb`）                                 |
| `write.merge-schema`              | 是否允许动态合并 Schema（新增字段时有用）                                  |

---

### **5. 写入优化**
| **参数**                          | **说明**                                                                 |
|-----------------------------------|-------------------------------------------------------------------------|
| `write.parallelism`               | 写入并行度（控制并发写入任务数）                                           |
| `write.compression`               | 压缩算法（如 `zstd`、`lz4`、`snappy`）                                   |
| `write.local-sort.max-num-files`  | 本地排序时处理的最大文件数（影响排序性能）                                  |

---

### **6. 分区与元数据**
| **参数**                          | **说明**                                                                 |
|-----------------------------------|-------------------------------------------------------------------------|
| `partition.expiration-time`       | 分区过期时间（如 `7d` 表示自动清理7天前的分区）                            |
| `partition.timestamp-formatter`   | 分区时间戳格式（如 `yyyy-MM-dd HH:mm:ss`）                                |

---

### **示例代码**
```java
Schema schema = new Schema.Builder()
    .column("id", DataTypes.INT())
    .column("name", DataTypes.STRING())
    .option("compaction.min.file-num", "3")
    .option("compaction.max.file-num", "6")
    .option("snapshot.time-retained", "2h")
    .option("hive.table.owner", "zhangsan")
    .option("file.format", "orc")
    .build();
```

---

### **注意事项**
1. **时间单位**：`snapshot.time-retained` 等参数支持 `s`（秒）、`m`（分钟）、`h`（小时）、`d`（天）。
2. **Hive 集成**：需确保 Paimon 与 Hive Metastore 的版本兼容。
3. **动态配置**：部分参数支持动态修改（需参考 Paimon 官方文档）。

建议结合具体场景（如实时流写入或离线批处理）调整参数，并通过 `DESC EXTENDED table_name` 命令验证配置是否生效。完整参数列表可参考 [Paimon 官方文档](https://paimon.apache.org/docs/master/)。

### 3.3 类型转换

| DataX 内部类型| paimon 数据类型    |
| -------- | -----  |
| Long     |long|
| float     |float|
| float     |float|
| decimal   |decimal|
| String   |string   |
| Date     |date, timestamp,datatime, string   |
| Boolean  |boolean   |


请注意:

* 目前不支持union,row,struct类型和custom类型。

## 4 性能报告

略

## 5 约束限制

### 5.1 主备同步数据恢复问题

略

## 6 FAQ




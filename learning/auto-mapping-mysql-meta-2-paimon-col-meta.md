在 Apache Flink CDC 将 MySQL 同步到 Paimon 的流程中，**元数据列类型映射的核心实现**发生在 **Paimon 的 Flink CDC Connector 组件**中，具体是在构建 Paimon 表结构的阶段。以下是关键实现步骤和相关类：

---

### 核心流程与关键类
#### 1. **类型映射入口点：`MySqlSyncTableAction` 或 `MySqlSyncDatabaseAction`**
- **类**：
    - `org.apache.paimon.flink.action.cdc.mysql.MySqlSyncTableAction`（同步单表）
    - `org.apache.paimon.flink.action.cdc.mysql.MySqlSyncDatabaseAction`（同步整库）
- **作用**：  
  作为同步任务的入口，在初始化时会调用元数据解析器，将 MySQL 的列类型转换为 Paimon 的列类型。

#### 2. **元数据解析器：`MySqlTableSchemaBuilder`**
- **类**：  
  `org.apache.paimon.flink.action.cdc.mysql.schema.MySqlTableSchemaBuilder`
- **作用**：  
  解析 MySQL 表的 Schema（列名、类型、精度等），并构建 Paimon 兼容的表结构。  
  核心方法 `buildPaimonSchema()` 会遍历 MySQL 的每一列，调用类型映射工具进行转换。

#### 3. **类型映射工具：`MySqlTypeUtils`**
- **类**：  
  `org.apache.paimon.flink.action.cdc.mysql.schema.MySqlTypeUtils`
- **核心方法**：
  ```java
  public static DataType toPaimonType(
      String mysqlType,  // MySQL 原始类型（如 "int", "varchar(255)"）
      int precision,    // 精度（如 varchar 的长度）
      int scale,        // 刻度（如 decimal 的小数位数）
      boolean nullable  // 是否允许为空
  )
  ```
- **映射逻辑**：  
  通过 `switch-case` 匹配 MySQL 类型，返回对应的 Paimon `DataType`：
  ```java
  switch (mysqlType.toUpperCase()) {
      case "INT":
          return DataTypes.INT();
      case "VARCHAR":
          return DataTypes.VARCHAR(precision);
      case "DECIMAL":
          return DataTypes.DECIMAL(precision, scale);
      case "DATETIME":
          return DataTypes.TIMESTAMP(3);
      // ... 其他类型映射
  }
  ```

#### 4. **Paimon 表结构构建：`SchemaUtils`**
- **类**：  
  `org.apache.paimon.flink.action.cdc.SchemaUtils`
- **作用**：  
  将映射后的 Paimon 列类型组合成完整的表结构（`Schema`），并处理主键、分区键等元数据。

---

### 完整流程示例
1. **获取 MySQL 表结构**：  
   通过 Debezium 解析 MySQL 的 DDL 或 `information_schema`，获取原始列定义。
2. **逐列类型转换**：  
   对每一列调用 `MySqlTypeUtils.toPaimonType()`，例如：
    - MySQL `BIGINT` → Paimon `BIGINT()`
    - MySQL `TINYTEXT` → Paimon `VARCHAR(255)`
    - MySQL `DATETIME` → Paimon `TIMESTAMP(3)`
3. **构建 Paimon 表**：  
   使用转换后的列类型创建 Paimon 表（通过 `Catalog.createTable()`）。

---

### 关键映射规则
MySQL 类型与 Paimon 类型的对应关系如下（部分常见类型）：

| **MySQL 类型**       | **Paimon 类型**               | **处理逻辑**                                  |
|----------------------|-------------------------------|---------------------------------------------|
| `INT`                | `INT()`                       | 直接映射                                     |
| `BIGINT`             | `BIGINT()`                    | 直接映射                                     |
| `VARCHAR(N)`         | `VARCHAR(N)`                  | 保留长度精度                                 |
| `DECIMAL(P,S)`       | `DECIMAL(P,S)`                | 保留精度和小数位                             |
| `DATE`               | `DATE()`                      | 直接映射                                     |
| `DATETIME`           | `TIMESTAMP(3)`                | 转换为毫秒级时间戳                           |
| `TIMESTAMP`          | `TIMESTAMP_LTZ(3)`            | 转换为带时区的时间戳                         |
| `TINYINT(1)`         | `BOOLEAN()`                   | 布尔类型特殊处理                             |
| `BLOB`/`BINARY`      | `VARBINARY(2000000)`          | 二进制类型映射                               |
| `JSON`               | `STRING()`                    | JSON 转为字符串存储                          |

> 注：完整映射表见 [MySqlTypeUtils 源码](https://github.com/apache/paimon/blob/main/paimon-flink/paimon-flink-cdc/src/main/java/org/apache/paimon/flink/action/cdc/mysql/schema/MySqlTypeUtils.java)。

---

### 配置影响
- **类型覆盖**：  
  可通过 `type-mapping` 参数强制指定映射规则（如将所有字符串映射为 `STRING`）：
  ```sql
  'type-mapping' = 'to-string' -- 或 'to-nullable', 'to-bytes'
  ```
- **精度处理**：  
  若 MySQL 的 `VARCHAR` 未指定长度，默认映射为 `VARCHAR(1)`（可通过 `default-string-length` 调整）。

---

### 总结
- **核心类**：  
  `MySqlTypeUtils`（类型转换）、`MySqlTableSchemaBuilder`（表结构构建）。
- **触发点**：  
  在同步任务启动时（`MySqlSyncTableAction` 初始化阶段）完成类型映射。
- **特点**：  
  映射基于 MySQL 的列元数据（类型名、精度、刻度），按预定义规则转换为 Paimon 的 `DataType` 体系。
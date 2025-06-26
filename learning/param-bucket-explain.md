
在 Apache Paimon 中，`bucket` 参数定义了表数据的存储分桶策略，这是 Paimon 的核心设计之一。不同值对应完全不同的存储和写入模式：

### 🪣 三种 Bucket 模式详解

| **模式**         | **值** | **特点**                                                                 | **适用场景**                     |
|------------------|--------|--------------------------------------------------------------------------|----------------------------------|
| **固定桶模式**    | >0     | 固定数量的桶，桶数不可变                                                 | 数据分布均匀的静态表             |
| **动态桶模式**    | -1     | 桶数量自动扩展/收缩，根据数据量调整                                      | 数据量变化大的通用场景（推荐）   |
| **延迟桶模式**    | -2     | 写入时不立即分桶，先存临时文件，Compaction 时才分桶                      | 超高吞吐写入的 Append-Only 表    |

---

### 🔧 深度解析三种模式

#### 1. 固定桶模式 (`bucket = N`, N > 0)
```sql
CREATE TABLE fixed_bucket_table (...) WITH (
  'bucket' = '5'  -- 固定5个桶
);
```
- **工作原理**：
    - 数据根据主键哈希分配到固定 N 个桶
    - 每个桶对应一个目录（如 `bucket-0`, `bucket-1`）
    - 桶数量**永不改变**
- **局限性**：
    - 数据倾斜时某些桶会过大
    - 扩容需重写全表数据
    - 小表可能产生空桶

#### 2. 动态桶模式 (`bucket = -1`) ★ 最常用
```sql
CREATE TABLE dynamic_bucket_table (...) WITH (
  'bucket' = '-1'  -- 动态桶模式
);
```
- **核心机制**：
    1. **初始状态**：只有 1 个桶
    2. **桶分裂**：当桶数据量 > `bucket-target-file-size`（默认 128MB）时，自动分裂为 2 个新桶
    3. **桶合并**：当桶数据量 < 分裂阈值/2 时，可能合并相邻桶
- **优势**：
    - 自动适应数据增长
    - 避免小文件问题
    - 优化查询性能（减少扫描数据量）
- **配置参数**：
  ```sql
  ALTER TABLE my_table SET (
    'bucket-target-file-size' = '256mb',  -- 桶分裂阈值
    'min-bucket-num' = '1',               -- 最小桶数
    'max-bucket-num' = '100'              -- 最大桶数
  );
  ```

#### 3. 延迟桶模式 (`bucket = -2`)
```sql
CREATE TABLE postpone_bucket_table (...) WITH (
  'bucket' = '-2',        -- 延迟桶模式
  'write-mode' = 'append-only'  -- 必须为append-only
);
```
- **特殊工作流**：
  ```mermaid
  graph LR
    A[数据写入] --> B{是否Compaction触发?}
    B -->|否| C[写入临时文件]
    B -->|是| D[读取所有临时文件]
    D --> E[按主键分桶]
    E --> F[写入正式桶目录]
  ```
- **设计目的**：
    - **极高写入吞吐**：避免写入时立即分桶的开销
    - **减少小文件**：临时文件更大，合并时生成优化后的桶文件
- **严格限制**：
    - 仅支持 `append-only` 表（不可更新/删除）
    - 查询性能较差（需扫描所有临时文件）

---

### ⚠️ 关键注意事项

1. **模式切换限制**：
   ```sql
   -- 禁止从固定桶切换为动态桶
   ALTER TABLE t SET ('bucket' = '-1'); -- 报错！
   
   -- 唯一允许的切换方向：
   ALTER TABLE t SET ('bucket' = '-2'); -- 固定桶 → 延迟桶（需满足append-only）
   ```

2. **动态桶的扩容过程**：
   ```mermaid
   graph TB
     A[桶0: 128MB] -->|达到阈值| B[分裂为桶0和桶1]
     B --> C[桶0: 64MB] 
     B --> D[桶1: 64MB]
     D -->|继续写入| E[桶1: 180MB]
     E -->|再次达到阈值| F[分裂为桶1和桶2]
   ```

3. **延迟桶的 Compaction 触发**：
   ```sql
   -- 手动触发延迟桶的分桶操作
   CALL sys.compact('db.postpone_table');
   
   -- 自动触发配置
   ALTER TABLE postpone_table SET (
     'compaction.trigger' = 'process-time',  -- 按处理时间触发
     'compaction.interval' = '5 min'         -- 每5分钟尝试一次
   );
   ```

---

### 🚀 生产环境建议

#### 场景 1：CDC 数据同步（如您的 2 亿数据表）
```sql
CREATE TABLE cdc_table (
  id BIGINT PRIMARY KEY,
  ...
) WITH (
  'bucket' = '-1',  -- ★ 动态桶最优
  'bucket-target-file-size' = '256mb',
  'changelog-producer' = 'input'  -- 支持更新
);
```

#### 场景 2：日志类 Append-Only 数据
```sql
CREATE TABLE log_table (
  log_time TIMESTAMP(3),
  ...
) WITH (
  'bucket' = '-2',  -- 延迟桶模式
  'write-mode' = 'append-only',
  'compaction.max.file-num' = '100'  -- 合并大文件
);
```

#### 场景 3：小型静态维度表
```sql
CREATE TABLE dim_table (
  sku_id STRING PRIMARY KEY,
  ...
) WITH (
  'bucket' = '3',  -- 固定少量桶
  'snapshot.time-retained' = '1 h'  -- 短时保留
);
```

### 🔍 问题排查技巧
若遇到桶相关问题：
```sql
-- 查看桶分布状态
SELECT * FROM sys.buckets WHERE table_name = 'my_table';

-- 检查桶大小分布
SELECT 
  bucket, 
  COUNT(*) AS file_count,
  SUM(file_size) AS total_size
FROM sys.snapshots 
WHERE table_name = 'my_table'
GROUP BY bucket;
```

动态桶模式 (`bucket=-1`) 在大多数生产场景中是最佳选择，它能自动平衡数据分布，特别适合您的 2 亿级数据同步场景。


在 Apache Paimon 中，`bucket-key` 是一个**关键的分桶字段配置**，它决定了数据在桶内的分布方式。这个参数与 `bucket` 模式密切相关，但在不同桶模式下其作用和有效性有显著差异。下面我将详细解释：

---

### 🗝️ `bucket-key` 的核心作用
**定义数据在桶内的分布方式**：
- 当数据被分配到某个桶后，`bucket-key` 决定这些数据在桶内文件中的**物理排序和分布**
- 类似于传统数据库中的**聚簇索引**（Clustered Index）
- 主要影响：
  1. **查询性能**：相同 `bucket-key` 的数据物理相邻，范围查询更快
  2. **Compaction效率**：相同键值的数据更容易合并
  3. **数据局部性**：优化 JOIN 和聚合操作

---

### 🔄 `bucket-key` 在不同桶模式下的有效性

#### 1. **固定桶模式 (`bucket > 0`)**
- ✅ **完全有效且推荐使用**
- 工作方式：
  ```mermaid
  graph LR
    A[新数据] --> B{哈希分桶}
    B -->|bucket=5| C[桶0-4]
    C --> D[按bucket-key排序存储]
  ```
- 示例：
  ```sql
  CREATE TABLE users (
      user_id BIGINT,
      region STRING,
      ...
  ) WITH (
      'bucket' = '8',  -- 固定8个桶
      'bucket-key' = 'region'  -- 按region在桶内排序
  )
  ```
- 效果：
  - 相同 `region` 的用户在同一个桶内物理相邻
  - `WHERE region='Asia'` 只需扫描少量文件

#### 2. **动态桶模式 (`bucket = -1`)**
- ⚠️ **有条件有效**
- 限制：
  - 仅当 **`bucket-key` = 主键（或主键子集）** 时才有效
  - 如果设置非主键字段，Paimon 会**静默忽略**该配置
- 原因：
  - 动态桶需要根据主键分裂/合并桶
  - 桶内排序必须与桶分裂逻辑一致
- 正确示例：
  ```sql
  CREATE TABLE orders (
      order_id STRING PRIMARY KEY,
      customer_id BIGINT,
      ...
  ) WITH (
      'bucket' = '-1',
      'bucket-key' = 'order_id'  -- 必须等于主键
  )
  ```

#### 3. **延迟桶模式 (`bucket = -2`)**
- ❌ **完全无效**
- 原因：
  - 延迟桶模式写入时**不立即分桶**
  - 数据以临时文件存储，Compaction 时才分桶
  - `bucket-key` 在写入阶段无法生效
- 替代方案：
  ```sql
  CREATE TABLE logs (
      log_time TIMESTAMP(3),
      device_id STRING,
      ...
  ) WITH (
      'bucket' = '-2',
      'write-mode' = 'append-only',
      -- 使用partition和sort键代替
      'partition' = 'dt', 
      'sort-key' = 'log_time,device_id'
  )
  ```

---

### 💡 最佳实践总结

| **桶模式**      | `bucket-key` 是否有效 | 推荐配置策略                          |
|----------------|---------------------|---------------------------------------|
| **固定桶 (>0)**  | ✅ 有效              | 设置为高频过滤/JOIN的字段              |
| **动态桶 (-1)**  | ⚠️ 仅当等于主键时有效 | 必须设置为表的主键字段                  |
| **延迟桶 (-2)**  | ❌ 无效              | 改用 `partition` + `sort-key` 替代方案 |

---

### ⚙️ 配置建议

#### 场景 1：主键表（如用户表）
```sql
CREATE TABLE users (
    user_id STRING PRIMARY KEY,
    country STRING,
    ...
) WITH (
    'bucket' = '-1',  -- 动态桶
    'bucket-key' = 'user_id',  -- 必须=主键
    'bucket-target-file-size' = '256mb'
)
```

#### 场景 2：无主键大表（如日志表）
```sql
CREATE TABLE server_logs (
    log_time TIMESTAMP(3),
    ip STRING,
    ...
) WITH (
    'bucket' = '32',  -- 固定桶
    'bucket-key' = 'log_time',  -- 按时间排序
    'partition' = 'dt'  -- 按天分区
)
```

#### 场景 3：超高吞吐写入
```sql
CREATE TABLE sensor_data (
    device STRING,
    ts TIMESTAMP(3),
    ...
) WITH (
    'bucket' = '-2',  -- 延迟桶
    'write-mode' = 'append-only',
    -- bucket-key无效，改用sort-key
    'sort-key' = 'device,ts',
    'compaction.trigger' = 'process-time'
)
```

---

### ⚠️ 重要注意事项
1. **动态桶的特殊约束**：
   ```java
   // 如果尝试设置非主键bucket-key会报错
   if (bucket == -1 && !bucketKeys.equals(primaryKeys)) {
       throw new IllegalArgumentException("Dynamic bucket mode requires bucket-key=primary key");
   }
   ```

2. **性能影响**：
  - 好的 `bucket-key` 可提升 3-10 倍查询性能
  - 错误配置可能导致数据倾斜（固定桶模式）

3. **存储结构可视化**：
   ```
   /user/hive/warehouse/my_table
     ├── bucket-0
     │   ├── part-0.parquet  # 内部按bucket-key排序
     │   └── part-1.parquet
     ├── bucket-1
     │   └── ... 
     └── ...
   ```

通过合理配置 `bucket-key`，您可以显著优化 Paimon 表的查询性能和写入效率，特别是在处理亿级数据时效果更为明显。
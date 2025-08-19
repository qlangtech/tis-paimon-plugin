## compaction

Apache Paimon 的 Compaction（压缩）机制是其核心功能之一，尤其在基于 LSM-Tree（Log-Structured Merge-Tree）架构的表格式中至关重要。它主要负责解决由频繁数据写入（尤其是流式写入）带来的小文件问题、读放大问题，并优化数据组织以提升查询性能。

Apache Paimon 的 Compaction 机制是其保证高性能、低延迟查询，尤其是处理高频更新/删除和流式写入场景的基石。它通过智能地合并小文件、清理过期数据、按主键排序数据，有效解决了 LSM-Tree 架构带来的挑战。Universal Compaction 策略因其对流式写入的友好性和易用性，成为 Paimon 的默认和推荐选项。理解并合理配置 Compaction 策略和参数，对于在生产环境中高效稳定地使用 Paimon 至关重要。

## snapshot

Apache Paimon 的 Snapshot（快照）机制 是其实现多版本并发控制（MVCC）、时间旅行查询（Time Travel）、增量计算和数据一致性的核心基础。它借鉴了 Iceberg 等现代数据表格式的设计思想，为存储在 Paimon 表中的数据提供了原子性、一致性、隔离性和持久性（ACID）的保证，特别适用于流批一体和实时更新的场景。

## paimonWriteMode

Paimon 的写入模式（`write-mode`）是影响数据写入行为的关键配置，`batch` 和 `stream` 两种模式在底层实现、数据可见性和适用场景上有本质区别：

🔄 **核心区别对比**

| **特性**               | **Batch 模式**                     | **Stream 模式**                   |
|------------------------|-----------------------------------|----------------------------------|
| **设计目标**           | 高吞吐批量处理                    | 低延迟实时处理                   |
| **数据可见性**         | 批次完成后可见                    | **实时可见**                     |
| **提交机制**           | 显式提交（如 Flink Checkpoint）   | 自动增量提交                     |
| **文件生成**           | 大文件（MB-GB级）                 | 小文件（KB-MB级）                |
| **写入延迟**           | 秒级~分钟级                       | **毫秒级~秒级**                  |
| **典型数据源**         | Hive/离线数仓                     | Kafka/CDC 流                    |
| **压缩效率**           | 高（大文件易压缩）                | 低（需额外小文件合并）           |
| **元数据开销**         | 低（单个提交点）                  | 高（频繁生成提交点）             |
| **Exactly-Once 保证**  | 依赖批处理框架                    | **原生支持**                     |

🚀 **适用场景**

✅ **Batch 模式最佳场景**
1. **离线数仓同步**
    - 每日全量同步 Hive 表：`write-mode=batch`
2. **大规模历史数据回填**
    - 初始化 TB 级历史数据
3. **资源敏感型环境**
    - 机械硬盘集群（减少 IOPS 压力）
4. **OLAP 分析优化**

✅ **Stream 模式最佳场景**
1. **实时 CDC 管道**
2. **低延迟监控看板**
    - 实时订单看板（要求 5s 内可见）
3. **事件驱动型应用**
    - 用户行为实时分析（点击流处理）
4. **渐进式更新场景**


## changelog

`changelog-producer` 的作用
1. **核心功能**
   - **生成变更日志**：将底层存储文件的物理更新（如覆盖写入）**转换为逻辑变更事件**（`+I`/`-U`/`+U`/`-D`）。
   - **支持流式消费**：让 Flink 等引擎能像读 Kafka 一样**增量读取 Paimon 表的变更**（类似 `SELECT * FROM table /*+ OPTIONS('scan.mode'='incremental') */`）。

2. 为何需要它？
   - **湖仓痛点**：传统湖存储（如 Parquet）只存最终状态，缺少 **"行级变更记录"**，无法支持增量计算。
   - **流读需求**：Flink CDC 等场景需要实时捕获 `INSERT/UPDATE/DELETE` 事件。

## writeBuffer  

在 Apache Paimon 中，控制底层（特别是 Level 0）小文件大小的核心参数直接影响着 Compaction 的触发频率。**通过适当增加单个文件中存储的记录条数（或文件大小），确实可以有效减少 L0 层小文件的数量，从而降低 Compaction 被触发的频率。**

以下是控制 L0 层小文件大小和记录数的关键参数及其工作原理：

 🧩 1. 核心控制参数：`write-buffer-size` 和 `target-file-size`

*   **`write-buffer-size` (默认值: 128mb):**
   *   **作用：** 这是控制**内存中 Buffer** 大小的参数。当 Flink 或 Spark 任务向 Paimon 写入数据时，数据首先会缓存在内存中的一个排序缓冲区（Sorter Buffer）中。
   *   **原理：** 当一个 Buffer 被填满（达到 `write-buffer-size`）时，它会被排序并**刷写（Spill）到磁盘，形成一个 L0 文件**。
   *   **影响：** `write-buffer-size` **直接决定了单个 L0 文件的最小期望大小**。如果你期望每个 L0 文件大约是 256MB，那么就应该设置 `write-buffer-size=256mb`。Buffer 满了就刷写，自然就生成了一个 ~256MB 的文件。

*   **`target-file-size` (默认值: 128mb):**
   *   **作用：** 这是 Paimon **最终期望生成的稳定数据文件（通常是经过 Compaction 合并到更高层级的文件）的目标大小**。
   *   **原理：** 在 **Compaction 过程**中（无论是 Universal 还是 Size-Tiered），Paimon 会尝试将输入的小文件合并，并输出大小接近 `target-file-size` 的文件到更高的层级（如 L1, L2）。
   *   **影响 L0 的间接方式：** 虽然 `target-file-size` 主要影响 Compaction 的输出，但它**间接影响 L0 文件的“合并潜力”**。如果 `target-file-size` 设置得很大（比如 1GB），那么 Compaction 需要积累更多的 L0 小文件（累计大小达到 ~1GB）才会触发合并操作。这相当于放宽了触发 Compaction 的“累计大小”阈值。
   *   **注意：** `target-file-size` **不直接控制单个 L0 文件的大小**。L0 文件大小主要由 `write-buffer-size` 和 Checkpointing 决定。

 🧩 2. 其他影响 L0 文件大小和记录数的因素

*   **Checkpointing / Commit Interval (Flink Streaming):**
   *   在 Flink 流式写入场景下，**Flink 的 Checkpoint 间隔**是决定 L0 文件大小和数量的**最关键因素之一**。
   *   **原理：** Paimon 的 Sink 算子通常会在 Flink Checkpoint 时执行 `snapshotState`。为了确保精确一次的语义，当前内存 Buffer 中的数据（即使未达到 `write-buffer-size`）也会在 Checkpoint 时强制刷写到磁盘，生成一个 L0 文件。
   *   **影响：** 如果 Flink 的 Checkpoint 间隔（`checkpoint.interval`）很短（例如 10 秒），即使 `write-buffer-size` 设置为 256MB，也可能因为每个 Checkpoint 只积累了少量数据（比如 50MB）就被强制刷出，从而导致产生大量远小于 `write-buffer-size` 的 L0 小文件。
   *   **结论：要减少 L0 小文件数量，在流式写入场景下，适当增加 Flink Checkpoint 间隔 (`checkpoint.interval`) 是至关重要的**，让 Buffer 有更多时间积累接近 `write-buffer-size` 的数据量再刷出。但需权衡故障恢复时间（RTO）。

*   **`sort-spill-threshold` (默认值: 未明确设置，通常内部管理):**
   *   这个参数控制内存排序缓冲区在内存中最多能容纳多少行数据。当行数超过此阈值时，即使 Buffer 的内存占用未达到 `write-buffer-size`，也可能触发部分数据溢写（Spill）。这主要用于防止 OOM。
   *   **影响：** 如果记录非常大（例如宽表、大 JSON 对象），即使记录数不多，也可能快速占满内存 Buffer (`write-buffer-size`) 而刷写。如果记录非常小（例如计数器更新），则 `sort-spill-threshold` 可能先达到，导致按记录数刷写。**调整 `sort-spill-threshold` 主要应对特殊数据分布场景，一般优先调整 `write-buffer-size` 和 Checkpoint 间隔。**

 ✅ 如何通过增加文件大小/记录数减少 Compaction 频率

1.  **增大 `write-buffer-size`:**
   *   这是**最直接有效**的方法。例如，从默认的 `128mb` 增加到 `256mb` 或 `512mb`。
   *   **效果：** 每个 L0 文件平均变大（更接近新设置的 buffer size），L0 层积累到触发 Compaction 阈值（如 Universal 的 `num-sorted-run.stop-trigger`）所需的**文件数量变少**，从而降低触发频率。
   *   **注意：** 需要确保 TaskManager 有足够的 JVM Heap 或 Managed Memory 来容纳更大的 Buffer。否则可能导致 OOM 或频繁 GC。

2.  **增大 Flink Checkpoint 间隔 (`checkpoint.interval`):**
   *   对于流式写入，这是**避免超小 L0 文件的关键**。根据业务容忍的延迟和恢复时间，尽可能增加间隔（例如从 30s 增加到 1min 或 5min）。
   *   **效果：** 显著减少因频繁 Checkpoint 强制刷写产生的远小于 `write-buffer-size` 的微型 L0 文件数量。让大多数 L0 文件大小接近 `write-buffer-size`。

3.  **增大 `target-file-size`:**
   *   例如从 `128mb` 增加到 `256mb` 或 `512mb`。
   *   **效果：** 主要在 Universal Compaction 中影响较大。因为 Universal 在合并时会尽量将一组文件合并成一个接近 `target-file-size` 的大文件。增大 `target-file-size` 意味着 Compaction 需要积累更多的 L0 文件（总数据量更大）才会触发合并。对于 Size-Tiered，它定义了更高层级文件的大小目标，也间接影响合并策略。
   *   **注意：** 过大的 `target-file-size` 可能影响查询效率（读取大文件可能慢）和 Compaction 本身的资源消耗（合并更大量数据）。

4.  **调整 Compaction 触发阈值 (如 Universal 的 `num-sorted-run.stop-trigger`):**
   *   虽然不是直接控制文件大小，但**配合增大文件大小**，调整这个阈值效果更好。例如，如果文件平均大了 2 倍，那么将 `num-sorted-run.stop-trigger` 从默认的 5 增加到 8 或 10，可能仍然能保持或延长触发间隔，同时允许 L0 积累更多文件（但总数据量更大），进一步提升读性能。
   *   **效果：** 直接放宽了触发 Compaction 的条件，让 L0 层可以堆积更多的文件（但每个文件更大了）才触发合并。

 ⚠️ 总结与权衡

*   **核心策略：** **增大 `write-buffer-size` 并适当增大 Flink Checkpoint 间隔** 是减少 L0 小文件数量、从而降低 Compaction 频率的最有效手段。这直接让每个 L0 文件包含更多记录/更大体积。
*   **辅助策略：** 增大 `target-file-size` 和/或 Compaction 触发阈值（如 `num-sorted-run.stop-trigger`）可以进一步减少 Compaction 频率，因为它们提高了触发合并所需的“数据量”或“文件数”门槛。
*   **重要权衡：**
   *   **内存资源：** 增大 `write-buffer-size` 会增加每个 Writer Task 的内存需求。
   *   **故障恢复：** 增大 Flink Checkpoint 间隔会增加故障恢复时需要重放的数据量（RTO 可能变长）。
   *   **读取延迟：** 过大的 L0 文件或过少的 Compaction 可能导致查询（尤其是点查）需要扫描更多的 L0 文件，增加读取延迟。需要监控读性能。
   *   **空间放大：** 减少 Compaction 频率可能导致短时间内存在更多冗余数据（旧版本数据、删除标记等），增加存储空间占用（空间放大）。需要配合合理的快照过期策略。

**建议：** 从调整 `write-buffer-size` (如 256MB) 和 Flink `checkpoint.interval` (如 1-5分钟) 开始，观察 L0 文件平均大小是否显著增大、Compaction 频率是否下降。再根据效果和资源/延迟情况，考虑是否调整 `target-file-size` 或 Compaction 触发参数。务必监控集群资源（内存、CPU）、Compaction 耗时、查询延迟和存储空间变化。📊

## tableBucket

`bucket` 参数定义了表数据的存储分桶策略，这是 Paimon 的核心设计之一。不同值对应完全不同的存储和写入模式：

 🪣 三种 Bucket 模式详解

| **模式**             | **值** | **特点**                                                                 | **适用场景**                     |
|--------------------|--------|--------------------------------------------------------------------------|----------------------------------|
| **固定桶模式(Fixed)**   | >0     | 固定数量的桶，桶数不可变                                                 | 数据分布均匀的静态表             |
| **动态桶模式(Dynamic)** | -1     | 桶数量自动扩展/收缩，根据数据量调整                                      | 数据量变化大的通用场景（推荐）   |
| **延迟桶模式(Postpone)**        | -2     | 写入时不立即分桶，先存临时文件，Compaction 时才分桶                      | 超高吞吐写入的 Append-Only 表    |
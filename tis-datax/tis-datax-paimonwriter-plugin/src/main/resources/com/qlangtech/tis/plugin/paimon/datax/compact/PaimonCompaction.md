##numStoredRunsTrigger

`num-sorted-run.compaction-trigger = N` 意味着：**当 Paimon 表（LSM 结构）的磁盘文件片段（Sorted Run）积累到 `N` 个时，系统会自动启动后台合并（Compaction）任务，将这些文件合并成更少、更大的文件，以优化后续的查询性能和空间利用率。** 这个参数是平衡写入吞吐量和查询性能的关键杠杆之一。

##numStoredRunsStopTrigger

以下是 `num-sorted-run.stop-trigger` 的详细解释：

1.  **背景：写入与合并的速率差**
    *   数据持续写入会不断产生新的 Sorted Run。
    *   后台的 Compaction 任务负责合并这些 Sorted Run 以减少其数量。
    *   理想情况下，Compaction 的速度应该能跟上写入产生新 Sorted Run 的速度，保持 Sorted Run 数量在 `compaction-trigger` 附近波动。
    *   但是，如果**写入速率远远超过 Compaction 的处理能力**，Sorted Run 的数量就会持续增长。

2.  **`num-sorted-run.stop-trigger` 的核心作用**
    *   这个参数定义了一个**更高的 Sorted Run 数量阈值**。
    *   当磁盘上的 Sorted Run 数量**达到或超过**这个 `stop-trigger` 设定的值时，Paimon 会采取**强制措施**：**暂停（或显著减慢）新的数据写入操作**。
    *   **简单说：它设定了“积攒的文件片段（Sorted Run）数量达到某个危险上限时，必须停止写入，优先让合并追赶上来”。**
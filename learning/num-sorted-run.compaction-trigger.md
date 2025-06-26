在 Apache Paimon 的 LSM 树结构中，`num-sorted-run.stop-trigger` 是一个与 `num-sorted-run.compaction-trigger` **紧密相关且非常重要**的配置参数，它主要用于**控制写入阻塞**，起到**安全阀**的作用。

在 Apache Paimon 中，`num-sorted-run.compaction-trigger` 是一个**非常重要的配置参数**，它直接关系到 LSM 树（Log-Structured Merge-Tree）结构的**合并（Compaction）触发时机**。

以下是它的作用详解：

1.  **核心概念：Sorted Run**
    *   当数据写入 Paimon 表时（尤其是采用 LSM 结构的表如主键表），数据首先写入内存缓冲区（MemTable）。
    *   当 MemTable 写满时，它会被刷新（Flush）到磁盘，形成一个**有序的文件片段**。这个在磁盘上的、内部有序的文件片段就称为一个 **Sorted Run**。
    *   随着持续写入，磁盘上会积累多个 Sorted Run。

2.  **问题：多个 Sorted Run 的影响**
    *   每个 Sorted Run 内部数据是有序的，但**不同 Sorted Run 之间的数据范围可能有重叠**。
    *   当执行查询（尤其是范围查询）时，为了获取正确的结果，查询引擎可能需要**读取多个 Sorted Run** 并合并结果。这会显著**增加读取放大（Read Amplification）**，降低查询性能。
    *   随着 Sorted Run 数量的增多，这个问题会越来越严重。

3.  **解决方案：Compaction（合并）**
    *   Compaction 是 LSM 树的核心后台过程。它的主要目标是将**多个较小的、可能有重叠的 Sorted Run 合并成数量更少、更大且范围更清晰（甚至无重叠）的 Sorted Run**。
    *   合并后的好处：
        *   **减少读取放大：** 查询需要扫描的文件数量减少，提升查询性能。
        *   **优化空间：** 合并过程可以清除标记为删除的数据（Delete Tombstones），释放磁盘空间。
        *   **维持有序性：** 保证数据在更大范围内保持有序性，利于后续查询和进一步合并。

4.  **`num-sorted-run.compaction-trigger` 的作用**
    *   这个参数**定义了在触发 Compaction 之前，磁盘上允许存在的 Sorted Run 的最大数量阈值**。
    *   **当磁盘上的 Sorted Run 数量达到或超过这个参数设定的值时，Paimon 的后台 Compaction 任务就会被自动触发**，开始合并这些 Sorted Run。
    *   **简单说：它控制着“积攒多少个文件片段（Sorted Run）后就该启动合并了”。**

5.  **参数值的权衡**
    *   **设置较低 (e.g., 2, 3, 5):**
        *   **优点：** Compaction 触发更频繁，能更快地减少 Sorted Run 数量，保持较优的读取性能。查询延迟更稳定。
        *   **缺点：** 后台 Compaction 任务更频繁，消耗更多的 CPU、I/O 和网络（分布式环境）资源。可能会对写入吞吐量造成一定影响（写入线程可能偶尔需要等待 Compaction 释放资源）。
    *   **设置较高 (e.g., 10, 20, 50):**
        *   **优点：** Compaction 触发不那么频繁，后台资源消耗较低，写入吞吐量受影响较小。
        *   **缺点：** 在触发 Compaction 之前，磁盘上会堆积较多的 Sorted Run，导致读取性能下降（查询需要扫描更多文件）。查询延迟可能波动较大。

6.  **典型值与建议**
    *   默认值通常为 `5` (具体请查阅你使用的 Paimon 版本文档)。
    *   选择最佳值需要根据你的**具体工作负载进行权衡**：
        *   **优先查询性能：** 设置较低的值 (如 3-5)。
        *   **优先写入吞吐量：** 设置较高的值 (如 8-10 或更高)。
        *   **混合负载：** 通常折中选择 (如 5-8)。
    *   监控 Sorted Run 的数量、查询延迟和系统资源使用情况是调整该参数的关键。

**总结:**

`num-sorted-run.compaction-trigger = N` 意味着：**当 Paimon 表（LSM 结构）的磁盘文件片段（Sorted Run）积累到 `N` 个时，系统会自动启动后台合并（Compaction）任务，将这些文件合并成更少、更大的文件，以优化后续的查询性能和空间利用率。** 这个参数是平衡写入吞吐量和查询性能的关键杠杆之一。

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

3.  **为什么需要暂停写入？**
    *   **防止系统失控：** 如果不加限制，Sorted Run 数量可能无限增长。
    *   **避免灾难性后果：**
        *   **查询性能崩溃：** Sorted Run 数量过多会导致任何查询都需要扫描巨量文件，查询变得极其缓慢甚至超时。
        *   **内存压力剧增：** 管理大量 Sorted Run 的元数据（如文件列表、Key 范围）会消耗大量内存，可能导致 OOM (Out-Of-Memory)。
        *   **磁盘空间耗尽：** 大量未合并的 Sorted Run 可能包含大量重复或已删除数据的中间状态（Tombstones），占用过多空间。
        *   **Compaction 雪崩：** 积压的 Sorted Run 过多，后续的 Compaction 任务会变得非常庞大和耗时，进一步恶化情况，形成恶性循环。

4.  **与 `compaction-trigger` 的关系**
    *   `compaction-trigger (N)`: **启动** Compaction 的阈值。当 Sorted Run >= N 时，**触发**后台 Compaction。
    *   `stop-trigger (M)`: **停止**写入的阈值。当 Sorted Run >= M 时，**阻塞**新写入，让 Compaction 优先追赶。`M` **必须严格大于** `N` (`M > N`)。
    *   **工作区间：**
        *   `Sorted Runs < N`: 正常写入，Compaction 空闲或处理低优先级任务。
        *   `N <= Sorted Runs < M`: 正常写入，**后台 Compaction 被触发并持续工作**，尝试将 Sorted Run 数量降低到 `N` 以下。系统处于“追赶”状态。
        *   `Sorted Runs >= M`: **写入被暂停或严重限流**。系统全力进行 Compaction，直到 Sorted Run 数量回落到 `M` 以下（通常会努力降到 `N` 以下），才恢复写入。系统处于“紧急制动”状态。

5.  **参数值的设置建议**
    *   **`M` (stop-trigger) 必须大于 `N` (compaction-trigger)**：这是硬性要求。
    *   **典型比例：** `M` 通常是 `N` 的 **2 到 5 倍** 左右。例如：
        *   如果 `compaction-trigger = 5`, 那么 `stop-trigger` 可以设置为 `10`, `15`, `20` 或 `25`。
    *   **权衡考虑：**
        *   **设置 `M` 较大 (e.g., M = 25, N=5)**:
            *   **优点：** 对短暂的写入高峰容忍度更高，不太容易触发写入暂停，写入吞吐量更平滑。
            *   **缺点：** 一旦达到 `M`，系统积压的问题已经很严重（Sorted Run 非常多），Compaction 清理积压需要很长时间，期间查询性能会非常差，恢复写入的等待时间也较长。内存和磁盘压力风险更高。
        *   **设置 `M` 较小 (e.g., M = 10, N=5)**:
            *   **优点：** 能更快地对写入积压做出反应，防止问题恶化到不可收拾的地步。恢复速度相对较快。
            *   **缺点：** 对写入波动的容忍度较低，更容易触发写入暂停，可能导致写入吞吐量出现毛刺。
    *   **监控是关键：** 观察 Sorted Run 数量的变化趋势、Compaction 任务的进度和延迟、以及是否频繁触发写入暂停 (`stop-trigger`)，是调整 `N` 和 `M` 最重要的依据。目标是让 Sorted Run 数量大部分时间稳定在 `N` 附近，避免频繁达到 `M`。

**总结:**

`num-sorted-run.stop-trigger = M` (且 `M > num-sorted-run.compaction-trigger = N`) 意味着：**当磁盘上的 Sorted Run 数量达到 `M` 这个更高的警戒线时，Paimon 会主动暂停或严重限制新的数据写入。** 这是一个保护机制，目的是在后台 Compaction 严重落后于写入速度、系统面临性能或稳定性风险时，强制让 Compaction 优先处理积压的 Sorted Run，直到情况缓解（Sorted Run 数量回落到 `M` 以下）。它和 `compaction-trigger` 共同构成了 LSM 树管理写入与合并平衡、保障系统稳定的关键控制参数。

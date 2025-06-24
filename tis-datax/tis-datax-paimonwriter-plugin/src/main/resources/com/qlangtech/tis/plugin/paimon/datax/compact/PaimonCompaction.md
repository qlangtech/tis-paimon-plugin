## writeBufferSize
**`write-buffer-size` (默认值: 128mb):**
  *   **作用：** 这是控制**内存中 Buffer** 大小的参数。当 Flink 或 Spark 任务向 Paimon 写入数据时，数据首先会缓存在内存中的一个排序缓冲区（Sorter Buffer）中。
  *   **原理：** 当一个 Buffer 被填满（达到 `write-buffer-size`）时，它会被排序并**刷写（Spill）到磁盘，形成一个 L0 文件**。
  *   **影响：** `write-buffer-size` **直接决定了单个 L0 文件的最小期望大小**。如果你期望每个 L0 文件大约是 256MB，那么就应该设置 `write-buffer-size=256mb`。Buffer 满了就刷写，自然就生成了一个 ~256MB 的文件。

## targetFileSize
**`target-file-size` (默认值: 128mb):**
  *   **作用：** 这是 Paimon **最终期望生成的稳定数据文件（通常是经过 Compaction 合并到更高层级的文件）的目标大小**。
  *   **原理：** 在 **Compaction 过程**中（无论是 Universal 还是 Size-Tiered），Paimon 会尝试将输入的小文件合并，并输出大小接近 `target-file-size` 的文件到更高的层级（如 L1, L2）。
  *   **影响 L0 的间接方式：** 虽然 `target-file-size` 主要影响 Compaction 的输出，但它**间接影响 L0 文件的“合并潜力”**。如果 `target-file-size` 设置得很大（比如 1GB），那么 Compaction 需要积累更多的 L0 小文件（累计大小达到 ~1GB）才会触发合并操作。这相当于放宽了触发 Compaction 的“累计大小”阈值。
  *   **注意：** `target-file-size` **不直接控制单个 L0 文件的大小**。L0 文件大小主要由 `write-buffer-size` 和 Checkpointing 决定。
  *   **效果：** 主要在 Universal Compaction 中影响较大。因为 Universal 在合并时会尽量将一组文件合并成一个接近 `target-file-size` 的大文件。增大 `target-file-size` 意味着 Compaction 需要积累更多的 L0 文件（总数据量更大）才会触发合并。对于 Size-Tiered，它定义了更高层级文件的大小目标，也间接影响合并策略。
  *   **注意：** 过大的 `target-file-size` 可能影响查询效率（读取大文件可能慢）和 Compaction 本身的资源消耗（合并更大量数据）。
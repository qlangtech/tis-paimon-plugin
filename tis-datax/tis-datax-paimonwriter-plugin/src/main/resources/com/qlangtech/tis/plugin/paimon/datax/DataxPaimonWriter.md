## compaction

Apache Paimon 的 Compaction（压缩）机制是其核心功能之一，尤其在基于 LSM-Tree（Log-Structured Merge-Tree）架构的表格式中至关重要。它主要负责解决由频繁数据写入（尤其是流式写入）带来的小文件问题、读放大问题，并优化数据组织以提升查询性能。

Apache Paimon 的 Compaction 机制是其保证高性能、低延迟查询，尤其是处理高频更新/删除和流式写入场景的基石。它通过智能地合并小文件、清理过期数据、按主键排序数据，有效解决了 LSM-Tree 架构带来的挑战。Universal Compaction 策略因其对流式写入的友好性和易用性，成为 Paimon 的默认和推荐选项。理解并合理配置 Compaction 策略和参数，对于在生产环境中高效稳定地使用 Paimon 至关重要。

## snapshot

Apache Paimon 的 Snapshot（快照）机制 是其实现多版本并发控制（MVCC）、时间旅行查询（Time Travel）、增量计算和数据一致性的核心基础。它借鉴了 Iceberg 等现代数据表格式的设计思想，为存储在 Paimon 表中的数据提供了原子性、一致性、隔离性和持久性（ACID）的保证，特别适用于流批一体和实时更新的场景。

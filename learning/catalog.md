## 目录
1.  [paimon 中 底层（Level 0或更低层）的多个小文件，单个小文件中存储的记录数是有什么参数控制的，我理解只要适当增加单个文件中存储的记录条数，就能减少compaction执行的频率了吧](how-to-control-LSM-level0-single-filesize.md)
2. 数据写入LSM 的L0层 ，数据并不能在查询端（例如hive jdbc查询）可见，还需要经过commit执行后，进入L1级才能通过 hive jdbc 客户端查询到，是这样吗？[answer](lsm-leve0-relation-with-querable.md)
3. paimon 中 num-sorted-run.compaction-trigger这个配置参数的作用 帮我说明一下 [answer](num-sorted-run.compaction-trigger.md)
4. paion bucket 详细解释 [answer](param-bucket-explain.md)
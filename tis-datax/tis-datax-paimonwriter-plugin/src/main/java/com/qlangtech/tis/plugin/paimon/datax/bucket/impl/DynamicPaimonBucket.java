package com.qlangtech.tis.plugin.paimon.datax.bucket.impl;


import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.extension.util.OverwriteProps;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.paimon.datax.bucket.PaimonBucket;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.index.BucketAssigner;
import org.apache.paimon.index.HashBucketAssigner;
import org.apache.paimon.table.DataTable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.RowPartitionKeyExtractor;
import org.apache.paimon.table.sink.TableWrite;
import org.apache.paimon.utils.MathUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.paimon.CoreOptions.DYNAMIC_BUCKET_ASSIGNER_PARALLELISM;
import static org.apache.paimon.CoreOptions.DYNAMIC_BUCKET_INITIAL_BUCKETS;
import static org.apache.paimon.CoreOptions.DYNAMIC_BUCKET_MAX_BUCKETS;
import static org.apache.paimon.CoreOptions.DYNAMIC_BUCKET_TARGET_ROW_NUM;


/**
 * 桶数量自动扩展/收缩，根据数据量调整
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-06-25 14:47
 **/
public class DynamicPaimonBucket extends PaimonBucket {

    /**
     * @see org.apache.paimon.CoreOptions#DYNAMIC_BUCKET_TARGET_ROW_NUM
     */
    @FormField(ordinal = 1, type = FormFieldType.INT_NUMBER, validate = {Validator.require, Validator.integer})
    public Integer targetRowNum;

    /**
     * @see org.apache.paimon.CoreOptions#DYNAMIC_BUCKET_INITIAL_BUCKETS
     */
    @FormField(ordinal = 2, advance = true, type = FormFieldType.INT_NUMBER, validate = {Validator.integer})
    public Integer initialBuckets;

    /**
     * @see org.apache.paimon.CoreOptions#DYNAMIC_BUCKET_MAX_BUCKETS
     */
    @FormField(ordinal = 3, advance = true, type = FormFieldType.INT_NUMBER, validate = {Validator.integer})
    public Integer maxBuckets;

    /**
     * @see org.apache.paimon.CoreOptions#DYNAMIC_BUCKET_ASSIGNER_PARALLELISM
     */
    @FormField(ordinal = 4, advance = true, type = FormFieldType.INT_NUMBER, validate = {Validator.integer})
    public Integer parallelism;


    private static final int totalTasksNumber = 1;
    private static final AtomicInteger currentTaskNumber = new AtomicInteger();


    @Override
    public int getBucketCount() {
        return -1;
    }

    /**
     * 只能用在批量同步的场景
     */
    private transient static final ThreadLocal<Map<String, Pair<BucketAssigner, RowPartitionKeyExtractor>>> tableBucketAssignerMapperThreadLocal = new ThreadLocal<>() {
        @Override
        protected Map<String, Pair<BucketAssigner, RowPartitionKeyExtractor>> initialValue() {
            return new HashMap<>();
        }
    };

    /**
     * 参考：org.apache.flink.cdc.connectors.paimon.sink.v2.bucket.BucketAssignOperator#processElement() 方法中的实现
     *
     * @param table
     * @param write
     * @param row
     * @return
     */
    @Override
    public int getBucket(DataTable table, TableWrite write, GenericRow row) {
        Map<String, Pair<BucketAssigner, RowPartitionKeyExtractor>> tableBucketAssignerMapper = tableBucketAssignerMapperThreadLocal.get();
        Pair<BucketAssigner, RowPartitionKeyExtractor> pair = tableBucketAssignerMapper.get(table.fullName());
        if (pair == null) {
            pair = this.getTableInfo(table);
            tableBucketAssignerMapper.put(table.fullName(), pair);
        }
        return pair.getKey().assign(pair.getValue().partition(row), pair.getValue().trimmedPrimaryKey(row).hashCode());
    }

    private Pair<BucketAssigner, RowPartitionKeyExtractor>
    getTableInfo(DataTable table) {
        // Objects.requireNonNull(tableId, "Invalid tableId in given event.");
//        FileStoreTable table;
//        try {
//            table = (FileStoreTable) catalog.getTable(Identifier.fromString(tableId.toString()));
//        } catch (Catalog.TableNotExistException e) {
//            throw new RuntimeException(e);
//        }
        FileStoreTable tab = (FileStoreTable) table;

        long targetRowNum = table.coreOptions().dynamicBucketTargetRowNum();
        Integer numAssigners = table.coreOptions().dynamicBucketInitialBuckets();

        int maxBucketsNum = table.coreOptions().dynamicBucketMaxBuckets();

        return Pair.of(

                // SnapshotManager snapshotManager, String commitUser, IndexFileHandler indexFileHandler, int numChannels, int numAssigners, int assignId, long targetBucketRowNumber, int maxBucketsNum
                new HashBucketAssigner(
                        table.snapshotManager(),
                        null,
                        tab.store().newIndexFileHandler(),
                        totalTasksNumber,
                        MathUtils.min(numAssigners, totalTasksNumber),
                        currentTaskNumber.getAndIncrement() % totalTasksNumber,
                        targetRowNum, maxBucketsNum),
                new RowPartitionKeyExtractor(tab.schema()));
    }

    @TISExtension
    public static class Desc extends BasicDesc {
        public Desc() {
            super();
            OverwriteProps overwriteProps = new OverwriteProps();
            overwriteProps.setLabelRewrite((label) -> {
                return StringUtils.removeStart(label, "dynamic-bucket.");
            });
            opts.add("targetRowNum", DYNAMIC_BUCKET_TARGET_ROW_NUM, overwriteProps);
            opts.add("initialBuckets", DYNAMIC_BUCKET_INITIAL_BUCKETS, overwriteProps);
            opts.add("maxBuckets", DYNAMIC_BUCKET_MAX_BUCKETS, overwriteProps);
            opts.add("parallelism", DYNAMIC_BUCKET_ASSIGNER_PARALLELISM, overwriteProps);
        }

        @Override
        public String getDisplayName() {
            return "Dynamic";
        }
    }


}

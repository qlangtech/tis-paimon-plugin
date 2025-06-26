package com.qlangtech.tis.plugin.paimon.datax.test;

import com.alibaba.datax.plugin.writer.hdfswriter.HdfsColMeta;
import com.google.common.collect.Lists;
import com.qlangtech.tis.datax.IDataxProcessor.TableMap;
import com.qlangtech.tis.plugin.MemorySize;
import com.qlangtech.tis.plugin.datax.common.AutoCreateTable;
import com.qlangtech.tis.plugin.datax.common.impl.NoneCreateTable;
import com.qlangtech.tis.plugin.ds.DataType;
import com.qlangtech.tis.plugin.ds.JDBCTypes;
import com.qlangtech.tis.plugin.paimon.catalog.HiveCatalog;
import com.qlangtech.tis.plugin.paimon.catalog.cache.CatalogCacheOFF;
import com.qlangtech.tis.plugin.paimon.catalog.cache.CatalogCacheON;
import com.qlangtech.tis.plugin.paimon.catalog.lock.CatalogLockOFF;
import com.qlangtech.tis.plugin.paimon.datax.DataxPaimonWriter;
import com.qlangtech.tis.plugin.paimon.datax.PaimonSelectedTab;
import com.qlangtech.tis.plugin.paimon.datax.bucket.impl.DynamicPaimonBucket;
import com.qlangtech.tis.plugin.paimon.datax.bucket.impl.PaimonBucketKeysOFF;
import com.qlangtech.tis.plugin.paimon.datax.changelog.ChangelogProducerOff;
import com.qlangtech.tis.plugin.paimon.datax.compact.PaimonCompaction;
import com.qlangtech.tis.plugin.paimon.datax.pt.OnPaimonPartition;
import com.qlangtech.tis.plugin.paimon.datax.utils.PaimonSnapshot;
import com.qlangtech.tis.plugin.paimon.datax.writebuffer.PaimonWriteBufferCustomize;
import com.qlangtech.tis.plugin.paimon.datax.writemode.BatchInsertWriteMode;
import com.qlangtech.tis.plugin.paimon.datax.writemode.StreamInsertWriteMode;
import org.apache.paimon.CoreOptions;

import static org.apache.paimon.CoreOptions.FILE_FORMAT_PARQUET;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-05-26 10:09
 **/
public class PaimonTestUtils {
    public static final String KEY_HDFS200 = "hdfs200";
    public static final String DATAX_PAIMON_NAME = "paimon_writer";

    public static DataxPaimonWriter getPaimonWriter() {
        return getPaimonWriter(new NoneCreateTable());
    }

    public static DataxPaimonWriter getPaimonWriter(AutoCreateTable autoCreateTable) {
        //   DaMengDataSourceFactory dsFactory = TestDaMengDataSourceFactory.createDaMengDataSourceFactory();

        DataxPaimonWriter writer = new DataxPaimonWriter() {
            @Override
            public Class<?> getOwnerClass() {
                return DataxPaimonWriter.class;
            }
        };
        writer.dataXName = DATAX_PAIMON_NAME;
        writer.template = DataxPaimonWriter.getDftTemplate();
        HiveCatalog hiveCatalog = createHiveCatalog();
        writer.catalog = hiveCatalog;
      //  BatchInsertWriteMode writeMode = new BatchInsertWriteMode();

        StreamInsertWriteMode writeMode = new StreamInsertWriteMode();
        writeMode.batchSize = 100;
        // writer.fsName = KEY_HDFS200;
        writer.paimonWriteMode = writeMode;
        DynamicPaimonBucket dynamicBucket = new DynamicPaimonBucket();
        dynamicBucket.targetRowNum = org.apache.paimon.CoreOptions.DYNAMIC_BUCKET_TARGET_ROW_NUM.defaultValue().intValue();
        writer.tableBucket = dynamicBucket;
        writer.storeFormat = FILE_FORMAT_PARQUET;

        PaimonCompaction compaction = new PaimonCompaction();
        compaction.optimizationInterval = null;// CoreOptions.COMPACTION_OPTIMIZATION_INTERVAL.defaultValue();
        compaction.sizeAmplificationPercent = org.apache.paimon.CoreOptions.COMPACTION_MAX_SIZE_AMPLIFICATION_PERCENT.defaultValue();
        compaction.sizeRatio = CoreOptions.COMPACTION_SIZE_RATIO.defaultValue();
        compaction.minFileNum = CoreOptions.COMPACTION_MIN_FILE_NUM.defaultValue();
        compaction.writeOnly = false;
        writer.compaction = compaction;

        PaimonSnapshot snapshot = new PaimonSnapshot();
        snapshot.retainedMax = org.apache.paimon.CoreOptions.SNAPSHOT_NUM_RETAINED_MAX.defaultValue();
        snapshot.retainedMin = org.apache.paimon.CoreOptions.SNAPSHOT_NUM_RETAINED_MIN.defaultValue();
        snapshot.timeRetained = org.apache.paimon.CoreOptions.SNAPSHOT_TIME_RETAINED.defaultValue();
        writer.snapshot = snapshot;

        writer.autoCreateTable = autoCreateTable;
        writer.changelog = new ChangelogProducerOff();
        PaimonWriteBufferCustomize writeBuffer = new PaimonWriteBufferCustomize();
        writeBuffer.writeBufferSize = MemorySize.ofBytes(CoreOptions.WRITE_BUFFER_SIZE.defaultValue().getBytes());
        writer.writeBuffer = writeBuffer;
        //writer.autoCreateTable = AutoCreateTable.dft();

        return writer;
    }

    public static HiveCatalog createHiveCatalog() {
        HiveCatalog hiveCatalog = new HiveCatalog();
        // hiveCatalog.dbName = "hive200_2";
        hiveCatalog.tableOwner = "hive";
        hiveCatalog.dbName = "hive200_2";
        hiveCatalog.fsName = KEY_HDFS200;
        //hiveCatalog.catalogPath = "/user/hive/warehouse/paimon";

        hiveCatalog.catalogCache = new CatalogCacheOFF();
        hiveCatalog.catalogLock = new CatalogLockOFF();
        return hiveCatalog;
    }

    public static PaimonSelectedTab createPaimonSelectedTab() {
        final String targetTableName = "customer_order_relation";
        PaimonSelectedTab tab = new PaimonSelectedTab();
        String keyCreateTime = "create_time";

        tab.bucketField = new PaimonBucketKeysOFF();
        tab.sequenceField = new com.qlangtech.tis.plugin.paimon.datax.sequence.PaimonSequenceFieldsOff();

        OnPaimonPartition pt = new OnPaimonPartition();
        pt.partitionPathFields = Lists.newArrayList(keyCreateTime);
        tab.partition = pt;
        tab.name = targetTableName;
        // List<IColMetaGetter> colMetas = Lists.newArrayList();

//                "customerregister_id",
//                "waitingorder_id",
//                "kind",
//                "create_time",
//                "last_ver"
        // DataType
        HdfsColMeta cmeta = null;
        // String colName, Boolean nullable, Boolean pk, DataType dataType
        tab.primaryKeys = Lists.newArrayList();
        cmeta = new HdfsColMeta("customerregister_id", false
                , true, DataType.createVarChar(150));
        tab.cols.add(TableMap.getCMeta(cmeta));
        tab.primaryKeys.add(cmeta.getName());

        cmeta = new HdfsColMeta("waitingorder_id", false, true
                , DataType.createVarChar(150));
        tab.cols.add(TableMap.getCMeta(cmeta));
        tab.primaryKeys.add(cmeta.getName());

        cmeta = new HdfsColMeta("kind"
                , true, false, DataType.getType(JDBCTypes.BIGINT));
        tab.cols.add(TableMap.getCMeta(cmeta));

        cmeta = new HdfsColMeta(keyCreateTime
                , true, false, DataType.getType(JDBCTypes.BIGINT));
        tab.cols.add(TableMap.getCMeta(cmeta));

        cmeta = new HdfsColMeta("last_ver"
                , true, false, DataType.getType(JDBCTypes.BIGINT));
        tab.cols.add(TableMap.getCMeta(cmeta));

        return tab;
    }
}

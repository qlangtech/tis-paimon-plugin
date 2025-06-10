package com.qlangtech.tis.plugin.paimon.datax.test;

import com.qlangtech.tis.plugin.paimon.catalog.HiveCatalog;
import com.qlangtech.tis.plugin.paimon.catalog.cache.CatalogCacheOFF;
import com.qlangtech.tis.plugin.paimon.catalog.cache.CatalogCacheON;
import com.qlangtech.tis.plugin.paimon.catalog.lock.CatalogLockOFF;
import com.qlangtech.tis.plugin.paimon.datax.DataxPaimonWriter;
import com.qlangtech.tis.plugin.paimon.datax.compact.PaimonCompaction;
import com.qlangtech.tis.plugin.paimon.datax.utils.PaimonSnapshot;
import com.qlangtech.tis.plugin.paimon.datax.writemode.BatchInsertWriteMode;
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
        BatchInsertWriteMode writeMode = new BatchInsertWriteMode();
        // writer.fsName = KEY_HDFS200;
        writer.paimonWriteMode = writeMode;
        writer.tableBucket = -1;
        writer.storeFormat = FILE_FORMAT_PARQUET;

        PaimonCompaction compaction = new PaimonCompaction();
        compaction.optimizationInterval = null;// CoreOptions.COMPACTION_OPTIMIZATION_INTERVAL.defaultValue();
        compaction.sizeAmplificationPercent = org.apache.paimon.CoreOptions.COMPACTION_MAX_SIZE_AMPLIFICATION_PERCENT.defaultValue();
        compaction.sizeRatio = CoreOptions.COMPACTION_SIZE_RATIO.defaultValue();
        compaction.minFileNum = CoreOptions.COMPACTION_MIN_FILE_NUM.defaultValue();
        writer.compaction = compaction;

        PaimonSnapshot snapshot = new PaimonSnapshot();
        snapshot.retainedMax = org.apache.paimon.CoreOptions.SNAPSHOT_NUM_RETAINED_MAX.defaultValue();
        snapshot.retainedMin = org.apache.paimon.CoreOptions.SNAPSHOT_NUM_RETAINED_MIN.defaultValue();
        snapshot.timeRetained = org.apache.paimon.CoreOptions.SNAPSHOT_TIME_RETAINED.defaultValue();
        writer.snapshot = snapshot;
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
}

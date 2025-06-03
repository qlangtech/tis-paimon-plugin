package com.qlangtech.tis.plugin.paimon.datax.test;

import com.qlangtech.tis.plugin.paimon.catalog.HiveCatalog;
import com.qlangtech.tis.plugin.paimon.datax.DataxPaimonWriter;
import com.qlangtech.tis.plugin.paimon.datax.writemode.BatchInsertWriteMode;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-05-26 10:09
 **/
public class PaimonTestUtils {
    public static final String KEY_HDFS200 = "hdfs200";

    public static DataxPaimonWriter getPaimonWriter() {
        //   DaMengDataSourceFactory dsFactory = TestDaMengDataSourceFactory.createDaMengDataSourceFactory();

        DataxPaimonWriter writer = new DataxPaimonWriter() {
            @Override
            public Class<?> getOwnerClass() {
                return DataxPaimonWriter.class;
            }
        };
        HiveCatalog hiveCatalog = createHiveCatalog();
        writer.catalog = hiveCatalog;
        BatchInsertWriteMode writeMode = new BatchInsertWriteMode();
        // writer.fsName = KEY_HDFS200;
        writer.paimonWriteMode = writeMode;
        writer.tableBucket = 1;
        //writer.autoCreateTable = AutoCreateTable.dft();

        return writer;
    }

    public static HiveCatalog createHiveCatalog() {
        HiveCatalog hiveCatalog = new HiveCatalog();
        hiveCatalog.dbName = "hive200_2";
        hiveCatalog.tableOwner = "hive";
        hiveCatalog.dbName = KEY_HDFS200;
        //hiveCatalog.catalogPath = "/user/hive/warehouse/paimon";
        return hiveCatalog;
    }
}

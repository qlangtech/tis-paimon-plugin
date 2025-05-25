package com.qlangtech.tis.plugin.paimon.catalog;

import com.qlangtech.tis.config.hive.IHiveConnGetter;
import com.qlangtech.tis.config.hive.meta.HiveTable;
import com.qlangtech.tis.config.hive.meta.IHiveMetaStore;
import com.qlangtech.tis.offline.FileSystemFactory;
import com.qlangtech.tis.plugin.common.PluginDesc;
import com.qlangtech.tis.plugin.paimon.datax.TestDataxPaimonWriterRealReal;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Database;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-05-18 14:46
 **/
public class TestHiveCatalog {
    public static HiveCatalog createHiveCatalog() {
        HiveCatalog hiveCatalog = new HiveCatalog();
        hiveCatalog.dbName = "hive200_2";
        hiveCatalog.tableOwner = "hive";
        hiveCatalog.catalogPath = "/user/hive/warehouse/paimon";
        return hiveCatalog;
    }

    @Test
    public void testDescGenerate() {
        PluginDesc.testDescGenerate(HiveCatalog.class, "paimon-hive-catalog-descriptor.json");
    }

    @Test
    public void testCreateCatalog() throws Exception {
        HiveCatalog hiveCatalog = createHiveCatalog();
        FileSystemFactory fsFactory = FileSystemFactory.getFsFactory(TestDataxPaimonWriterRealReal.KEY_HDFS200);
        Assert.assertNotNull(hiveCatalog);
        Catalog catalog = hiveCatalog.createCatalog(fsFactory);
        Assert.assertNotNull("catalog can not be null", catalog);

        //  catalog.createDatabase("paimon", true);

       // Database paimonDB = catalog.getDatabase("paimon");
       // Assert.assertNotNull(paimonDB);
        List<String> dbs = catalog.listDatabases();
        Assert.assertNotNull(dbs);


        hiveCatalog.getDataSourceFactory().visitAllConnection((conn) -> {
            try {

              //  conn.execute("create database paimon");
                conn.query("SELECT * FROM paimon.customer_order_relation", (result) -> {
                    System.out.println(result.getString(1));
                    return true;
                });
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        IHiveConnGetter hiveConn = (IHiveConnGetter) hiveCatalog.getDataSourceFactory();

        IHiveMetaStore metaStoreClient = hiveConn.createMetaStoreClient();
        List<HiveTable> tabs = metaStoreClient.getTables("default");
        Assert.assertNotNull(tabs);
    }

}

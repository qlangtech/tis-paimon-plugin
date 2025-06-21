package com.qlangtech.tis.plugin.paimon.catalog;

import com.qlangtech.tis.config.hive.IHiveConnGetter;
import com.qlangtech.tis.config.hive.meta.HiveTable;
import com.qlangtech.tis.config.hive.meta.IHiveMetaStore;
import com.qlangtech.tis.datax.StoreResourceTypeConstants;
import com.qlangtech.tis.manage.common.CenterResource;
import com.qlangtech.tis.offline.FileSystemFactory;
import com.qlangtech.tis.plugin.paimon.datax.test.PaimonTestUtils;
import org.apache.paimon.catalog.Catalog;
import org.junit.Assert;
import org.junit.Test;

import java.net.URL;
import java.util.Enumeration;
import java.util.List;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-05-26 14:19
 **/
public class TestHiveCatalog {


    @Test
    public void testUrl() throws Exception {
        short i = 1;
        System.out.println(String.valueOf((byte) i));


//        Enumeration<URL> resources = this.getClass().getClassLoader().getResources("org/apache/paimon/schema/Schema$Builder.class");
//        while (resources.hasMoreElements()) {
//            System.out.println(resources.nextElement());
//        }
    }

    @Test
    public void testCreateCatalog() throws Exception {


        HiveCatalog hiveCatalog = PaimonTestUtils.createHiveCatalog();
        //  FileSystemFactory fsFactory = FileSystemFactory.getFsFactory(PaimonTestUtils.KEY_HDFS200);
        Assert.assertNotNull(hiveCatalog);
        Catalog catalog = hiveCatalog.createCatalog(PaimonTestUtils.DATAX_PAIMON_NAME);
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

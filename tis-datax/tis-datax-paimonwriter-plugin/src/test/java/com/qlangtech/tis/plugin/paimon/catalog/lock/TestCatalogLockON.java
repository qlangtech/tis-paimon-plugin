package com.qlangtech.tis.plugin.paimon.catalog.lock;

import com.qlangtech.tis.plugin.common.PluginDesc;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-06-03 13:55
 **/
public class TestCatalogLockON {

    @Test
    public void testDescGenerate() {
        PluginDesc.testDescGenerate(CatalogLockON.class, "catalog-lock-on-descriptor.json");
    }

    @Test
    public void setOptions() {
        CatalogLockON catalogLockON = new CatalogLockON();
        catalogLockON.acquireTimeout = CatalogOptions.LOCK_ACQUIRE_TIMEOUT.defaultValue();
        catalogLockON.checkMaxSleep = CatalogOptions.LOCK_CHECK_MAX_SLEEP.defaultValue();
        catalogLockON.type = "hive";
        org.apache.paimon.options.Options options = new Options();
        catalogLockON.setOptions(options);

        Assert.assertEquals(CatalogOptions.LOCK_ACQUIRE_TIMEOUT.defaultValue(), options.get(CatalogOptions.LOCK_ACQUIRE_TIMEOUT));
        Assert.assertEquals(CatalogOptions.LOCK_CHECK_MAX_SLEEP.defaultValue(), options.get(CatalogOptions.LOCK_CHECK_MAX_SLEEP));

        Assert.assertEquals("hive", options.get(CatalogOptions.LOCK_TYPE));
    }
}
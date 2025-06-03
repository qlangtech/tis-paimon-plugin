package com.qlangtech.tis.plugin.paimon.catalog.lock;

import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.junit.Assert;
import org.junit.Test;

import java.time.Duration;

import static org.junit.Assert.*;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-06-03 13:55
 **/
public class TestCatalogLockON {

    @Test
    public void setOptions() {
        CatalogLockON catalogLockON = new CatalogLockON();
        catalogLockON.acquireTimeout = CatalogOptions.LOCK_ACQUIRE_TIMEOUT.defaultValue().toMinutes();
        catalogLockON.checkMaxSleep = CatalogOptions.LOCK_CHECK_MAX_SLEEP.defaultValue().toSeconds();
        catalogLockON.type = "hive";
        org.apache.paimon.options.Options options = new Options();
        catalogLockON.setOptions(options);

        Assert.assertEquals(8, options.get(CatalogOptions.LOCK_ACQUIRE_TIMEOUT).toMinutes());
        Assert.assertEquals(8, options.get(CatalogOptions.LOCK_CHECK_MAX_SLEEP).toSeconds());

        Assert.assertEquals("hive", options.get(CatalogOptions.LOCK_TYPE));
    }
}
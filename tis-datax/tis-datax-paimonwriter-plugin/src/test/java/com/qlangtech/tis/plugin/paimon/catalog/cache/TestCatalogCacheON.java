package com.qlangtech.tis.plugin.paimon.catalog.cache;

import com.qlangtech.tis.extension.impl.PropertyType;
import com.qlangtech.tis.extension.impl.RootFormProperties;
import com.qlangtech.tis.plugin.MemorySize;
import com.qlangtech.tis.plugin.paimon.catalog.lock.CatalogLockON;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.junit.Assert;
import org.junit.Test;
import com.qlangtech.tis.plugin.common.PluginDesc;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-06-04 08:08
 **/
public class TestCatalogCacheON {

    @Test
    public void testDescGenerate() {

        CatalogCacheON.Desc catalogCacheOnDesc = new CatalogCacheON.Desc();
        RootFormProperties fieldProps = (RootFormProperties) catalogCacheOnDesc.getPluginFormPropertyTypes();

        Assert.assertEquals(6, fieldProps.propertiesType.size());

        PropertyType manifestSmallFieldMemoryProp = fieldProps.propertiesType.get(CatalogCacheON.KEY_FIELD_MANIFEST_SMALL_FIELD_MEMORY);
        Assert.assertNotNull("field:" + CatalogCacheON.KEY_FIELD_MANIFEST_SMALL_FIELD_MEMORY + " can not be null"
                , manifestSmallFieldMemoryProp);
        // Assert.assertNotNull(manifestSmallFieldMemoryProp.getExtraProps());
        PluginDesc.testDescGenerate(CatalogCacheON.class, catalogCacheOnDesc, "catalog-cache-on-descriptor.json");
    }

    @Test
    public void setOptions() {
        CatalogCacheON catalogLockON = new CatalogCacheON();
        catalogLockON.expirationInterval = CatalogOptions.CACHE_EXPIRATION_INTERVAL_MS.defaultValue();

        org.apache.paimon.options.MemorySize expectManifestMaxMemory = org.apache.paimon.options.MemorySize.ofMebiBytes(999);
        // CatalogOptions.CACHE_MANIFEST_MAX_MEMORY
        catalogLockON.manifestMaxMemory = convert(expectManifestMaxMemory);
        catalogLockON.manifestSmallFileMemory = convert(CatalogOptions.CACHE_MANIFEST_SMALL_FILE_MEMORY.defaultValue());
        catalogLockON.manifestSmallFileThreshold = convert(CatalogOptions.CACHE_MANIFEST_SMALL_FILE_THRESHOLD.defaultValue());
        catalogLockON.partitionMaxNum = CatalogOptions.CACHE_PARTITION_MAX_NUM.defaultValue();
        catalogLockON.snapshotMaxNumPerTable = CatalogOptions.CACHE_SNAPSHOT_MAX_NUM_PER_TABLE.defaultValue();
        Options options = new Options();
        catalogLockON.setOptions(options);


        Assert.assertEquals(7, options.toMap().size());
        Assert.assertTrue(options.get(CatalogOptions.CACHE_ENABLED));

        Assert.assertEquals(CatalogOptions.CACHE_EXPIRATION_INTERVAL_MS.defaultValue(), options.get(CatalogOptions.CACHE_EXPIRATION_INTERVAL_MS));

        Assert.assertEquals(expectManifestMaxMemory, options.get(CatalogOptions.CACHE_MANIFEST_MAX_MEMORY));

        Assert.assertEquals(CatalogOptions.CACHE_MANIFEST_SMALL_FILE_MEMORY.defaultValue(), options.get(CatalogOptions.CACHE_MANIFEST_SMALL_FILE_MEMORY));

        Assert.assertEquals(CatalogOptions.CACHE_MANIFEST_SMALL_FILE_THRESHOLD.defaultValue(), options.get(CatalogOptions.CACHE_MANIFEST_SMALL_FILE_THRESHOLD));
        Assert.assertEquals(CatalogOptions.CACHE_PARTITION_MAX_NUM.defaultValue(), options.get(CatalogOptions.CACHE_PARTITION_MAX_NUM));

        Assert.assertEquals(CatalogOptions.CACHE_SNAPSHOT_MAX_NUM_PER_TABLE.defaultValue(), options.get(CatalogOptions.CACHE_SNAPSHOT_MAX_NUM_PER_TABLE));

    }

    private MemorySize convert(org.apache.paimon.options.MemorySize memorySize) {
        return MemorySize.ofBytes(memorySize.getBytes());
    }
}
package com.qlangtech.tis.plugin.paimon.catalog.cache;

import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.paimon.catalog.CatalogCache;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-06-03 14:42
 **/
public class CatalogCacheOFF extends CatalogCache {

    @Override
    public void setOptions(Options options) {
        options.set(CatalogOptions.CACHE_ENABLED, false);
    }

    @TISExtension
    public static class Desc extends Descriptor<CatalogCache> {
       // Options<CatalogLock, ConfigOption> opts;

        @Override
        public String getDisplayName() {
            return SWITCH_OFF;
        }

        public Desc() {
            super();
        }
    }
}

package com.qlangtech.tis.plugin.paimon.catalog.cache;

import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.extension.util.AbstractPropAssist.Options;
import com.qlangtech.tis.plugin.paimon.catalog.CatalogCache;
import com.qlangtech.tis.plugin.paimon.catalog.CatalogLock;
import org.apache.paimon.options.ConfigOption;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-06-03 14:42
 **/
public class CatalogCacheOFF extends CatalogCache {

    @TISExtension
    public static class Desc extends Descriptor<CatalogCache> {
       // Options<CatalogLock, ConfigOption> opts;

        @Override
        public String getDisplayName() {
            return SWITCH_ON;
        }

        public Desc() {
            super();
        }
    }
}

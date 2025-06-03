package com.qlangtech.tis.plugin.paimon.catalog.lock;

import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.extension.util.AbstractPropAssist.Options;
import com.qlangtech.tis.plugin.paimon.catalog.CatalogLock;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.ConfigOption;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-06-03 13:28
 **/
public class CatalogLockOFF extends CatalogLock {

    @Override
    public void setOptions(org.apache.paimon.options.Options options) {
        options.set(CatalogOptions.LOCK_ENABLED, false);
    }

    @TISExtension
    public static class Desc extends Descriptor<CatalogLock> {
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

package com.qlangtech.tis.plugin.paimon.catalog;

import com.qlangtech.tis.extension.Describable;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-06-03 14:39
 * @see com.qlangtech.tis.plugin.paimon.catalog.cache.CatalogCacheOFF
 * @see com.qlangtech.tis.plugin.paimon.catalog.cache.CatalogCacheON
 **/
public abstract class CatalogCache implements Describable<CatalogCache> {

    public abstract void setOptions(org.apache.paimon.options.Options options);
}

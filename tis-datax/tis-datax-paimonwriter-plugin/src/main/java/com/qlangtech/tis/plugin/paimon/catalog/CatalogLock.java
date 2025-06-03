package com.qlangtech.tis.plugin.paimon.catalog;

import com.qlangtech.tis.extension.Describable;
import org.apache.paimon.options.Options;

/**
 * Enable Catalog Lock.
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-06-03 12:43
 **/
public abstract class CatalogLock implements Describable<CatalogLock> {


    public abstract void setOptions(Options options);
}

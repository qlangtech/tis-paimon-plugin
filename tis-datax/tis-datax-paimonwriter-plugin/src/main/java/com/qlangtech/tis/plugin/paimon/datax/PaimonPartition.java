package com.qlangtech.tis.plugin.paimon.datax;

import com.qlangtech.tis.extension.Describable;

import java.util.List;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-06-12 10:04
 * @see com.qlangtech.tis.plugin.paimon.datax.pt.OffPaimonPartition
 * @see com.qlangtech.tis.plugin.paimon.datax.pt.OnPaimonPartition
 **/
public abstract class PaimonPartition implements Describable<PaimonPartition> ,SchemaBuilderSetter {

    abstract public List<String> getPartitionKeys();
}

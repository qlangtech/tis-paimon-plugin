package com.qlangtech.tis.plugin.paimon.datax.bucket;

import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.plugin.paimon.datax.PaimonPropAssist;
import com.qlangtech.tis.plugin.paimon.datax.PaimonPropAssist.PaimonOptions;
import com.qlangtech.tis.plugin.paimon.datax.PaimonSelectedTab;
import com.qlangtech.tis.plugin.paimon.datax.SchemaBuilderSetter;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.schema.Schema.Builder;
import org.apache.paimon.table.DataTable;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.TableWrite;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-06-25 14:43
 **/
public abstract class PaimonBucket implements Describable<PaimonBucket>, SchemaBuilderSetter {

    @Override
    public final void initializeSchemaBuilder(Builder schemaBuilder, PaimonSelectedTab tab) {
        schemaBuilder.option(CoreOptions.BUCKET.key(), String.valueOf(getBucketCount()));
        BasicDesc desc = (BasicDesc) this.getDescriptor();
        desc.opts.setTarget((field, val) -> {
            schemaBuilder.option(field.key(), String.valueOf(val));
        }, this);
    }

    protected abstract int getBucketCount();


    @Override
    public Descriptor<PaimonBucket> getDescriptor() {
        Descriptor<PaimonBucket> desc = Describable.super.getDescriptor();
        if (!(desc instanceof BasicDesc)) {
            throw new IllegalStateException("desc must be inherit from " + BasicDesc.class);
        }
        return desc;
    }

    /**
     * 批量同步时计算 对应的bucket值，实时同步是不使用此方法，使用的是org.apache.flink.cdc.connectors.paimon.sink.v2.bucket.BucketAssignOperator#processElement() 方法中的实现
     *
     * @param write
     * @param row
     * @return
     */
    public int getBucket(DataTable table, TableWrite write, GenericRow row) {
        return write.getBucket(row);
    }

    protected static class BasicDesc extends Descriptor<PaimonBucket> {
        protected final PaimonOptions<PaimonBucket> opts;

        public BasicDesc() {
            super();
            this.opts = PaimonPropAssist.createOpts(this);
        }
    }
}

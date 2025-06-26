package com.qlangtech.tis.plugin.paimon.datax.bucket.impl;

import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.paimon.datax.PaimonPropAssist;
import com.qlangtech.tis.plugin.paimon.datax.PaimonPropAssist.PaimonOptions;
import com.qlangtech.tis.plugin.paimon.datax.PaimonSelectedTab;
import com.qlangtech.tis.plugin.paimon.datax.bucket.PaimonBucketKeys;
import org.apache.paimon.schema.Schema.Builder;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-06-25 15:31
 **/
public class PaimonBucketKeysOFF extends PaimonBucketKeys {
    @Override
    public void initializeSchemaBuilder(Builder schemaBuilder, PaimonSelectedTab tab) {

    }

    @TISExtension
    public static class Desc extends Descriptor<PaimonBucketKeys> {
        private final PaimonOptions<PaimonBucketKeys> opts;

        public Desc() {
            super();
            this.opts = PaimonPropAssist.createOpts(this);
        }

        @Override
        public String getDisplayName() {
            return SWITCH_OFF;
        }
    }
}

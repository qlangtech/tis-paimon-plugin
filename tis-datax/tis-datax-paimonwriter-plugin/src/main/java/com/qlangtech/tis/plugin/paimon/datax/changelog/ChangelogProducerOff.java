package com.qlangtech.tis.plugin.paimon.datax.changelog;

import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.paimon.datax.ChangelogProducer;
import com.qlangtech.tis.plugin.paimon.datax.PaimonSelectedTab;
import org.apache.paimon.schema.Schema.Builder;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-06-21 09:59
 **/
public class ChangelogProducerOff extends com.qlangtech.tis.plugin.paimon.datax.ChangelogProducer {

    @Override
    public void initializeSchemaBuilder(Builder schemaBuilder, PaimonSelectedTab tab) {

    }

    @TISExtension
    public static class Desc extends Descriptor<ChangelogProducer> {
        @Override
        public String getDisplayName() {
            return SWITCH_OFF;
        }
    }
}

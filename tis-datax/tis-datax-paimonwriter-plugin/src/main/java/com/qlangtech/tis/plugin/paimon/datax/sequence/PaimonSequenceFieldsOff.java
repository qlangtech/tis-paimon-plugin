package com.qlangtech.tis.plugin.paimon.datax.sequence;

import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.paimon.datax.PaimonSelectedTab;
import com.qlangtech.tis.plugin.paimon.datax.PaimonSequenceFields;
import org.apache.paimon.schema.Schema.Builder;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-06-21 09:37
 **/
public class PaimonSequenceFieldsOff extends PaimonSequenceFields {
    @Override
    public void initializeSchemaBuilder(Builder schemaBuilder, PaimonSelectedTab tab) {

    }

    @TISExtension
    public static class Desc extends Descriptor<PaimonSequenceFields> {
        @Override
        public String getDisplayName() {
            return SWITCH_OFF;
        }
    }

}

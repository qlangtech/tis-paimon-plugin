package com.qlangtech.tis.plugin.paimon.datax.bucket.impl;

import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.paimon.datax.PaimonPropAssist;
import com.qlangtech.tis.plugin.paimon.datax.PaimonPropAssist.PaimonOptions;
import com.qlangtech.tis.plugin.paimon.datax.PaimonSelectedTab;
import com.qlangtech.tis.plugin.paimon.datax.bucket.PaimonBucketKeys;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.schema.Schema.Builder;

import java.util.List;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-06-25 15:31
 * @see com.qlangtech.tis.plugin.paimon.datax.sequence.PaimonSequenceFieldsOn
 * @see com.qlangtech.tis.plugin.paimon.datax.PaimonSelectedTab
 **/
public class PaimonBucketKeysON extends PaimonBucketKeys {

    public static final String FIELD_FIELD = "field";

    @FormField(ordinal = 1, type = FormFieldType.ENUM, validate = {Validator.require})
    public List<String> field;

    @Override
    public void initializeSchemaBuilder(Builder schemaBuilder, PaimonSelectedTab tab) {
        Desc descriptor = (Desc) this.getDescriptor();
        descriptor.opts.setTarget((f, val) -> {
            schemaBuilder.option(f.key(), (val instanceof List) ? String.join(",", (List) val) : String.valueOf(val));
        }, this);
    }

    @TISExtension
    public static class Desc extends Descriptor<PaimonBucketKeys> {
        private final PaimonOptions<PaimonBucketKeys> opts;

        public Desc() {
            super();
            this.opts = PaimonPropAssist.createOpts(this);
            this.opts.add(FIELD_FIELD, CoreOptions.BUCKET_KEY);
        }

        @Override
        public String getDisplayName() {
          return SWITCH_ON;
        }
    }

}

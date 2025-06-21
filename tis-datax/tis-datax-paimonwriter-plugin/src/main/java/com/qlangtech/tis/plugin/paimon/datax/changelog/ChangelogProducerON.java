package com.qlangtech.tis.plugin.paimon.datax.changelog;

import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.extension.util.OverwriteProps;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.paimon.datax.PaimonPropAssist;
import com.qlangtech.tis.plugin.paimon.datax.PaimonPropAssist.PaimonOptions;
import com.qlangtech.tis.plugin.paimon.datax.PaimonSelectedTab;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.schema.Schema.Builder;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-06-21 09:58
 **/
public class ChangelogProducerON extends com.qlangtech.tis.plugin.paimon.datax.ChangelogProducer {

    @FormField(ordinal = 1, type = FormFieldType.ENUM, validate = {Validator.require})
    public String mode;

    @FormField(ordinal = 2, type = FormFieldType.ENUM, validate = {Validator.require})
    public String rowDeduplicate;

    @FormField(ordinal = 3, type = FormFieldType.ENUM, validate = {Validator.require})
    public String deduplicateIgnoreFields;

    @Override
    public void initializeSchemaBuilder(Builder schemaBuilder, PaimonSelectedTab tab) {
        Desc desc = (Desc) this.getDescriptor();
        desc.opts.setTarget((field, val) -> {
            schemaBuilder.option(field.key(), String.valueOf(val));
        }, this);
    }

    @TISExtension
    public static class Desc extends Descriptor<com.qlangtech.tis.plugin.paimon.datax.ChangelogProducer> {
        public PaimonOptions<com.qlangtech.tis.plugin.paimon.datax.ChangelogProducer> opts;

        @Override
        public String getDisplayName() {
            return SWITCH_ON;
        }

        public Desc() {
            super();

            this.opts = PaimonPropAssist.createOpts(this);
            OverwriteProps labelOverWrite = new OverwriteProps();
            labelOverWrite.setLabelRewrite((label) -> {
                if (CoreOptions.CHANGELOG_PRODUCER.key().equals(label)) {
                    return "producer";
                } else if (CoreOptions.CHANGELOG_PRODUCER_ROW_DEDUPLICATE.key().equals(label)) {
                    return "row-deduplicate";
                } else if (CoreOptions.CHANGELOG_PRODUCER_ROW_DEDUPLICATE_IGNORE_FIELDS.key().equals(label)) {
                    return "deduplicate-ignore-fields";
                } else {
                    throw new IllegalStateException("lable:" + label);
                }
            });

            opts.add("mode", CoreOptions.CHANGELOG_PRODUCER, labelOverWrite);
            opts.add("rowDeduplicate", CoreOptions.CHANGELOG_PRODUCER_ROW_DEDUPLICATE, labelOverWrite);
            opts.add("deduplicateIgnoreFields", CoreOptions.CHANGELOG_PRODUCER_ROW_DEDUPLICATE_IGNORE_FIELDS, labelOverWrite);

        }
    }

}

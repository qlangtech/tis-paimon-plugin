package com.qlangtech.tis.plugin.paimon.datax.utils;

import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.extension.util.AbstractPropAssist.Options;
import com.qlangtech.tis.extension.util.AbstractPropAssist.TISAssistProp;
import com.qlangtech.tis.extension.util.OverwriteProps;
import com.qlangtech.tis.extension.util.PropValFilter;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.paimon.datax.PaimonPropAssist;
import com.qlangtech.tis.plugin.paimon.datax.PaimonSelectedTab;
import com.qlangtech.tis.plugin.paimon.datax.SchemaBuilderSetter;
import org.apache.commons.lang.StringUtils;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.schema.Schema.Builder;
import org.apache.paimon.utils.TimeUtils;

import java.time.Duration;
import java.util.Objects;
import java.util.function.Function;

/**
 * <pre>
 *     参数	说明	默认值
 * snapshot.time-retained	快照保留时间（如 2h 表示保留最近2小时内的快照）	1h
 * snapshot.num-retained.min	最小保留快照数量（即使超过时间保留，至少保留该数量的快照）	5
 * snapshot.num-retained.max	最大保留快照数量（防止存储爆炸）	2147483647
 * </pre>
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-05-15 15:49
 **/
public class PaimonSnapshot implements Describable<PaimonSnapshot>, SchemaBuilderSetter {
    /**
     * @see org.apache.paimon.CoreOptions#SNAPSHOT_NUM_RETAINED_MIN
     */
    @FormField(ordinal = 1, type = FormFieldType.INT_NUMBER, validate = {Validator.require, Validator.integer})
    public Integer retainedMin;

    /**
     * @see org.apache.paimon.CoreOptions#SNAPSHOT_NUM_RETAINED_MAX
     */
    @FormField(ordinal = 2, type = FormFieldType.INT_NUMBER, validate = {Validator.require, Validator.integer})
    public Integer retainedMax;

    /**
     * @see org.apache.paimon.CoreOptions#SNAPSHOT_TIME_RETAINED
     */
    @FormField(ordinal = 3, type = FormFieldType.DURATION_OF_HOUR, validate = {Validator.require, Validator.integer})
    public Duration timeRetained;

    @Override
    public void initializeSchemaBuilder(Builder schemaBuilder, PaimonSelectedTab tab) {
        DefaultDescriptor desc = (DefaultDescriptor) this.getDescriptor();
        Objects.requireNonNull(desc, "desc can not be null").opts.setTarget((field, val) -> {
            schemaBuilder.option(field.key(), String.valueOf(val));
        }, this);
    }

    @TISExtension
    public static final class DefaultDescriptor extends Descriptor<PaimonSnapshot> {
        private Options<PaimonSnapshot, ConfigOption> opts;

        public DefaultDescriptor() {
            super();
            this.opts = PaimonPropAssist.createOpts(this);
            Function<String, String> labelRewriter = (label) -> StringUtils.substringAfter(label, "snapshot.");
            OverwriteProps overwriteProps = new OverwriteProps();
            overwriteProps.setLabelRewrite(labelRewriter);


            opts.add("retainedMin", CoreOptions.SNAPSHOT_NUM_RETAINED_MIN, overwriteProps);
            opts.add("retainedMax", CoreOptions.SNAPSHOT_NUM_RETAINED_MAX, overwriteProps);

            OverwriteProps labelProp = OverwriteProps.withAppendHelper("unit：hour").setLabelRewrite(labelRewriter);
//            labelProp.dftValConvert = (val) -> {
//                Duration dur = (Duration) val;
//                return dur.toHours();
//            };
            TISAssistProp<ConfigOption> snapshotAssistProp
                    = TISAssistProp.create(CoreOptions.SNAPSHOT_TIME_RETAINED);
            snapshotAssistProp.setOverwriteProp(labelProp);
            opts.add("timeRetained", snapshotAssistProp);
        }

        @Override
        public String getDisplayName() {
            return SWITCH_DEFAULT;
        }
    }

}

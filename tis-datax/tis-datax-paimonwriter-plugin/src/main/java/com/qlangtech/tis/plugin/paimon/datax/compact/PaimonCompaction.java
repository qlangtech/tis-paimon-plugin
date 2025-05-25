package com.qlangtech.tis.plugin.paimon.datax.compact;

import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.extension.util.AbstractPropAssist.Options;
import com.qlangtech.tis.extension.util.AbstractPropAssist.TISAssistProp;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.paimon.datax.PaimonPropAssist;
import com.qlangtech.tis.plugin.paimon.datax.SchemaBuilderSetter;
import org.apache.commons.lang.StringUtils;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.schema.Schema;

import java.util.function.Function;

/**
 * <pre>
 * compaction.min.file-num	触发合并的最小文件数量（小文件积累到该值时触发合并）	3
 * compaction.max.file-num	合并时一次性处理的最大文件数量	6
 * compaction.size-amplification-threshold	文件大小放大阈值（触发合并的条件，如总大小/最大文件大小 > 阈值）	1.5
 * compaction.async.enabled	是否启用异步合并（流式写入时建议开启）	false
 * compaction.early-max.file-num	提前合并的阈值（避免写入延迟过高）
 * </pre>
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-05-15 15:46
 * @see org.apache.paimon.CoreOptions#COMPACTION_MIN_FILE_NUM
 **/
public class PaimonCompaction implements Describable<PaimonCompaction>, SchemaBuilderSetter {

    @FormField(ordinal = 1, type = FormFieldType.INT_NUMBER, validate = {Validator.require, Validator.integer})
    public Integer minFileNum;

//    @FormField(ordinal = 2, type = FormFieldType.INT_NUMBER, validate = {Validator.require, Validator.integer})
//    public Integer maxFileNum;

    /**
     * max-size-amplification-percent
     *
     * @see org.apache.paimon.CoreOptions#COMPACTION_MAX_SIZE_AMPLIFICATION_PERCENT
     */
    @FormField(ordinal = 3, type = FormFieldType.INT_NUMBER, validate = {Validator.require, Validator.integer})
    public Integer sizeAmplificationPercent;

    @FormField(ordinal = 4, type = FormFieldType.INT_NUMBER, validate = {Validator.require, Validator.integer})
    public Integer sizeRatio;
    // optimization-interval
    /**
     * compaction 间隔时间，单位：秒
     */
    @FormField(ordinal = 6, type = FormFieldType.INT_NUMBER, validate = {Validator.require, Validator.integer})
    public Integer optimizationInterval;

    @Override
    public void initializeSchemaBuilder(Schema.Builder schemaBuilder) {
        DefaultDescriptor desc = (DefaultDescriptor) this.getDescriptor();
        desc.opts.setTarget((field, val) -> {
            schemaBuilder.option(field.key(), String.valueOf(val));
        }, this);
    }



    @TISExtension
    public static final class DefaultDescriptor extends Descriptor<PaimonCompaction> {
        Options<PaimonCompaction, ConfigOption> opts;

        public DefaultDescriptor() {
            super();
//            OverwriteProps overwriteProps = new OverwriteProps();
//            overwriteProps.setLabelRewrite();

            Function<String, String> dftLableRewrite = (label) -> {
                return StringUtils.substringAfter(label, "compaction.");
            };
            opts = PaimonPropAssist.createOpts(this);
            opts.add("minFileNum", TISAssistProp.create(CoreOptions.COMPACTION_MIN_FILE_NUM).overwriteLabel(dftLableRewrite));
           // opts.add("maxFileNum", TISAssistProp.create(CoreOptions.COMPACTION_MAX_FILE_NUM).overwriteLabel(dftLableRewrite));

            opts.add("sizeAmplificationPercent", TISAssistProp.create(CoreOptions.COMPACTION_MAX_SIZE_AMPLIFICATION_PERCENT).overwriteLabel("放大比例"));
            opts.add("sizeRatio", TISAssistProp.create(CoreOptions.COMPACTION_SIZE_RATIO).overwriteLabel(dftLableRewrite));
            opts.add("optimizationInterval", TISAssistProp.create(CoreOptions.COMPACTION_OPTIMIZATION_INTERVAL).overwriteLabel(dftLableRewrite));
        }

        @Override
        public String getDisplayName() {
            return SWITCH_DEFAULT;
        }
    }
}

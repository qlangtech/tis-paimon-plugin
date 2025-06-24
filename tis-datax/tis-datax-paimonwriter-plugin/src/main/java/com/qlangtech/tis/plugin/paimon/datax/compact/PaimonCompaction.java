package com.qlangtech.tis.plugin.paimon.datax.compact;

import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.extension.util.AbstractPropAssist.Options;
import com.qlangtech.tis.extension.util.AbstractPropAssist.TISAssistProp;
import com.qlangtech.tis.extension.util.OverwriteProps;
import com.qlangtech.tis.extension.util.PropValFilter;
import com.qlangtech.tis.plugin.MemorySize;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.paimon.catalog.cache.CatalogCacheON;
import com.qlangtech.tis.plugin.paimon.datax.PaimonPropAssist;
import com.qlangtech.tis.plugin.paimon.datax.PaimonPropAssist.PaimonOptions;
import com.qlangtech.tis.plugin.paimon.datax.PaimonSelectedTab;
import com.qlangtech.tis.plugin.paimon.datax.SchemaBuilderSetter;
import org.apache.commons.lang.StringUtils;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.schema.Schema;

import java.time.Duration;
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




    // WRITE_ONLY
    @FormField(ordinal = 70, advance = true, type = FormFieldType.ENUM, validate = {Validator.require})
    public Boolean writeOnly;

    @FormField(ordinal = 20, type = FormFieldType.INT_NUMBER, validate = {Validator.require, Validator.integer})
    public Integer minFileNum;


//    @FormField(ordinal = 2, type = FormFieldType.INT_NUMBER, validate = {Validator.require, Validator.integer})
//    public Integer maxFileNum;

    /**
     * max-size-amplification-percent
     *
     * @see org.apache.paimon.CoreOptions#COMPACTION_MAX_SIZE_AMPLIFICATION_PERCENT
     */
    @FormField(ordinal = 30, type = FormFieldType.INT_NUMBER, validate = {Validator.require, Validator.integer})
    public Integer sizeAmplificationPercent;

    @FormField(ordinal = 40, type = FormFieldType.INT_NUMBER, validate = {Validator.require, Validator.integer})
    public Integer sizeRatio;
    // optimization-interval
    /**
     * compaction 间隔时间，单位：秒
     */
    @FormField(ordinal = 60, type = FormFieldType.DURATION_OF_MINUTE, validate = {Validator.integer})
    public Duration optimizationInterval;

    @Override
    public void initializeSchemaBuilder(Schema.Builder schemaBuilder, PaimonSelectedTab tab) {
        DefaultDescriptor desc = (DefaultDescriptor) this.getDescriptor();
        desc.opts.setTarget((field, val) -> {
            schemaBuilder.option(field.key(), String.valueOf(val));
        }, this);
    }


    @TISExtension
    public static final class DefaultDescriptor extends Descriptor<PaimonCompaction> {
        PaimonOptions<PaimonCompaction> opts;

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

            opts.add("writeOnly", TISAssistProp.create(CoreOptions.WRITE_ONLY).overwriteLabel(dftLableRewrite));
            opts.add("sizeAmplificationPercent", TISAssistProp.create(CoreOptions.COMPACTION_MAX_SIZE_AMPLIFICATION_PERCENT).overwriteLabel("放大比例"));
            opts.add("sizeRatio", TISAssistProp.create(CoreOptions.COMPACTION_SIZE_RATIO).overwriteLabel(dftLableRewrite));
            OverwriteProps optimizationIntervalOverwrite = new OverwriteProps();
            optimizationIntervalOverwrite.labelRewrite = dftLableRewrite;
            optimizationIntervalOverwrite.setAppendHelper("compaction.optimization-interval 的核心是平衡后台优化开销和数据查询/管理效率。1 分钟是安全的默认起点。 最佳值需要通过仔细监控你的特定工作负载在特定环境下的表现（资源使用、文件状态、查询延迟、写入稳定性）来确定。优先解决已观察到的瓶颈（资源争用或小文件堆积），然后进行有针对性的调整。记住，它需要与 compaction.max.file-num 和 compaction.early.max.file-num 等参数协同工作。");
            optimizationIntervalOverwrite.setDftVal(Duration.ofMinutes(2));
            opts.add("optimizationInterval", TISAssistProp.create(CoreOptions.COMPACTION_OPTIMIZATION_INTERVAL).setOverwriteProp(optimizationIntervalOverwrite));

//            OverwriteProps memoryOverwrite = new OverwriteProps();
//            memoryOverwrite.dftValConvert = (val) -> {
//                return MemorySize.ofBytes(((org.apache.paimon.options.MemorySize) val).getBytes());
//            };



        }

        @Override
        public String getDisplayName() {
            return SWITCH_DEFAULT;
        }
    }
}

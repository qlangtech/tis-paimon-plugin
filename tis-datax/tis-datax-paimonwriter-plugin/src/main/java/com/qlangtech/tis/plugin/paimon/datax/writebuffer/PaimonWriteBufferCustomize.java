package com.qlangtech.tis.plugin.paimon.datax.writebuffer;

import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.extension.util.AbstractPropAssist.TISAssistProp;
import com.qlangtech.tis.extension.util.OverwriteProps;
import com.qlangtech.tis.plugin.MemorySize;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.paimon.datax.PaimonPropAssist;
import com.qlangtech.tis.plugin.paimon.datax.PaimonPropAssist.PaimonOptions;
import com.qlangtech.tis.plugin.paimon.datax.PaimonSelectedTab;
import org.apache.commons.lang.StringUtils;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.schema.Schema.Builder;

import java.util.function.Consumer;
import java.util.function.Function;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-06-24 17:12
 **/
public class PaimonWriteBufferCustomize extends PaimonWriteBuffer {

    @FormField(ordinal = 0, type = FormFieldType.MEMORY_SIZE_OF_MEGA, validate = {Validator.require})
    public MemorySize writeBufferSize;

    @FormField(ordinal = 2, type = FormFieldType.ENUM, validate = {})
    public Boolean writeBufferSpillable;

    @FormField(ordinal = 3, type = FormFieldType.MEMORY_SIZE_OF_MEGA, validate = {})
    public MemorySize writeBufferMaxDiskSize;

    @FormField(ordinal = 4, type = FormFieldType.MEMORY_SIZE_OF_MEGA, validate = {})
    public MemorySize targetFileSize;

    @Override
    public void initializeSchemaBuilder(Builder schemaBuilder, PaimonSelectedTab tab) {
        DefaultDescriptor descriptor = (DefaultDescriptor) this.getDescriptor();
        PaimonOptions<PaimonWriteBuffer> opts = descriptor.opts;
        opts.setTarget((field, val) -> {
            schemaBuilder.option(field.key(), String.valueOf(val));
        }, this);
    }

    @TISExtension
    public static final class DefaultDescriptor extends Descriptor<PaimonWriteBuffer> {
        private final PaimonOptions<PaimonWriteBuffer> opts;

        public DefaultDescriptor() {
            super();
            opts = PaimonPropAssist.createOpts(this);

            // write-buffer-

            Function<String, String> labelOverwrite = (label) -> StringUtils.removeStart(label, "write-buffer-");

            Consumer<OverwriteProps> memoryOverwriteConsumer = (overwrite) -> {
                overwrite.setLabelRewrite(labelOverwrite);
            };

            opts.addMemorySize("writeBufferSize", CoreOptions.WRITE_BUFFER_SIZE, memoryOverwriteConsumer);
            opts.addMemorySize("targetFileSize", CoreOptions.TARGET_FILE_SIZE, memoryOverwriteConsumer);

            opts.add("writeBufferSpillable", TISAssistProp.create(CoreOptions.WRITE_BUFFER_SPILLABLE).overwriteLabel(labelOverwrite));
            opts.addMemorySize("writeBufferMaxDiskSize"
                    , CoreOptions.WRITE_BUFFER_MAX_DISK_SIZE
                    , memoryOverwriteConsumer.andThen((o) -> {
                        o.setDftVal(null);
                    }));
        }

        @Override
        public String getDisplayName() {
            return SWITCH_CUSTOMIZE;
        }
    }
}

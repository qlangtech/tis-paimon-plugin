package com.qlangtech.tis.plugin.paimon.datax.writebuffer;

import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.paimon.datax.PaimonSelectedTab;
import org.apache.paimon.schema.Schema.Builder;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-06-24 17:12
 **/
public class PaimonWriteBufferDefault extends PaimonWriteBuffer {
    @Override
    public void initializeSchemaBuilder(Builder schemaBuilder, PaimonSelectedTab tab) {

    }

    @TISExtension
    public static final class DefaultDescriptor extends Descriptor<PaimonWriteBuffer> {
        public DefaultDescriptor() {
            super();
        }

        @Override
        public String getDisplayName() {
            return SWITCH_DEFAULT;
        }
    }
}

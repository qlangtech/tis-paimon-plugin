package com.qlangtech.tis.plugin.paimon.datax.utils;

import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-05-15 15:44
 *
 * @see org.apache.paimon.CoreOptions#FILE_FORMAT
 **/
public class PaimonConfig implements Describable<PaimonConfig> {

    @TISExtension
    public static final class DefaultDescriptor extends Descriptor<PaimonConfig> {
        public DefaultDescriptor() {
            super();
        }
    }
}

package com.qlangtech.tis.plugin.paimon.datax.pt;

import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.paimon.datax.PaimonPartition;

import java.util.Collections;
import java.util.List;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-06-12 10:05
 **/
public class OffPaimonPartition extends PaimonPartition {
    @Override
    public final List<String> getPartitionKeys() {
        return Collections.emptyList();
    }

    @TISExtension
    public static final class Desc extends Descriptor<PaimonPartition> {
        public Desc() {
            super();
        }

        @Override
        public String getDisplayName() {
            return SWITCH_OFF;
        }
    }
}

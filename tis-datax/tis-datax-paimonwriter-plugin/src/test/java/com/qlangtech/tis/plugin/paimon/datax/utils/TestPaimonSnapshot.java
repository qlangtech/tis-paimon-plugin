package com.qlangtech.tis.plugin.paimon.datax.utils;

import com.qlangtech.tis.plugin.common.PluginDesc;
import org.junit.Test;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-06-02 18:35
 **/
public class TestPaimonSnapshot {

    @Test
    public void testDescGenerate() throws Exception {
        PluginDesc.testDescGenerate(PaimonSnapshot.class, "paimon-snapshot-descriptor.json");
    }
}
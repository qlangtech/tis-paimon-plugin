package com.qlangtech.tis.plugin.paimon.datax.changelog;

import com.qlangtech.tis.plugin.common.PluginDesc;
import org.junit.Test;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-06-21 14:10
 **/
public class ChangelogProducerONTest {


    @Test
    public void testDescGenerate() {
        PluginDesc.testDescGenerate(ChangelogProducerON.class, "changelog-on-descriptor.json");
    }

}
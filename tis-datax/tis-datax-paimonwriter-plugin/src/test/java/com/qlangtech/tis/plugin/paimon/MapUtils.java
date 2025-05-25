package com.qlangtech.tis.plugin.paimon;

import org.junit.Assert;

import java.util.Map;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-05-18 19:03
 **/
public class MapUtils {
    public static boolean assertMapEquals(Map<String, String> expectOptions, Map<String, String> actualOptions) {
        for (Map.Entry<String, String> entry : expectOptions.entrySet()) {
            Assert.assertEquals("key of " + entry.getKey(), entry.getValue(), actualOptions.get(entry.getKey()));
        }
        return true;
    }
}

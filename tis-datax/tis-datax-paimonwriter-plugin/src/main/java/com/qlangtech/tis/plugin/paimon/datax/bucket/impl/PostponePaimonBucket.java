package com.qlangtech.tis.plugin.paimon.datax.bucket.impl;

import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.paimon.datax.bucket.PaimonBucket;

/**
 * 延迟桶模式,写入时不立即分桶，先存临时文件，Compaction 时才分桶
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-06-25 14:49
 **/
public class PostponePaimonBucket extends PaimonBucket {
    @Override
    public int getBucketCount() {
        return -2;
    }

    @TISExtension
    public static class Desc extends BasicDesc {
        public Desc() {
            super();
        }

        @Override
        public String getDisplayName() {
            return "Postpone";
        }
    }
}

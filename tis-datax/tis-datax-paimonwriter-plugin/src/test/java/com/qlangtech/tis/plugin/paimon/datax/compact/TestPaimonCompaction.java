package com.qlangtech.tis.plugin.paimon.datax.compact;

import com.google.common.collect.Maps;
import com.qlangtech.tis.plugin.paimon.MapUtils;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.Schema.Builder;
import org.junit.Assert;
import org.junit.Test;

import java.time.Duration;
import java.util.Map;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-05-18 18:24
 **/
public class TestPaimonCompaction {

    @Test
    public void initializeSchemaBuilder() {
        PaimonCompaction paimonCompaction = new PaimonCompaction();
        paimonCompaction.minFileNum = 1;
        // paimonCompaction.maxFileNum = 2;
        paimonCompaction.optimizationInterval = Duration.ofSeconds(60);
        paimonCompaction.sizeRatio = 1;
        paimonCompaction.sizeAmplificationPercent = 200;

        Schema.Builder schemaBuilder = new Builder();
        paimonCompaction.initializeSchemaBuilder(schemaBuilder);
        Schema schema = schemaBuilder.build();
        Assert.assertNotNull(schema);
        Map<String, String> options = schema.options();

        Map<String, String> expectOptions = Maps.newHashMap();
        expectOptions.put(CoreOptions.COMPACTION_MIN_FILE_NUM.key(), String.valueOf(paimonCompaction.minFileNum));
        // expectOptions.put(CoreOptions.COMPACTION_MAX_FILE_NUM.key(), String.valueOf(paimonCompaction.maxFileNum));
        expectOptions.put(CoreOptions.COMPACTION_OPTIMIZATION_INTERVAL.key(), String.valueOf(paimonCompaction.optimizationInterval));
        expectOptions.put(CoreOptions.COMPACTION_SIZE_RATIO.key(), String.valueOf(paimonCompaction.sizeRatio));
        expectOptions.put(CoreOptions.COMPACTION_MAX_SIZE_AMPLIFICATION_PERCENT.key(), String.valueOf(paimonCompaction.sizeAmplificationPercent));

        MapUtils.assertMapEquals(expectOptions, options);
    }

}
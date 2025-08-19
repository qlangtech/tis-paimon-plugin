package com.qlangtech.tis.plugin.paimon.datax.compact;

import com.google.common.collect.Maps;
import com.qlangtech.tis.extension.IPropertyType;
import com.qlangtech.tis.extension.impl.PropertyType;
import com.qlangtech.tis.plugin.common.PluginDesc;
import com.qlangtech.tis.plugin.paimon.MapUtils;
import com.qlangtech.tis.plugin.paimon.datax.PaimonSelectedTab;
import com.qlangtech.tis.plugin.paimon.datax.compact.PaimonCompaction.DefaultDescriptor;
import com.qlangtech.tis.plugin.paimon.datax.test.PaimonTestUtils;
import com.qlangtech.tis.plugin.paimon.datax.utils.PaimonSnapshot;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.Schema.Builder;
import org.apache.paimon.utils.TimeUtils;
import org.junit.Assert;
import org.junit.Test;

import java.time.Duration;
import java.util.Map;

import static com.qlangtech.tis.plugin.paimon.datax.compact.PaimonCompaction.FIELD_NUM_STORED_RUNS_TRIGGER;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-05-18 18:24
 **/
public class TestPaimonCompaction {

    @Test
    public void testDescGenerate() throws Exception {
        PluginDesc.testDescGenerate(PaimonCompaction.class, "paimon-compaction-descriptor.json");
    }

    @Test
    public void testNummStoredRunsTrigger() {
        DefaultDescriptor cpmpactDescriptor = new DefaultDescriptor();
        IPropertyType propertyType = cpmpactDescriptor.getPropertyType(FIELD_NUM_STORED_RUNS_TRIGGER);
        Assert.assertNotNull(FIELD_NUM_STORED_RUNS_TRIGGER, propertyType);
        Assert.assertTrue(propertyType instanceof PropertyType);
        PropertyType pt = (PropertyType) propertyType;
        Assert.assertTrue(pt.extraProp.isAsynHelp());

        Assert.assertEquals("The number of stored runs trigger", pt.extraProp.getAsynHelp());
    }

    @Test
    public void initializeSchemaBuilder() {
        PaimonSelectedTab paimonTab = PaimonTestUtils.createPaimonSelectedTab();
        PaimonCompaction paimonCompaction = new PaimonCompaction();
        paimonCompaction.minFileNum = 1;
        paimonCompaction.writeOnly = false;
        // paimonCompaction.maxFileNum = 2;
        paimonCompaction.optimizationInterval = Duration.ofSeconds(60);
        paimonCompaction.sizeRatio = 1;
        paimonCompaction.sizeAmplificationPercent = 200;

        Schema.Builder schemaBuilder = new Builder();
        paimonCompaction.initializeSchemaBuilder(schemaBuilder, paimonTab);
        Schema schema = schemaBuilder.build();
        Assert.assertNotNull(schema);
        Map<String, String> options = schema.options();

        Map<String, String> expectOptions = Maps.newHashMap();
        expectOptions.put(CoreOptions.COMPACTION_MIN_FILE_NUM.key(), String.valueOf(paimonCompaction.minFileNum));
        // expectOptions.put(CoreOptions.COMPACTION_MAX_FILE_NUM.key(), String.valueOf(paimonCompaction.maxFileNum));
        expectOptions.put(CoreOptions.COMPACTION_OPTIMIZATION_INTERVAL.key(), TimeUtils.formatWithHighestUnit(paimonCompaction.optimizationInterval));
        expectOptions.put(CoreOptions.COMPACTION_SIZE_RATIO.key(), String.valueOf(paimonCompaction.sizeRatio));
        expectOptions.put(CoreOptions.COMPACTION_MAX_SIZE_AMPLIFICATION_PERCENT.key(), String.valueOf(paimonCompaction.sizeAmplificationPercent));

        MapUtils.assertMapEquals(expectOptions, options);
    }

}
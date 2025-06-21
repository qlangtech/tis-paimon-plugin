package com.qlangtech.tis.plugin.paimon.datax.sequence;

import com.beust.jcommander.internal.Lists;
import com.qlangtech.tis.plugin.common.PluginDesc;
import com.qlangtech.tis.plugin.paimon.datax.PaimonSelectedTab;
import com.qlangtech.tis.plugin.paimon.datax.test.PaimonTestUtils;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.CoreOptions.SortOrder;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.Schema.Builder;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-06-21 11:47
 **/
public class PaimonSequenceFieldsOnTest {
    @Test
    public void testDescGenerate() {
        PluginDesc.testDescGenerate(PaimonSequenceFieldsOn.class, "paimon-sequence-fields-descriptor.json");
    }

    @Test
    public void testInitializeSchemaBuilder() {

        Builder schemaBuilder = new Builder();
        PaimonSelectedTab table = PaimonTestUtils.createPaimonSelectedTab();
        PaimonSequenceFieldsOn sequenceFieldsOn = new PaimonSequenceFieldsOn();
        sequenceFieldsOn.field = Lists.newArrayList("a", "b", "c");
        sequenceFieldsOn.sortOrder = SortOrder.ASCENDING.name();
        sequenceFieldsOn.initializeSchemaBuilder(schemaBuilder, table);

        Schema schema = schemaBuilder.build();
        Assert.assertNotNull(schema);

        Map<String, String> options = schema.options();
        Assert.assertEquals(2, options.size());

        Assert.assertEquals("a,b,c", options.get(CoreOptions.SEQUENCE_FIELD.key()));
        Assert.assertEquals(SortOrder.ASCENDING.name(), options.get(CoreOptions.SEQUENCE_FIELD_SORT_ORDER.key()));
    }
}
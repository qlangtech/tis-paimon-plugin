package com.qlangtech.tis.plugins.incr.flink.pipeline.utils;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.IPropertyType;
import com.qlangtech.tis.extension.impl.PropertyType;
import com.qlangtech.tis.extension.util.PluginExtraProps;
import com.qlangtech.tis.plugins.incr.flink.pipeline.paimon.sink.PaimonPipelineSinkFactory.DftDesc;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

import static com.qlangtech.tis.plugins.incr.flink.pipeline.paimon.sink.PaimonPipelineSinkFactory.KEY_SCHEMA_BEHAVIOR;
import static org.apache.flink.cdc.common.pipeline.SchemaChangeBehavior.IGNORE;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-06-19 06:43
 **/
public class TestFlinkCDCPropAssist {

    @Test
    public void createOpts() {

        DftDesc dftDesc = new DftDesc();
        Map<String, IPropertyType> props = dftDesc.opts.getProps();
        PropertyType schemaBehaviorField = (PropertyType) dftDesc.getPropertyType(KEY_SCHEMA_BEHAVIOR);
        Assert.assertNotNull("schemaBehaviorField can not be null", schemaBehaviorField);
        // FlinkCDCPropAssist propAssist = FlinkCDCPropAssist.createOpts()
        JSONObject extraProps = schemaBehaviorField.getExtraProps();
        Assert.assertNotNull("extraProps can not be null", extraProps);

        JSONArray ops = (JSONArray) extraProps.get(Descriptor.KEY_ENUM_PROP);
        Assert.assertNotNull("extraProps can not be null", ops);
        Assert.assertEquals(5, ops.size());

        String dftVal = (String) extraProps.get(PluginExtraProps.KEY_DFTVAL_PROP);
        Assert.assertEquals(IGNORE.name(), dftVal);

    }
}
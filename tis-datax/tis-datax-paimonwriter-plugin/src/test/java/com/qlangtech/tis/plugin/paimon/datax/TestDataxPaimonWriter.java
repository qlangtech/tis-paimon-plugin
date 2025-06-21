package com.qlangtech.tis.plugin.paimon.datax;

import com.google.common.collect.Lists;
import com.qlangtech.tis.datax.IDataXPluginMeta.DataXMeta;
import com.qlangtech.tis.datax.IDataxContext;
import com.qlangtech.tis.datax.IDataxGlobalCfg;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.IDataxProcessor.TableMap;
import com.qlangtech.tis.datax.IDataxReader;
import com.qlangtech.tis.datax.IDataxReaderContext;
import com.qlangtech.tis.datax.TableAliasMapper;
import com.qlangtech.tis.datax.impl.DataXCfgGenerator;
import com.qlangtech.tis.plugin.common.PluginDesc;
import com.qlangtech.tis.plugin.datax.test.TestSelectedTabs;
import com.qlangtech.tis.plugin.paimon.datax.DataxPaimonWriter.PaimonFSDataXContext;
import com.qlangtech.tis.plugin.paimon.datax.test.PaimonTestUtils;
import com.qlangtech.tis.trigger.util.JsonUtil;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.Schema.Builder;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-05-16 11:21
 **/
public class TestDataxPaimonWriter {
    @Test
    public void testDescGenerate() throws Exception {
//        Enumeration<URL> resources = this.getClass().getClassLoader().getResources("org/apache/hadoop/ipc/protobuf/RpcHeaderProtos.class");
//        while (resources.hasMoreElements()) {
//            System.out.println(resources.nextElement());
//        }
        PluginDesc.testDescGenerate(DataxPaimonWriter.class, "paimon-datax-writer-descriptor.json");
    }

    @Test
    public void testInitializeSchemaBuilder() {
        PaimonSelectedTab tab = PaimonTestUtils.createPaimonSelectedTab();
        DataxPaimonWriter paimonWriter = PaimonTestUtils.getPaimonWriter();
        Schema.Builder tabSchemaBuilder = new Schema.Builder();
        paimonWriter.initializeSchemaBuilder(tabSchemaBuilder, tab);
        Schema schema = tabSchemaBuilder.build();
        Map<String, String> options = schema.options();
        Assert.assertEquals(8, options.size());
    }


    @Test
    public void testTemplateGenerate() throws Exception {
        //  PluginDesc.testDescGenerate(DataxPaimonWriter.class, "paimon-datax-writer-descriptor.json");

        DataxPaimonWriter paimonWriter = PaimonTestUtils.getPaimonWriter();
//        paimonWriter.dataXName = "paimon_writer";
//        paimonWriter.template = DataxPaimonWriter.getDftTemplate();

        validateConfigGenerate("paimon-datax-writer-assert.json", paimonWriter);
    }

    private void validateConfigGenerate(String assertFileName, DataxPaimonWriter paimonWriter) throws IOException {

        Optional<TableMap> tableMap = TestSelectedTabs.createTableMapper();
        IDataxContext subTaskCtx = paimonWriter.getSubTask(tableMap, Optional.empty());
        Assert.assertNotNull(subTaskCtx);

        PaimonFSDataXContext paimonContext = (PaimonFSDataXContext) subTaskCtx;
        Assert.assertNotNull("can not be null", paimonContext.getPaimonCols());
//        Assert.assertEquals(mysqlJdbcUrl, paimonContext.getJdbcUrl());
//        Assert.assertEquals("123456", paimonContext.getPassword());
//        Assert.assertEquals("orderinfo_new", paimonContext.tabName);
//        Assert.assertEquals("root", paimonContext.getUsername());

        IDataxProcessor processor = EasyMock.mock("dataxProcessor", IDataxProcessor.class);
        IDataxGlobalCfg dataxGlobalCfg = EasyMock.mock("dataxGlobalCfg", IDataxGlobalCfg.class);
        EasyMock.expect(dataxGlobalCfg.getChannel()).andReturn(3);
        EasyMock.expect(dataxGlobalCfg.getErrorLimitCount()).andReturn(1);
        EasyMock.expect(dataxGlobalCfg.getErrorLimitPercentage()).andReturn(0.2f);
        EasyMock.expect(dataxGlobalCfg.getTemplate()).andReturn(IDataxGlobalCfg.getDefaultTemplate());

        IDataxReaderContext readerContext = EasyMock.mock("readerContext", IDataxReaderContext.class);

        EasyMock.expect(readerContext.getSourceTableName()).andReturn(tableMap.get().getFrom()).anyTimes();
        IDataxReader dataxReader = EasyMock.mock("dataxReader", IDataxReader.class);
        DataXMeta readerPluginMeta = new DataXMeta();
        readerPluginMeta.setName("testReader");
        // readerPluginMeta.
        EasyMock.expect(dataxReader.getDataxMeta()).andReturn(readerPluginMeta);
        EasyMock.expect(dataxReader.getTemplate()).andReturn("{name:'" + readerPluginMeta.getName() + "'}").anyTimes();
        EasyMock.expect(processor.getRecordTransformerRulesAndPluginStore(null, TestSelectedTabs.tabNameOrderinfo))
                .andReturn(Pair.of(Lists.newArrayList(), null));
        EasyMock.expect(processor.getTabAlias(null)).andReturn(TableAliasMapper.Null);
        EasyMock.expect(processor.isReaderUnStructed(null)).andReturn(false);
        // EasyMock.expect(processor.getReader(null)).andReturn(dataxReader);
        EasyMock.expect(processor.getWriter(null)).andReturn(paimonWriter);
        EasyMock.expect(processor.getDataXGlobalCfg()).andReturn(dataxGlobalCfg);
        EasyMock.replay(processor, readerContext, dataxGlobalCfg, dataxReader);


        DataXCfgGenerator dataProcessor = new DataXCfgGenerator(null, "testDataXName", processor) {
        };

        String cfgResult = dataProcessor.generateDataxConfig(readerContext, paimonWriter, dataxReader, tableMap);

        JsonUtil.assertJSONEqual(this.getClass(), assertFileName, cfgResult, (m, e, a) -> {
            Assert.assertEquals(m, e, a);
        });
        EasyMock.verify(processor, dataxGlobalCfg, dataxReader);
    }
}

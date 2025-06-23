package com.qlangtech.tis.plugin.paimon.datax;

import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.IDataxProcessor.TableMap;
import com.qlangtech.tis.datax.IDataxReader;
import com.qlangtech.tis.datax.IDataxReaderContext;
import com.qlangtech.tis.datax.TableAliasMapper;
import com.qlangtech.tis.datax.impl.DataXCfgGenerator;
import com.qlangtech.tis.datax.impl.DataxProcessor;
import com.qlangtech.tis.datax.impl.DataxWriter;
import com.qlangtech.tis.exec.IExecChainContext;
import com.qlangtech.tis.fullbuild.indexbuild.IRemoteTaskPostTrigger;
import com.qlangtech.tis.fullbuild.indexbuild.IRemoteTaskPreviousTrigger;
import com.qlangtech.tis.plugin.common.BasicTemplate;
import com.qlangtech.tis.plugin.common.DataXCfgJson;
import com.qlangtech.tis.plugin.common.WriterTemplate;
import com.qlangtech.tis.plugin.paimon.datax.test.PaimonTestUtils;
import com.qlangtech.tis.sql.parser.tuple.creator.EntityName;

import org.apache.commons.lang3.tuple.Pair;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.Optional;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-05-19 13:40
 **/
public class TestDataxPaimonWriterRealReal {

//    @Rule
//    public TemporaryFolder folder = new TemporaryFolder();

    @BeforeClass
    public static void beforeClazz() {
        //
        //  System.setProperty(KEY_JAVA_RUNTIME_PROP_ENV_PROPS, Boolean.TRUE.toString());
    }

    @Test
    public void testRealDump() throws Exception {


        String testDataXName = "mysql_paimon";

        final DataxPaimonWriter writer = PaimonTestUtils.getPaimonWriter();

        // writer.tableBucket = 1;
        writer.dataXName = testDataXName;

        PaimonSelectedTab tab = PaimonTestUtils.createPaimonSelectedTab();

        IDataxProcessor.TableMap tabMap = new IDataxProcessor.TableMap(tab); //IDataxProcessor.TableMap.create(targetTableName, colMetas);
        //   TestDataXDaMengWriter.setPlaceholderReader();

        DataXCfgJson wjson = DataXCfgJson.content(generateDataXCfg(writer, Optional.of(tabMap)));
        // CreateTableSqlBuilder.CreateDDL ddl = writer.generateCreateDDL(SourceColMetaGetter.getNone(), tabMap, Optional.empty());

        DataxProcessor dataXProcessor = EasyMock.mock("dataXProcessor", DataxProcessor.class);


        IDataxReader dataxReader = EasyMock.mock("dataXReader", IDataxReader.class);
        EasyMock.expect(dataXProcessor.getReader(null)).andReturn(dataxReader).anyTimes();
        EasyMock.expect(dataxReader.getSelectedTab(tab.getName())).andReturn(tab);
        // File createDDLDir = folder.newFolder();// new File(".");
        // File createDDLFile = null;
        try {
            //   createDDLFile = new File(createDDLDir, targetTableName + DataXCfgFile.DATAX_CREATE_DDL_FILE_NAME_SUFFIX);
            // FileUtils.write(createDDLFile, ddl.getDDLScript(), TisUTF8.get());

            //  EasyMock.expect(dataXProcessor.getDataxCreateDDLDir(null)).andReturn(createDDLDir);
            //  dataXProcessor.getReader(null);
            EasyMock.expect(dataXProcessor.getWriter(null)).andReturn(writer);
            DataxWriter.dataxWriterGetter = (dataXName) -> {
                return writer;
            };
            DataxProcessor.processorGetter = (dataXName) -> {
                Assert.assertEquals(testDataXName, dataXName);
                return dataXProcessor;
            };

            EasyMock.expect(dataXProcessor.getRecordTransformerRulesAndPluginStore(null, tab.getName()))
                    .andReturn(Pair.of(Collections.emptyList(), null)).anyTimes();
            final int taskId = 999;
            IExecChainContext execContext = EasyMock.mock("execContext", IExecChainContext.class);
            EasyMock.expect(execContext.getTaskId()).andReturn(taskId);
            EasyMock.expect(execContext.getProcessor()).andReturn(dataXProcessor).anyTimes();

            EasyMock.replay(dataXProcessor, dataxReader, execContext);
//            String[] jdbcUrl = new String[1];
//            writer.getDataSourceFactory().getDbConfig().vistDbURL(false, (a, b, url) -> {
//                jdbcUrl[0] = url;
//            });

//            if (StringUtils.isEmpty(jdbcUrl[0])) {
//                throw new IllegalStateException("jdbcUrl[0] can not be empty");
//            }
            //WriterJson wjson = WriterJson.path("dameng_writer_real_dump.json");
            wjson.addCfgSetter((cfg) -> {
                //  cfg.set("parameter.connection[0].jdbcUrl", jdbcUrl[0]);
                return cfg;
            });
            Assert.assertTrue("isGenerateCreateDDLSwitchOff shall be false", writer.isGenerateCreateDDLSwitchOff());

            EntityName entity = EntityName.create(writer.catalog.getDBName().get(), tab.getName());
            IRemoteTaskPreviousTrigger previousTrigger = writer.createPreExecuteTask(execContext, entity, tab);
            previousTrigger.run();

            IRemoteTaskPostTrigger postTask = writer.createPostTask(execContext, entity, tab, null);

            WriterTemplate.realExecuteDump(taskId, testDataXName, wjson, writer);

            postTask.run();

            EasyMock.verify(dataXProcessor, dataxReader, execContext);
        } finally {
            //   FileUtils.deleteQuietly(createDDLFile);
        }
    }

    public static String generateDataXCfg(DataxPaimonWriter paimonWriter, Optional<IDataxProcessor.TableMap> tableMap) throws IOException {
        TableMap tableMap1 = tableMap.get();
        DataXCfgGenerator dataProcessor = BasicTemplate.createMockDataXCfgGenerator(DataxPaimonWriter.getDftTemplate()
                , new TableAliasMapper(Collections.singletonMap(tableMap1.getFrom(), tableMap1)));

        String cfgResult = dataProcessor.generateDataxConfig(new MockDataxReaderContext(tableMap1.getFrom()), paimonWriter, null, tableMap);
        return cfgResult;
    }

    private static class MockDataxReaderContext implements IDataxReaderContext {
        private final String sourceTableName;

        public MockDataxReaderContext(String sourceTableName) {
            this.sourceTableName = sourceTableName;
        }

        @Override
        public String getReaderContextId() {
            return "";
        }

        @Override
        public String getTaskName() {
            return "";
        }

        @Override
        public String getSourceEntityName() {
            return "";
        }

        @Override
        public String getSourceTableName() {
            return this.sourceTableName;
        }
    }

}

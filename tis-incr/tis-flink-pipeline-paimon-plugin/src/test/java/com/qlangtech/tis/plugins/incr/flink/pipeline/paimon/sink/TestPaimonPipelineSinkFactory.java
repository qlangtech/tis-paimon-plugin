package com.qlangtech.tis.plugins.incr.flink.pipeline.paimon.sink;

import com.qlangtech.plugins.incr.flink.chunjun.doris.sink.TestFlinkSinkExecutor;
import com.qlangtech.tis.datax.TableAlias;
import com.qlangtech.tis.datax.impl.DataxWriter;
import com.qlangtech.tis.plugin.datax.SelectedTab;
import com.qlangtech.tis.plugin.datax.common.AutoCreateTable;
import com.qlangtech.tis.plugin.datax.common.impl.AutoCreateTableColCommentSwitchOFF;
import com.qlangtech.tis.plugin.datax.common.impl.NoneCreateTable;
import com.qlangtech.tis.plugin.ds.BasicDataSourceFactory;
import com.qlangtech.tis.plugin.ds.CMeta;
import com.qlangtech.tis.plugin.incr.IncrStreamFactory;
import com.qlangtech.tis.plugin.paimon.datax.PaimonSelectedTab;
import com.qlangtech.tis.plugin.paimon.datax.pt.OnPaimonPartition;
import com.qlangtech.tis.plugin.paimon.datax.sequence.PaimonSequenceFieldsOff;
import com.qlangtech.tis.plugin.paimon.datax.test.PaimonTestUtils;
import com.qlangtech.tis.plugin.paimon.datax.utils.PaimonAutoCreateTable;
import com.qlangtech.tis.plugin.timezone.TISTimeZone;
import com.qlangtech.tis.realtime.TabSinkFunc;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.pipeline.SchemaChangeBehavior;
import org.easymock.EasyMock;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-05-26 09:59
 **/
@RunWith(Parameterized.class)
public class TestPaimonPipelineSinkFactory extends TestFlinkSinkExecutor<PaimonPipelineSinkFactory, Event> {

    @Parameterized.Parameters(name = "{0}")
    public static Object[][] data() {
        PaimonAutoCreateTable paimonAutoCreateTable = new PaimonAutoCreateTable();
        paimonAutoCreateTable.aliasPrefix = "ods_";
        paimonAutoCreateTable.addComment = new AutoCreateTableColCommentSwitchOFF();
        return new Object[][]{ //
                {new NoneCreateTable()},
                {paimonAutoCreateTable}
        };
    }

    private final AutoCreateTable autoCreateTable;

    public TestPaimonPipelineSinkFactory(AutoCreateTable autoCreateTable) {
        this.autoCreateTable = autoCreateTable;
    }

    @Test
    public void testPaimonWrite() throws Exception {
        IncrStreamFactory streamFactory = mock("streamFactory", IncrStreamFactory.class);
        EasyMock.expect(streamFactory.getParallelism()).andReturn(1);
        IncrStreamFactory.stubStreamFactory = (dataXName) -> {
            return streamFactory;
        };
        super.testSinkSync();
    }

    @Override
    protected void startTestSinkSync(Map<TableAlias, TabSinkFunc<?, ?, Event>> sinkFunction) {
        super.startTestSinkSync(sinkFunction);

    }

    @Override
    protected boolean isFlinkCDCPipelineEnable() {
        return true;
    }

    @Override
    protected SelectedTab createSelectedTab(List<CMeta> metaCols) {
        PaimonSelectedTab tab = new PaimonSelectedTab();
        OnPaimonPartition pt = new OnPaimonPartition();
        pt.partitionPathFields = Collections.singletonList(colCreateTime);
        tab.partition = pt;
        tab.cols.addAll(metaCols);
        tab.sequenceField = new PaimonSequenceFieldsOff();
        return tab;
    }

    @Override
    protected BasicDataSourceFactory getDsFactory() {
        return null;
    }

    @Override
    protected PaimonPipelineSinkFactory getSinkFactory() {
        PaimonPipelineSinkFactory sinkFactory = new PaimonPipelineSinkFactory();
        sinkFactory.timeZone = TISTimeZone.dftZone();
        sinkFactory.schemaBehavior = SchemaChangeBehavior.IGNORE.name();
        // sinkFactory.parallelism = 1;
        return sinkFactory;
    }

    @Override
    protected DataxWriter createDataXWriter() {
        return PaimonTestUtils.getPaimonWriter(
                Objects.requireNonNull(this.autoCreateTable, "autoCreateTable can not be null"));

    }
}
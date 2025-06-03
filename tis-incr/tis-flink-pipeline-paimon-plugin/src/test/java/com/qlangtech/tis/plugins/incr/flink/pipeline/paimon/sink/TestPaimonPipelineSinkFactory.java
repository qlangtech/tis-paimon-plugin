package com.qlangtech.tis.plugins.incr.flink.pipeline.paimon.sink;

import com.qlangtech.plugins.incr.flink.chunjun.doris.sink.TestFlinkSinkExecutor;
import com.qlangtech.tis.datax.impl.DataxWriter;
import com.qlangtech.tis.plugin.datax.SelectedTab;
import com.qlangtech.tis.plugin.ds.BasicDataSourceFactory;
import com.qlangtech.tis.plugin.ds.CMeta;
import com.qlangtech.tis.plugin.paimon.datax.PaimonSelectedTab;
import com.qlangtech.tis.plugin.paimon.datax.test.PaimonTestUtils;
import org.apache.flink.cdc.common.event.Event;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-05-26 09:59
 **/
public class TestPaimonPipelineSinkFactory extends TestFlinkSinkExecutor<PaimonPipelineSinkFactory, Event> {

    @Test
    public void testPaimonWrite() throws Exception {
        super.testSinkSync();
    }

    @Override
    protected boolean isFlinkCDCPipelineEnable() {
        return true;
    }

    @Override
    protected SelectedTab createSelectedTab(List<CMeta> metaCols) {
        PaimonSelectedTab tab = new PaimonSelectedTab();
        tab.partitionPathFields = Collections.singletonList(colCreateTime);
        tab.cols.addAll(metaCols);
        return tab;
    }

    @Override
    protected BasicDataSourceFactory getDsFactory() {
        return null;
    }

    @Override
    protected PaimonPipelineSinkFactory getSinkFactory() {
        PaimonPipelineSinkFactory sinkFactory = new PaimonPipelineSinkFactory();
        sinkFactory.timeZone = com.qlangtech.tis.async.message.client.consumer.impl.MQListenerFactory.dftZoneId();
        sinkFactory.parallelism = 1;
        return sinkFactory;
    }

    @Override
    protected DataxWriter createDataXWriter() {
        return PaimonTestUtils.getPaimonWriter();

    }
}
//package com.qlangtech.tis.plugins.incr.flink.pipeline.paimon.sink;
//
//import com.qlangtech.plugins.incr.flink.chunjun.doris.sink.TestFlinkSinkExecutor;
//import com.qlangtech.tis.datax.impl.DataxWriter;
//import com.qlangtech.tis.plugin.ds.BasicDataSourceFactory;
//import com.qlangtech.tis.plugin.paimon.datax.test.PaimonTestUtils;
//import org.apache.flink.cdc.common.event.Event;
//import org.junit.Test;
//
//import java.net.URL;
//import java.util.Enumeration;
//
///**
// * @author: 百岁（baisui@qlangtech.com）
// * @create: 2025-05-26 09:59
// **/
//public class TestPaimonPipelineSinkFactory extends TestFlinkSinkExecutor<PaimonPipelineSinkFactory, Event> {
//
//    @Test
//    public void testPaimonWrite() throws Exception {
//         super.testSinkSync();
//    }
//
//    @Override
//    protected BasicDataSourceFactory getDsFactory() {
//        return null;
//    }
//
//    @Override
//    protected PaimonPipelineSinkFactory getSinkFactory() {
//        PaimonPipelineSinkFactory sinkFactory = new PaimonPipelineSinkFactory();
//        sinkFactory.timeZone = com.qlangtech.tis.async.message.client.consumer.impl.MQListenerFactory.dftZoneId();
//        sinkFactory.parallelism = 1;
//        return sinkFactory;
//    }
//
//    @Override
//    protected DataxWriter createDataXWriter() {
//        return PaimonTestUtils.getPaimonWriter();
//
//    }
//}
package com.qlangtech.tis.plugins.incr.flink.connector;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.List;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-05-23 13:05
 **/
public class CreateSinkFunctionResult {
    public List<String> primaryKeys;

    public Sink<Event> getSinkFunction() {

        org.apache.flink.cdc.connectors.paimon.sink.v2.PaimonSink<Event>  sinkFunc = null;
       // org.apache.flink.streaming.api.functions.sink.SinkFunction

        return sinkFunc;
    }
}

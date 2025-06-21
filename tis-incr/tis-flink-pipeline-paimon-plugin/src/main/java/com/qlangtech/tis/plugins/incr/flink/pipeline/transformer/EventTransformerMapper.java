//package com.qlangtech.tis.plugins.incr.flink.pipeline.transformer;
//
//import com.google.common.collect.Maps;
//import com.qlangtech.tis.datax.IDataxProcessor;
//import com.qlangtech.tis.datax.impl.DataxProcessor;
//import com.qlangtech.tis.plugin.datax.transformer.RecordTransformerRules;
//import org.apache.flink.api.common.functions.MapFunction;
//import org.apache.flink.api.common.functions.OpenContext;
//import org.apache.flink.api.common.functions.RichMapFunction;
//import org.apache.flink.cdc.common.event.DataChangeEvent;
//import org.apache.flink.cdc.common.event.Event;
//import org.apache.flink.cdc.common.event.OperationType;
//import org.apache.flink.cdc.common.event.TableId;
//import org.apache.flink.configuration.Configuration;
//
//import java.util.Map;
//import java.util.Objects;
//import java.util.Optional;
//import java.util.concurrent.ConcurrentMap;
//import java.util.function.Function;
//import com.qlangtech.tis.plugins.incr.flink.pipeline.transformer.EventRowFields
//
///**
// * @author: 百岁（baisui@qlangtech.com）
// * @create: 2025-06-17 17:36
// **/
//public class EventTransformerMapper extends RichMapFunction<Event, Event> {
//
//    private transient ConcurrentMap<TableId, Optional<RecordTransformerRules>> table2TransformerRules;
//
//    private transient Function<TableId, Optional<RecordTransformerRules>> transformerRulesCreator;
//
//    private transient IDataxProcessor dataxProcessor;
//
//    private final String pipelineName;
//
//    public EventTransformerMapper(String pipelineName) {
//        this.pipelineName = pipelineName;
//    }
//
//    @Override
//    public void open(Configuration parameters) throws Exception {
//        this.table2TransformerRules = Maps.newConcurrentMap();
//        this.dataxProcessor = DataxProcessor.load(null, pipelineName);
//        this.transformerRulesCreator = (tableId) -> {
//            RecordTransformerRules.loadTransformerRules(
//                    null
//                    , this.dataxProcessor
//                    , tableId.getTableName());
//        };
//    }
//
//    @Override
//    public Event map(Event value) throws Exception {
//
//        if (!(value instanceof DataChangeEvent)) {
//            return value;
//        }
//
//        DataChangeEvent dataChange = (DataChangeEvent) value;
//        if (OperationType.DELETE == dataChange.op()) {
//            return dataChange;
//        }
//
//        TableId tableId = dataChange.tableId();
//
//        Optional<RecordTransformerRules> transformerRules = table2TransformerRules.computeIfAbsent(tableId, transformerRulesCreator);
//
//        if (!transformerRules.isPresent()) {
//          return dataChange;
//        }
//
//        return dataChange;
//    }
//}

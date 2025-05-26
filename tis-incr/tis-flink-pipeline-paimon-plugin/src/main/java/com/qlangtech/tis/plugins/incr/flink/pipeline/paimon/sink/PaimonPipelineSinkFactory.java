//package com.qlangtech.tis.plugins.incr.flink.pipeline.paimon.sink;
//
//import com.google.common.collect.Maps;
//import com.google.common.collect.Sets;
//import com.qlangtech.plugins.incr.flink.cdc.FlinkCol;
//import com.qlangtech.tis.async.message.client.consumer.IFlinkColCreator;
//import com.qlangtech.tis.async.message.client.consumer.impl.MQListenerFactory;
//import com.qlangtech.tis.compiler.incr.ICompileAndPackage;
//import com.qlangtech.tis.compiler.streamcode.CompileAndPackage;
//import com.qlangtech.tis.datax.IDataxProcessor;
//import com.qlangtech.tis.datax.IDataxReader;
//import com.qlangtech.tis.datax.TableAlias;
//import com.qlangtech.tis.extension.TISExtension;
//import com.qlangtech.tis.plugin.IEndTypeGetter;
//import com.qlangtech.tis.plugin.annotation.FormField;
//import com.qlangtech.tis.plugin.annotation.FormFieldType;
//import com.qlangtech.tis.plugin.annotation.Validator;
//import com.qlangtech.tis.plugin.ds.ISelectedTab;
//import com.qlangtech.tis.plugins.incr.flink.connector.PipelineEventSinkFunc;
//import com.qlangtech.tis.plugins.incr.flink.connector.PipelineFlinkCDCSinkFactory;
//import com.qlangtech.tis.realtime.DTOSourceTagProcessFunction;
//import com.qlangtech.tis.realtime.TabSinkFunc;
//import com.qlangtech.tis.util.HeteroEnum;
//import org.apache.flink.cdc.common.event.Event;
//
//import java.time.ZoneId;
//import java.util.List;
//import java.util.Map;
//import java.util.Objects;
//
///**
// * @author: 百岁（baisui@qlangtech.com）
// * @create: 2025-05-22 09:46
// **/
//public class PaimonPipelineSinkFactory extends PipelineFlinkCDCSinkFactory {
//    @FormField(ordinal = 14, type = FormFieldType.ENUM, validate = {Validator.require})
//    public String timeZone;
//
//    public ZoneId getTimeZone() {
//        return ZoneId.of(this.timeZone);
//    }
//
//    @Override
//    public ICompileAndPackage getCompileAndPackageManager() {
//        return new CompileAndPackage(Sets.newHashSet(PaimonPipelineSinkFactory.class));
//    }
//
//    @Override
//    public Map<TableAlias, TabSinkFunc<?, ?, Event>> createSinkFunction(IDataxProcessor dataxProcessor, IFlinkColCreator flinkColCreator) {
//        //  return super.createSinkFunction(dataxProcessor, flinkColCreator);
//
//        IDataxReader reader = dataxProcessor.getReader(null);
//        List<ISelectedTab> tabs = reader.getSelectedTabs();
//        Map<TableAlias, TabSinkFunc<?, ?, Event>> sinkFuncs = Maps.newHashMap();
//        //  List<ISelectedTab> tabs = reader.getSelectedTabs();
//
//        //        IDataxProcessor dataxProcessor
////            , PaimonPipelineSinkFactory pipelineSinkFactory //
////            , List<ISelectedTab> tabs //
////            , IFlinkColCreator<FlinkCol> sourceFlinkColCreator
////            , Sink<Event> sinkFunction //
////            , int sinkTaskParallelism
//
//        MQListenerFactory sourceListenerFactory = HeteroEnum.getIncrSourceListenerFactory(dataxProcessor.getDataXName());
//        IFlinkColCreator<FlinkCol> sourceFlinkColCreator
//                = Objects.requireNonNull(sourceListenerFactory, "sourceListenerFactory").createFlinkColCreator(reader);
//
//        sinkFuncs.put(TableAlias.create(DTOSourceTagProcessFunction.KEY_MERGE_ALL_TABS_IN_ONE_BUS, DTOSourceTagProcessFunction.KEY_MERGE_ALL_TABS_IN_ONE_BUS),
//                new PipelineEventSinkFunc(dataxProcessor, this
//                        , tabs
//                        , sourceFlinkColCreator
//                        , null
//                        // , supportUpsetDML()
//                        // , filterRowKinds
//                        , this.parallelism)
//        );
//
//        return sinkFuncs;
//    }
//
//    @TISExtension
//    public static class DftDesc extends BasicPipelineSinkDescriptor {
//        @Override
//        protected IEndTypeGetter.EndType getTargetType() {
//            return EndType.Paimon;
//        }
//    }
//}

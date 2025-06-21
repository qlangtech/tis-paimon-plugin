package com.qlangtech.tis.plugins.incr.flink.pipeline.paimon.sink;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.qlangtech.plugins.incr.flink.cdc.FlinkCol;
import com.qlangtech.tis.async.message.client.consumer.IFlinkColCreator;
import com.qlangtech.tis.async.message.client.consumer.impl.MQListenerFactory;
import com.qlangtech.tis.compiler.incr.ICompileAndPackage;
import com.qlangtech.tis.compiler.streamcode.CompileAndPackage;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.IDataxReader;
import com.qlangtech.tis.datax.TableAlias;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.extension.util.AbstractPropAssist.Options;
import com.qlangtech.tis.extension.util.OverwriteProps;
import com.qlangtech.tis.manage.common.Option;
import com.qlangtech.tis.plugin.IEndTypeGetter;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.plugin.incr.IncrStreamFactory;
import com.qlangtech.tis.plugin.incr.TISSinkFactory;
import com.qlangtech.tis.plugin.timezone.TISTimeZone;
import com.qlangtech.tis.plugins.incr.flink.connector.PipelineEventSinkFunc;
import com.qlangtech.tis.plugins.incr.flink.connector.PipelineFlinkCDCSinkFactory;
import com.qlangtech.tis.plugins.incr.flink.pipeline.utils.FlinkCDCPropAssist;
import com.qlangtech.tis.realtime.DTOSourceTagProcessFunction;
import com.qlangtech.tis.realtime.TabSinkFunc;
import com.qlangtech.tis.util.HeteroEnum;
import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.pipeline.PipelineOptions;
import org.apache.flink.cdc.common.pipeline.SchemaChangeBehavior;

import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-05-22 09:46
 **/
public class PaimonPipelineSinkFactory extends PipelineFlinkCDCSinkFactory {

    public static final String KEY_SCHEMA_BEHAVIOR = "schemaBehavior";

    @FormField(ordinal = 14, validate = {Validator.require})
    public TISTimeZone timeZone;

    @FormField(ordinal = 13, type = FormFieldType.ENUM, validate = {Validator.require})
    public String schemaBehavior;

    // public static List<Option> a

    public SchemaChangeBehavior getSchemaChangeBehavior() {
        return SchemaChangeBehavior.valueOf(this.schemaBehavior);
    }

    public ZoneId getTimeZone() {
        return Objects.requireNonNull(timeZone, "timeZone can not be null").getTimeZone();// ZoneId.of(this.timeZone);
    }

    @Override
    public ICompileAndPackage getCompileAndPackageManager() {
        return new CompileAndPackage(Sets.newHashSet(PaimonPipelineSinkFactory.class));
    }

    @Override
    public Map<TableAlias, TabSinkFunc<?, ?, Event>> createSinkFunction(IDataxProcessor dataxProcessor, IFlinkColCreator flinkColCreator) {
        //  return super.createSinkFunction(dataxProcessor, flinkColCreator);

        IDataxReader reader = dataxProcessor.getReader(null);
        List<ISelectedTab> tabs = reader.getSelectedTabs();
        Map<TableAlias, TabSinkFunc<?, ?, Event>> sinkFuncs = Maps.newHashMap();
        //  List<ISelectedTab> tabs = reader.getSelectedTabs();

        //        IDataxProcessor dataxProcessor
//            , PaimonPipelineSinkFactory pipelineSinkFactory //
//            , List<ISelectedTab> tabs //
//            , IFlinkColCreator<FlinkCol> sourceFlinkColCreator
//            , Sink<Event> sinkFunction //
//            , int sinkTaskParallelism
        IncrStreamFactory streamFactory = IncrStreamFactory.getFactory(dataxProcessor.identityValue());
        MQListenerFactory sourceListenerFactory = HeteroEnum.getIncrSourceListenerFactory(dataxProcessor.getDataXName());
        IFlinkColCreator<FlinkCol> sourceFlinkColCreator
                = Objects.requireNonNull(sourceListenerFactory, "sourceListenerFactory").createFlinkColCreator(reader);

        sinkFuncs.put(DTOSourceTagProcessFunction.createAllMergeTableAlias(),
                new PipelineEventSinkFunc(dataxProcessor, this
                        , tabs
                        , sourceFlinkColCreator
                        , null
                        , streamFactory.getParallelism())
        );

        return sinkFuncs;
    }

    @TISExtension
    public static class DftDesc extends BasicPipelineSinkDescriptor {
        public Options<TISSinkFactory, ConfigOption> opts;

        public DftDesc() {
            super();
            this.opts = FlinkCDCPropAssist.createOpts(this);
            OverwriteProps schemaBehaviorOverwrite = new OverwriteProps();
            schemaBehaviorOverwrite.setDftVal(SchemaChangeBehavior.IGNORE.name());
//            schemaBehaviorOverwrite.setLabelRewrite((l) -> {
//                return "表结构变化";
//            });
            this.opts.add(KEY_SCHEMA_BEHAVIOR, PipelineOptions.PIPELINE_SCHEMA_CHANGE_BEHAVIOR, schemaBehaviorOverwrite);
        }

        @Override
        protected IEndTypeGetter.EndType getTargetType() {
            return EndType.Paimon;
        }


    }
}

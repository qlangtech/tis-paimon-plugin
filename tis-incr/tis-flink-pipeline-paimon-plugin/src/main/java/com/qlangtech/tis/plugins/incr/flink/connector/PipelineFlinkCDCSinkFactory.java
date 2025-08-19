package com.qlangtech.tis.plugins.incr.flink.connector;

import com.qlangtech.tis.async.message.client.consumer.IFlinkColCreator;
import com.qlangtech.tis.datax.IDataXNameAware;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.TableAlias;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.realtime.BasicTISSinkFactory;
import com.qlangtech.tis.realtime.DTOSourceTagProcessFunction;
import com.qlangtech.tis.realtime.TabSinkFunc;
import com.qlangtech.tis.sql.parser.tuple.creator.AdapterStreamTemplateData;
import com.qlangtech.tis.sql.parser.tuple.creator.IStreamIncrGenerateStrategy;
import org.apache.flink.cdc.common.event.Event;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * 支持flink-cdc pipeline sink
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-05-22 09:01
 **/
public abstract class PipelineFlinkCDCSinkFactory
        extends BasicTISSinkFactory<Event> implements IDataXNameAware, IStreamIncrGenerateStrategy {

    public static final String DISPLAY_NAME_FLINK_PIPELINE_SINK = "FlinkCDC-Pipeline-Sink-";
    @Override
    public Map<TableAlias, TabSinkFunc<?, ?, Event>> createSinkFunction(IDataxProcessor dataxProcessor, IFlinkColCreator flinkColCreator) {
        throw new UnsupportedOperationException();
    }

    @Override
    public IStreamTemplateData decorateMergeData(IStreamTemplateData mergeData) {
        return new AdapterStreamTemplateData(mergeData) {
            @Override
            public List<TableAlias> getDumpTables() {
                return Collections.singletonList(DTOSourceTagProcessFunction.createAllMergeTableAlias());
            }
        };
    }

    @Override
    public boolean flinkCDCPipelineEnable() {
        return true;
    }

    protected static abstract class BasicPipelineSinkDescriptor extends BaseSinkFunctionDescriptor {
        @Override
        public final String getDisplayName() {
            return DISPLAY_NAME_FLINK_PIPELINE_SINK + this.getTargetType().name();
        }

        @Override
        public final PluginVender getVender() {
            return PluginVender.FLINK_CDC;
        }
    }
}

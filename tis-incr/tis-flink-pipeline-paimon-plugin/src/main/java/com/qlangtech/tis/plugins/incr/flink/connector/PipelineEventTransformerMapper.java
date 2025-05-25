package com.qlangtech.tis.plugins.incr.flink.connector;

import com.qlangtech.plugins.incr.flink.cdc.FlinkCol;
import com.qlangtech.tis.plugins.incr.flink.cdc.AbstractTransformerRecord;
import com.qlangtech.tis.plugins.incr.flink.cdc.ReocrdTransformerMapper;
import com.qlangtech.tis.realtime.SelectedTableTransformerRules;
import org.apache.flink.cdc.common.event.Event;

import java.util.List;
import java.util.Objects;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-05-22 10:49
 **/
public class PipelineEventTransformerMapper extends ReocrdTransformerMapper<Event> {
    private final List<FlinkCol> originColsWithContextParamsFlinkCol;
    private final int delegateArity;

    public PipelineEventTransformerMapper(SelectedTableTransformerRules triple, int delegateArity) {
        super(FlinkCol.getAllTabColsMeta(triple.overwriteColsWithContextParams() //rule.overwriteCols(table.getCols())
                , Objects.requireNonNull(triple.getSourceFlinkColCreator(), "flinkColCreator")), triple.getTransformerRules());
        this.originColsWithContextParamsFlinkCol = triple.originColsWithContextParamsFlinkCol();
        this.delegateArity = delegateArity;
    }

    @Override
    protected AbstractTransformerRecord<Event> createDelegate(Event row) {
        return new TransformerPipelineEvent(row, this.cols, this.originColsWithContextParamsFlinkCol, this.delegateArity);
    }
}

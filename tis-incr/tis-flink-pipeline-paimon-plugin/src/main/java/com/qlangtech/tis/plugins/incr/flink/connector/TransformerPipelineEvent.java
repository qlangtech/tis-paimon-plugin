package com.qlangtech.tis.plugins.incr.flink.connector;

import com.qlangtech.plugins.incr.flink.cdc.FlinkCol;
import com.qlangtech.tis.plugins.incr.flink.cdc.AbstractTransformerRecord;
import org.apache.flink.cdc.common.event.Event;

import java.util.List;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-05-22 10:36
 **/
public class TransformerPipelineEvent extends AbstractTransformerRecord<Event> implements Event {
    public TransformerPipelineEvent(Event row, List<FlinkCol> rewriteCols
            , List<FlinkCol> originColsWithContextParamsFlinkCol, int delegateArity) {
        super(row);
    }

    @Override
    public Event getDelegate() {
        return row;
    }

    @Override
    public void setColumn(String field, Object colVal) {

    }

    @Override
    public void setString(String field, String val) {

    }

    @Override
    public Object getColumn(String field) {
        return null;
    }
}

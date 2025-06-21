package com.qlangtech.tis.plugins.incr.flink.pipeline.transformer;

import com.alibaba.datax.common.element.ICol2Index.Col;
import com.qlangtech.plugins.incr.flink.cdc.FlinkCol;
import com.qlangtech.tis.plugins.incr.flink.cdc.AbstractTransformerRecord;

import java.util.List;
import java.util.Objects;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-06-17 18:51
 **/
public class EventRowFields extends AbstractTransformerRecord<Object[]> {

    private final List<FlinkCol> cols;

    public EventRowFields(Object[] row, List<FlinkCol> cols) {
        super(row);
        this.cols = Objects.requireNonNull(cols, "cols can not be null");
    }

    @Override
    public Object getColumn(String field) {
        Col col = this.col2IndexMapper.getCol2Index().get(field);
        return this.row[col.getIndex()];
    }

    @Override
    public void setColumn(String field, Object colVal) {
        if (colVal == null) {
            return;
        }
        Col col = this.col2IndexMapper.getCol2Index().get(field);
        if (col == null) {
            throw new IllegalStateException("field:" + field + " relevant col can not be null");
        }
        try {
            FlinkCol flinkCol = cols.get(col.getIndex());
            this.row[col.getIndex()] = flinkCol.flinkCDCPipelineEventProcess.apply(colVal);
        } catch (Exception e) {
            throw new RuntimeException(String.valueOf(col), e);
        }
    }


    @Override
    public void setString(String field, String val) {
        this.setColumn(field, val);
    }

    @Override
    public String getString(String field, boolean origin) {
        Object colVal = this.getColumn(field);
        return colVal != null ? String.valueOf(colVal) : null;
    }

    @Override
    public Object[] getDelegate() {
        return this.row;
    }
}

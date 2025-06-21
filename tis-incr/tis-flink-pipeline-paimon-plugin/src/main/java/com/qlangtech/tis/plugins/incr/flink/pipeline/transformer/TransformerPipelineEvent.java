//package com.qlangtech.tis.plugins.incr.flink.pipeline.transformer;
//
//import com.qlangtech.plugins.incr.flink.cdc.FlinkCol;
//import com.qlangtech.tis.plugins.incr.flink.cdc.AbstractTransformerRecord;
//import org.apache.flink.cdc.common.event.Event;
//
//import java.util.List;
//
///**
// * @author: 百岁（baisui@qlangtech.com）
// * @create: 2025-05-22 10:36
// **/
//public class TransformerPipelineEvent extends AbstractTransformerRecord<Object[]>  {
//    public TransformerPipelineEvent(Object[] row, List<FlinkCol> rewriteCols
//            , List<FlinkCol> originColsWithContextParamsFlinkCol, int delegateArity) {
//        super(row);
//    }
//
//    @Override
//    public Object[] getDelegate() {
//        return row;
//    }
//
//    @Override
//    public void setColumn(String field, Object colVal) {
//
//    }
//
//    @Override
//    public void setString(String field, String val) {
//
//    }
//
//    @Override
//    public Object getColumn(String field) {
//        return null;
//    }
//}

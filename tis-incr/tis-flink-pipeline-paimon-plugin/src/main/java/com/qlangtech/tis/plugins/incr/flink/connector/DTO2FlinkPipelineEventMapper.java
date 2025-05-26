package com.qlangtech.tis.plugins.incr.flink.connector;


import com.google.common.collect.Maps;
import com.qlangtech.plugins.incr.flink.cdc.FlinkCol;
import com.qlangtech.plugins.incr.flink.cdc.FlinkCol.DTOConvertTo;
import com.qlangtech.tis.plugins.incr.flink.cdc.BasicFlinkDataMapper;
import com.qlangtech.tis.realtime.transfer.DTO;
import com.qlangtech.tis.realtime.transfer.DTO.EventType;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-05-22 11:31
 * @see com.qlangtech.tis.plugins.incr.flink.cdc.DTO2RowDataMapper
 **/
public class DTO2FlinkPipelineEventMapper extends BasicFlinkDataMapper<DataChangeEvent, Event> {
    private transient Map<String, BinaryRecordDataGenerator> tab2GenMapper;
    private final Map<String /**tabName*/, List<FlinkCol>> sourceColsMetaMapper;

    public DTO2FlinkPipelineEventMapper(Map<String /**tabName*/, List<FlinkCol>> sourceColsMetaMapper) {
        super(DTOConvertTo.FlinkCDCPipelineEvent);

        this.sourceColsMetaMapper = sourceColsMetaMapper;
//        this.tab2GenMapper
//                = sourceColsMetaMapper.entrySet().stream().collect(Collectors.toMap((e) -> e.getKey(), (e) -> {
//            List<FlinkCol> cols = e.getValue();
//            org.apache.flink.cdc.common.types.DataType[] dataTypes = new org.apache.flink.cdc.common.types.DataType[cols.size()];
//            for (int idx = 0; idx < cols.size(); idx++) {
//                FlinkCol flinkCol = cols.get(idx);
//                dataTypes[idx] =
//                        flinkCol.flinkCDCPipelineEventProcess.getDataType();
//            }
//            return new BinaryRecordDataGenerator(dataTypes);
//        }));
    }

    @Override
    protected void fillRowVals(DTO dto, DataChangeEvent row) {

    }

    private BinaryRecordDataGenerator getRecordDataGenerator(TableId tableId) {
        BinaryRecordDataGenerator recordDataGenerator = null;
        if (tab2GenMapper == null) {
            synchronized (this) {
                if (tab2GenMapper == null) {
                    tab2GenMapper = Maps.newHashMap();
                }
            }
        }
        if ((recordDataGenerator = tab2GenMapper.get(tableId.getTableName())) == null) {
            List<FlinkCol> cols = sourceColsMetaMapper.get(tableId.getTableName());
            if (CollectionUtils.isEmpty(cols)) {
                throw new IllegalStateException("tableId:" + tableId + " relevant flinkCols can not be null");
            }
            org.apache.flink.cdc.common.types.DataType[] dataTypes = new org.apache.flink.cdc.common.types.DataType[cols.size()];
            for (int idx = 0; idx < cols.size(); idx++) {
                FlinkCol flinkCol = cols.get(idx);
                dataTypes[idx] =
                        flinkCol.flinkCDCPipelineEventProcess.getDataType();
            }
            recordDataGenerator = new BinaryRecordDataGenerator(dataTypes);
            tab2GenMapper.put(tableId.getTableName(), recordDataGenerator);
        }
        return recordDataGenerator;
    }

    @Override
    protected DataChangeEvent createRowData(DTO dto) {
        DataChangeEvent changeEvent = null;

        TableId tableId = StringUtils.isEmpty(dto.getDbName())
                ? TableId.tableId(dto.getTableName())
                : TableId.tableId(dto.getDbName(), dto.getTableName());
        Map<String, Object> beforeVals = dto.getBefore();
        Map<String, Object> afterVals = dto.getAfter();

        final BinaryRecordDataGenerator recordDataGenerator = Objects.requireNonNull(
                tab2GenMapper.get(tableId.getTableName())
                , "table:" + tableId.getTableName() + " relevant recordDataGenerator can not be null");

        List<FlinkCol> sourceColsMeta = Objects.requireNonNull(sourceColsMetaMapper.get(tableId.getTableName())
                , "table:" + tableId.getTableName() + " relevant sourceColsMeta can not be null");

        EventType event = dto.getEventType();

        RecordData before = null;
        RecordData after = null;

        switch (event) {
            case UPDATE_BEFORE:
                return null;
            case UPDATE_AFTER: {
                before = createRecordData(recordDataGenerator, sourceColsMeta, beforeVals);
                after = createRecordData(recordDataGenerator, sourceColsMeta, afterVals);
                changeEvent = DataChangeEvent.updateEvent(tableId, before, after);
                break;
            }
            case DELETE:
                before = createRecordData(recordDataGenerator, sourceColsMeta, beforeVals);
                changeEvent = DataChangeEvent.deleteEvent(tableId, before);
                break;
            case ADD:
                before = createRecordData(recordDataGenerator, sourceColsMeta, beforeVals);
                changeEvent = DataChangeEvent.insertEvent(tableId, before);
                break;
            default:
                throw new IllegalStateException("illegal event type:" + event);
        }

        return changeEvent;
    }

    private RecordData createRecordData(BinaryRecordDataGenerator recordDataGenerator
            , List<FlinkCol> sourceColsMeta, Map<String, Object> vals) {
        Object[] rowFields = new Object[sourceColsMeta.size()];
        FlinkCol flinkCol = null;
        for (int idx = 0; idx < sourceColsMeta.size(); idx++) {
            flinkCol = sourceColsMeta.get(idx);
            rowFields[idx] = flinkCol.processVal(this.dtoConvert2Type, vals);
        }
        return recordDataGenerator.generate(rowFields);
    }
}

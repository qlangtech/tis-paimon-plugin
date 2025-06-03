package com.qlangtech.tis.plugins.incr.flink.connector;

import com.qlangtech.plugins.incr.flink.cdc.FlinkCol;
import com.qlangtech.tis.plugin.ds.CMeta;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.plugin.paimon.datax.PaimonSelectedTab;
import com.qlangtech.tis.sql.parser.tuple.creator.EntityName;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-05-31 15:03
 **/
public class SchemaEmitterFunction extends ProcessFunction<Event, Event> {
    private Set<TableId> alreadySendCreateTableTables = new HashSet<>();
    private Map<String, CreateTableEvent> createTableEventCache = new HashMap<>();
    // private List<ISelectedTab> tabs;

    /**
     * @param sinkDBName           sink端DB名称
     * @param tabs
     * @param sourceColsMetaMapper
     */
    public SchemaEmitterFunction(Optional<String> sinkDBName, List<ISelectedTab> tabs, Map<String /**tabName*/, List<FlinkCol>> sourceColsMetaMapper) {
        // this.tabs = tabs;
        TableId tableId = null;
        for (ISelectedTab tab : tabs) {
            EntityName entityName = tab.getEntityName();
            tableId = Objects.requireNonNull(sinkDBName, "sinkDBName can not be null").isEmpty()
                    ? TableId.tableId(entityName.getTableName())
                    : TableId.tableId(sinkDBName.get(), entityName.getTableName());
            createTableEventCache.put(tableId.getTableName()
                    , new CreateTableEvent(tableId
                            , parseDDL(tableId, (PaimonSelectedTab) tab
                            , Objects.requireNonNull(sourceColsMetaMapper.get(tableId.getTableName())
                                    , "table:" + tableId + " relevant flinkCols can not be null"))));
        }
    }

    private static Schema parseDDL(TableId tableId, PaimonSelectedTab tab, List<FlinkCol> cols) {
        // Table table = parseDdl(ddlStatement, tableId);

        //List<CMeta> columns = tab.getCols();
        Schema.Builder tableBuilder = Schema.newBuilder();
        DataType dataType = null;
        FlinkCol column = null;
        for (int i = 0; i < cols.size(); i++) {
            column = cols.get(i);
            dataType = column.flinkCDCPipelineEventProcess.getDataType();
            // String colName = column.name();
            tableBuilder.physicalColumn(
                    column.name,
                    dataType,
                    null,
                    null);
        }
        tableBuilder.comment(null);

        List<String> primaryKey = tab.getPrimaryKeys();
        if (Objects.nonNull(primaryKey) && !primaryKey.isEmpty()) {
            tableBuilder.primaryKey(primaryKey);
        }
        List<String> pts = tab.partitionPathFields;
        if (CollectionUtils.isNotEmpty(pts)) {
            tableBuilder.partitionKey(pts);
        }
        return tableBuilder.build();
    }

    @Override
    public void processElement(Event e, ProcessFunction<Event, Event>.Context ctx, Collector<Event> out) throws Exception {
        DataChangeEvent event = (DataChangeEvent) e;
        TableId tableId = event.tableId();
        if (!alreadySendCreateTableTables.contains(tableId)) {
            out.collect(Objects.requireNonNull(createTableEventCache.get(tableId.getTableName())
                    , "tableId:" + tableId + " relevant createTableEvent can not be null"));
            alreadySendCreateTableTables.add(tableId);
        }

        out.collect(e);
    }
}

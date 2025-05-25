package com.qlangtech.tis.plugins.incr.flink.connector;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.qlangtech.plugins.incr.flink.cdc.FlinkCol;
import com.qlangtech.tis.async.message.client.consumer.IFlinkColCreator;
import com.qlangtech.tis.async.message.client.consumer.impl.MQListenerFactory;
import com.qlangtech.tis.datax.IDataXNameAware;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.IDataxReader;
import com.qlangtech.tis.datax.TableAlias;
import com.qlangtech.tis.datax.TableAliasMapper;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.SelectedTab;
import com.qlangtech.tis.plugin.datax.common.BasicDataXRdbmsWriter;
import com.qlangtech.tis.plugin.ds.BasicDataSourceFactory;
import com.qlangtech.tis.plugin.ds.DBConfig;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.plugin.ds.JDBCConnection;
import com.qlangtech.tis.plugin.incr.TISSinkFactory;
import com.qlangtech.tis.plugin.paimon.datax.DataxPaimonWriter;
import com.qlangtech.tis.plugins.incr.flink.cdc.AbstractRowDataMapper;
import com.qlangtech.tis.realtime.BasicTISSinkFactory;
import com.qlangtech.tis.realtime.SelectedTableTransformerRules;
import com.qlangtech.tis.realtime.TabSinkFunc;
import com.qlangtech.tis.realtime.transfer.DTO.EventType;
import com.qlangtech.tis.util.HeteroEnum;
import com.qlangtech.tis.util.IPluginContext;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.data.RowData;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * 支持flink-cdc pipeline sink
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-05-22 09:01
 **/
public abstract class PipelineFlinkCDCSinkFactory
        extends BasicTISSinkFactory<Event> implements IDataXNameAware {

    public static final String DISPLAY_NAME_FLINK_PIPELINE_SINK = "FlinkCDC-Pipeline-Sink-";
    @FormField(ordinal = 12, type = FormFieldType.INT_NUMBER, validate = {Validator.require})
    public Integer parallelism;

    @Override
    public boolean flinkCDCPipelineEnable() {
        return true;
    }

    @Override
    public Map<TableAlias, TabSinkFunc<?, ?, Event>> createSinkFunction(
            IDataxProcessor dataxProcessor, IFlinkColCreator flinkColCreator) {

        Map<TableAlias, TabSinkFunc<?, ?, Event>> sinkFuncs = Maps.newHashMap();


        TableAliasMapper selectedTabs = dataxProcessor.getTabAlias(null);
        if (selectedTabs.isNull()) {
            throw new IllegalStateException("selectedTabs can not be empty");
        }
        // IDataxReader reader = dataxProcessor.getReader(null);
        // List<ISelectedTab> tabs = reader.getSelectedTabs();

        // 清空一下tabs的缓存以免有脏数据
        //  this.selTabs = null;

        selectedTabs.forEach((key, val/*TableAlias*/) -> {

            Objects.requireNonNull(val, "tableName can not be null");
            if (StringUtils.isEmpty(val.getFrom())) {
                throw new IllegalStateException("tableName.getFrom() can not be empty");
            }
            final TableAlias tabName = val;

            sinkFuncs.put(val, createRowDataSinkFunc(dataxProcessor, tabName, true));
        });

        if (sinkFuncs.size() < 1) {
            throw new IllegalStateException("size of sinkFuncs can not be small than 1");
        }
        return sinkFuncs;
    }


    private PipelineEventSinkFunc createRowDataSinkFunc(IDataxProcessor dataxProcessor
            , final TableAlias tabName, TableAliasMapper selectedTabs, boolean shallInitSinkTable) {
        String pipelineName = dataxProcessor.identityValue();
        IDataxReader reader = dataxProcessor.getReader(null);
        List<ISelectedTab> tabs = reader.getSelectedTabs();

        Optional<ISelectedTab> selectedTab = tabs.stream()
                .filter((tab) -> StringUtils.equals(tabName.getFrom(), tab.getName())).findFirst();
        if (!selectedTab.isPresent()) {
            throw new IllegalStateException("target table:" + tabName.getFrom()
                    + " can not find matched table in:["
                    + tabs.stream().map((t) -> t.getName()).collect(Collectors.joining(",")) + "]");
        }
        final SelectedTab tab = (SelectedTab) selectedTab.get();
        final CreateSinkFunctionResult sinkFunc
                = createSinFunctionResult(dataxProcessor
                , tab, tabName.getTo(), shallInitSinkTable);

        if (this.parallelism == null) {
            throw new IllegalStateException("param parallelism can not be null");
        }

        MQListenerFactory sourceListenerFactory = HeteroEnum.getIncrSourceListenerFactory(dataxProcessor.getDataXName());
        IFlinkColCreator<FlinkCol> sourceFlinkColCreator
                = Objects.requireNonNull(sourceListenerFactory, "sourceListenerFactory").createFlinkColCreator(reader);
        // List<EventType> filterRowKinds = sourceListenerFactory.getFilterRowKinds();


//        TableAlias tab, List<String> primaryKeys
//                , SinkFunction<Event> sinkFunction, List<FlinkCol> sourceColsMeta
//                , List<FlinkCol> sinkColsMeta, int sinkTaskParallelism
//            , Optional<SelectedTableTransformerRules> transformerOpt

        return new PipelineEventSinkFunc(pipelineName, tabName
                , sinkFunc.primaryKeys
                , sinkFunc.getSinkFunction()
                , IPluginContext.namedContext(dataxProcessor.identityValue())
                , tab
                , sourceFlinkColCreator
                , AbstractRowDataMapper.getAllTabColsMeta(
                Objects.requireNonNull(sinkFunc.tableCols, "tabCols can not be null").getCols())
                // , supportUpsetDML()
                // , filterRowKinds
                , this.parallelism
                , RowDataSinkFunc.createTransformerRules(dataxProcessor.identityValue()
                , tabName
                , tab
                , Objects.requireNonNull(sourceFlinkColCreator, "sourceFlinkColCreator can not be null")));
    }

    protected CreateSinkFunctionResult createSinFunctionResult(
            IDataxProcessor dataxProcessor, SelectedTab selectedTab, final String targetTabName, boolean shallInitSinkTable) {

        AtomicReference<Object[]> exceptionLoader = new AtomicReference<>();
        // AtomicReference<CreateChunjunSinkFunctionResult> sinkFuncRef = new AtomicReference<>();
        DataxPaimonWriter dataXWriter = (DataxPaimonWriter) dataxProcessor.getWriter(null);
        // BasicDataSourceFactory dsFactory = (BasicDataSourceFactory) dataXWriter.getDataSourceFactory();
//        if (dsFactory == null) {
//            throw new IllegalStateException("dsFactory can not be null");
//        }
        TableAlias tableAlias = TableAlias.create(selectedTab.getName(), targetTabName);
        // DBConfig dbConfig = dsFactory.getDbConfig();
        dbConfig.vistDbURL(false, (dbName, dbHost, jdbcUrl) -> {
            try {
                if (shallInitSinkTable) {
                    /**
                     * 需要先初始化表MySQL目标库中的表
                     */
                    dataXWriter.initWriterTable(selectedTab, targetTabName, Collections.singletonList(jdbcUrl));
                }

// FIXME 这里不能用 MySQLSelectedTab
                sinkFuncRef.set(createSinkFunction(dbName, tableAlias, selectedTab, jdbcUrl, dsFactory, dataXWriter));

            } catch (Throwable e) {
                exceptionLoader.set(new Object[]{jdbcUrl, e});
            }
        });

        if (exceptionLoader.get() != null) {
            Object[] error = exceptionLoader.get();
            throw new RuntimeException((String) error[0], (Throwable) error[1]);
        }
        Objects.requireNonNull(sinkFuncRef.get(), "sinkFunc can not be null");
        sinkFuncRef.get().parallelism = this.parallelism;
        return sinkFuncRef.get();
    }

    private CreateSinkFunctionResult createSinkFunction(
            String dbName, final TableAlias tabAlias, SelectedTab tab, String jdbcUrl
            , BasicDataSourceFactory dsFactory, BasicDataXRdbmsWriter dataXWriter) {


        CreateSinkFunctionResult sinkFunc
                = createChunjunSinkFunction(jdbcUrl, tabAlias, tab.getPrimaryKeys(), dsFactory, dataXWriter);
        return sinkFunc;
    }

    private CreateSinkFunctionResult createChunjunSinkFunction(
            String jdbcUrl, TableAlias tableAlias, List<String> primaryKeys, BasicDataSourceFactory dsFactory
            , BasicDataXRdbmsWriter dataXWriter) {
        CreateSinkFunctionResult sinkFactory = createSinkFactory(jdbcUrl, tableAlias, primaryKeys, dsFactory, dataXWriter);
        sinkFactory.initialize();
        return Objects.requireNonNull(sinkFactory, "create result can not be null");
    }

    protected CreateSinkFunctionResult createSinkFactory(String jdbcUrl, TableAlias tableAlias
            , List<String> primaryKeys, BasicDataSourceFactory dsFactory
            , BasicDataXRdbmsWriter dataXWriter) {
        final CreateSinkFunctionResult createResult = new CreateSinkFunctionResult();
        if (CollectionUtils.isEmpty(primaryKeys)) {
            throw new IllegalArgumentException("primaryKeys can not be empty");
        }
        createResult.primaryKeys = primaryKeys;

        createResult.setSinkFactory(new JdbcSinkFactory(syncConf, createJdbcDialect(syncConf)) {
            @Override
            public void initCommonConf(ChunJunCommonConf commonConf) {
                super.initCommonConf(commonConf);
                initChunjunJdbcConf(this.jdbcConf);
            }

            @Override
            protected JdbcOutputFormatBuilder getBuilder() {
                return new JdbcOutputFormatBuilder(createChunjunOutputFormat(tableAlias, dataXWriter.getDataSourceFactory(), this.jdbcConf));
            }

            @Override
            protected DataStreamSink<RowData> createOutput(
                    DataStream<RowData> dataSet, OutputFormat<RowData> outputFormat) {
                JdbcOutputFormat routputFormat = (JdbcOutputFormat) outputFormat;

                try (JDBCConnection conn = dsFactory.getConnection(jdbcUrl, Optional.empty(), false)) {
                    routputFormat.dbConn = conn.getConnection();
                    routputFormat.initColumnList();
                } catch (SQLException e) {
                    throw new RuntimeException("jdbcUrl:" + jdbcUrl, e);
                }
                TableCols tableCols = new TableCols(routputFormat.colsMeta);

//                JdbcColumnConverter rowConverter = (JdbcColumnConverter)
//                        DialectUtils.createColumnConverter(jdbcDialect, jdbcConf, tableCols.filterBy(jdbcConf.getColumn()));

                JdbcColumnConverter rowConverter = (JdbcColumnConverter)
                        DialectUtils.createColumnConverter(jdbcDialect, jdbcConf, tableCols.getCols());


                DtOutputFormatSinkFunction<RowData> sinkFunction =
                        new DtOutputFormatSinkFunction<>(outputFormat);
                createResult.setSinkCols(tableCols);
                createResult.setColumnConverter(rowConverter);
                createResult.setSinkFunction(sinkFunction);
                createResult.setOutputFormat(routputFormat);
                //   ref.set(Triple.of(sinkFunction, rowConverter, routputFormat));
                return null;
            }
        });


        return createResult;
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

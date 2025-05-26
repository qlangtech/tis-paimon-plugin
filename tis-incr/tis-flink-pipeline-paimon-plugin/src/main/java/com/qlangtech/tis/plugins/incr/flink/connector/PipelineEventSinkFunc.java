//package com.qlangtech.tis.plugins.incr.flink.connector;
//
//import com.google.common.collect.Maps;
//import com.qlangtech.plugins.incr.flink.cdc.FlinkCol;
//import com.qlangtech.tis.async.message.client.consumer.IFlinkColCreator;
//import com.qlangtech.tis.config.hive.IHiveConnGetter;
//import com.qlangtech.tis.datax.IDataxProcessor;
//import com.qlangtech.tis.plugin.ds.ISelectedTab;
//import com.qlangtech.tis.plugin.paimon.catalog.FileSystemCatalog;
//import com.qlangtech.tis.plugin.paimon.catalog.HiveCatalog;
//import com.qlangtech.tis.plugin.paimon.catalog.PaimonCatalog;
//import com.qlangtech.tis.plugin.paimon.catalog.PaimonCatalogVisitor;
//import com.qlangtech.tis.plugin.paimon.datax.DataxPaimonWriter;
//import com.qlangtech.tis.plugin.paimon.datax.PaimonSelectedTab;
//import com.qlangtech.tis.plugins.incr.flink.pipeline.paimon.sink.PaimonPipelineSinkFactory;
//import com.qlangtech.tis.realtime.BasicTISSinkFactory.RowDataSinkFunc;
//import com.qlangtech.tis.realtime.SelectedTableTransformerRules;
//import com.qlangtech.tis.realtime.TabSinkFunc;
//import com.qlangtech.tis.realtime.dto.DTOStream;
//import com.qlangtech.tis.realtime.transfer.DTO;
//import org.apache.commons.lang3.StringUtils;
//import org.apache.flink.api.common.typeinfo.TypeInformation;
//import org.apache.flink.api.connector.sink2.Sink;
//import org.apache.flink.cdc.common.configuration.Configuration;
//import org.apache.flink.cdc.common.event.Event;
//import org.apache.flink.cdc.common.event.TableId;
//import org.apache.flink.cdc.common.factories.Factory.Context;
//import org.apache.flink.cdc.common.factories.FactoryHelper;
//import org.apache.flink.cdc.common.sink.DataSink;
//import org.apache.flink.cdc.common.utils.Preconditions;
//import org.apache.flink.cdc.composer.definition.SinkDef;
//import org.apache.flink.cdc.composer.flink.translator.DataSinkTranslator;
//import org.apache.flink.cdc.connectors.paimon.sink.PaimonDataSink;
//import org.apache.flink.cdc.connectors.paimon.sink.PaimonDataSinkFactory;
//import org.apache.flink.cdc.connectors.paimon.sink.PaimonDataSinkOptions;
//import org.apache.flink.cdc.connectors.paimon.sink.v2.PaimonRecordEventSerializer;
//import org.apache.flink.cdc.connectors.paimon.sink.v2.PaimonRecordSerializer;
//import org.apache.flink.runtime.jobgraph.OperatorID;
//import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;
//import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap.Builder;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.table.data.RowData;
//import org.apache.flink.table.types.logical.LogicalType;
//import org.apache.paimon.catalog.Catalog;
//import org.apache.paimon.options.Options;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.time.ZoneId;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//import java.util.Objects;
//import java.util.Optional;
//
///**
// * 支持flink-cdc pipeline event sink
// *
// * @author: 百岁（baisui@qlangtech.com）
// * @create: 2025-05-22 10:15
// **/
//public class PipelineEventSinkFunc extends TabSinkFunc<Sink<Event>, Void, Event> {
//
//    private static final Logger logger = LoggerFactory.getLogger(PipelineEventSinkFunc.class);
//    private final IDataxProcessor dataxProcessor;
//    private final DataxPaimonWriter paimonWriter;
//    private final PaimonPipelineSinkFactory pipelineSinkFactory;
//    private final List<ISelectedTab> tabs;
//
//    protected final Map<String /**tabName*/, List<FlinkCol>> sourceColsMetaMapper;
//
//    public PipelineEventSinkFunc(IDataxProcessor dataxProcessor
//            , PaimonPipelineSinkFactory pipelineSinkFactory //
//            , List<ISelectedTab> tabs //
//            , IFlinkColCreator<FlinkCol> sourceFlinkColCreator
//            , Sink<Event> sinkFunction //
//            , int sinkTaskParallelism) {
//        super(sinkFunction, sinkTaskParallelism);
//        this.dataxProcessor = dataxProcessor;
//        this.paimonWriter = Objects.requireNonNull(
//                (DataxPaimonWriter) dataxProcessor.getWriter(null), "paimonWriter can not be null");
//        this.pipelineSinkFactory = Objects.requireNonNull(pipelineSinkFactory, "pipelineSinkFactory can not be null");
//        this.tabs = Objects.requireNonNull(tabs, "tabs can not be null");
//
//        Builder<String /**tabName*/, List<FlinkCol>> sourceColsMetaMapperBuilder = ImmutableMap.builder();
//        Optional<SelectedTableTransformerRules> transformerOpt = null;
//        for (ISelectedTab selTab : this.tabs) {
//
//            transformerOpt = RowDataSinkFunc.createTransformerRules(dataxProcessor.identityValue()
//                    , selTab
//                    , Objects.requireNonNull(sourceFlinkColCreator, "sourceFlinkColCreator can not be null"));
//
//            sourceColsMetaMapperBuilder.put(selTab.getName()
//                    , FlinkCol.createSourceCols(null, selTab, sourceFlinkColCreator, transformerOpt));
//        }
//        sourceColsMetaMapper = sourceColsMetaMapperBuilder.build();
//    }
//
//    @Override
//    protected DataStream<Event> streamMap(DTOStream sourceStream) {
//        DataStream<Event> result = null;
//        if (sourceStream.clazz == DTO.class) {
//            result = sourceStream.getStream().map(new DTO2FlinkPipelineEventMapper(this.sourceColsMetaMapper))
//                    .name(dataxProcessor.identityValue() + "_dto2Event")
//                    .setParallelism(this.sinkTaskParallelism);
//        } else if (sourceStream.clazz == org.apache.flink.cdc.common.event.Event.class) {
//            result = sourceStream.getStream();
//        } else if (sourceStream.clazz == RowData.class) {
//            // 当chunjun作为source时
//            logger.info("create stream directly, source type is RowData");
//            // result = sourceStream.getStream();
//            throw new UnsupportedOperationException("dataSource is create by chunjun is disabled");
//        }
//        if (result == null) {
//            throw new IllegalStateException("not illegal source Stream class:" + sourceStream.clazz);
//        }
//
//        return result;
////        if (transformers.isPresent()) {
////            SelectedTableTransformerRules triple = transformers.get();
////            List<FlinkCol> transformerColsWithoutContextParamsFlinkCol = triple.transformerColsWithoutContextParamsFlinkCol();
////            LogicalType[] fieldDataTypes
////                    = transformerColsWithoutContextParamsFlinkCol
////                    .stream().map((colmeta) -> colmeta.type.getLogicalType()).toArray(LogicalType[]::new);
////
////            String[] colNames = transformerColsWithoutContextParamsFlinkCol
////                    .stream().map((colmeta) -> colmeta.name).toArray(String[]::new);
////            TypeInformation<Event> outputType = null; //InternalTypeInfo.of(RowType.of(fieldDataTypes, colNames));
////            // = (TypeInformation<RowData>) TypeConversions.fromDataTypeToLegacyInfo(TypeConversions.fromLogicalToDataType(rowType));
////
////            logger.info("transformerColsWithoutContextParamsFlinkCol size:{},colNames:{}"
////                    , transformerColsWithoutContextParamsFlinkCol.size(), String.join(",", colNames));
////            return result.map(new PipelineEventTransformerMapper(triple, transformerColsWithoutContextParamsFlinkCol.size()), outputType)
////                    .name(tab.getFrom() + "_transformer").setParallelism(this.sinkTaskParallelism);
////        } else {
////            return result;
////        }
//    }
//
//    @Override
//    protected Void addSinkToSource(DataStream<Event> sourceStream) {
//        DataSinkTranslator sinkTranslator = new DataSinkTranslator();
//        Configuration configuration = Configuration.fromMap(Maps.newHashMap());
//        SinkDef sinkDef = new SinkDef(dataxProcessor.identityValue(), null, configuration);
//
//        //  PaimonDataSinkFactory paimonDataSinkFactory = new PaimonDataSinkFactory();
//        Context dataSinkContext = createDataSinkContext();
//        DataSink dataSink = createDataSink(dataSinkContext);
//
//        OperatorID schemaOperatorID = OperatorID.fromJobVertexID(null);
//        sinkTranslator.translate(sinkDef, sourceStream, dataSink, schemaOperatorID);
//        return null;
//    }
//
//
//    /**
//     * @param context
//     * @return
//     * @see PaimonDataSinkFactory#createDataSink
//     */
//    private DataSink createDataSink(Context context) {
//        Map<String, String> allOptions = context.getFactoryConfiguration().toMap();
//        Map<String, String> catalogOptions = new HashMap<>();
//        Map<String, String> tableOptions = new HashMap<>();
//        allOptions.forEach(
//                (key, value) -> {
//                    if (key.startsWith(PaimonDataSinkOptions.PREFIX_TABLE_PROPERTIES)) {
//                        tableOptions.put(
//                                key.substring(
//                                        PaimonDataSinkOptions.PREFIX_TABLE_PROPERTIES.length()),
//                                value);
//                    } else if (key.startsWith(PaimonDataSinkOptions.PREFIX_CATALOG_PROPERTIES)) {
//                        catalogOptions.put(
//                                key.substring(
//                                        PaimonDataSinkOptions.PREFIX_CATALOG_PROPERTIES.length()),
//                                value);
//                    }
//                });
//        Options options = Options.fromMap(catalogOptions);
//        // Catalog catalog = this.paimonWriter.createCatalog();
//        try (Catalog catalog = this.paimonWriter.createCatalog()) {
//            // try (Catalog catalog = FlinkCatalogFactory.createPaimonCatalog(options)) {
//            Preconditions.checkNotNull(
//                    catalog.listDatabases(), "catalog option of Paimon is invalid.");
//        } catch (Exception e) {
//            throw new RuntimeException("failed to create or use paimon catalog", e);
//        }
//        ZoneId zoneId = pipelineSinkFactory.getTimeZone();// ZoneId.systemDefault();
//
////        if (!Objects.equals(
////                context.getPipelineConfiguration().get(PipelineOptions.PIPELINE_LOCAL_TIME_ZONE),
////                PipelineOptions.PIPELINE_LOCAL_TIME_ZONE.defaultValue())) {
////            zoneId =
////                    ZoneId.of(
////                            context.getPipelineConfiguration()
////                                    .get(PipelineOptions.PIPELINE_LOCAL_TIME_ZONE));
////        }
//
//        //   context.getFactoryConfiguration().get(PaimonDataSinkOptions.COMMIT_USER);
//        String commitUser = paimonWriter.catalog.tableOwner;
//        if (StringUtils.isEmpty(commitUser)) {
//            throw new IllegalStateException("commitUser can not be null");
//        }
////        String partitionKey =
////                context.getFactoryConfiguration().get(PaimonDataSinkOptions.PARTITION_KEY);
//        Map<TableId, List<String>> partitionMaps = new HashMap<>();
//
//        for (ISelectedTab selectedTab : this.tabs) {
//            PaimonSelectedTab paimonTab = (PaimonSelectedTab) selectedTab;
//            TableId tableId = TableId.parse(paimonTab.getName());
//
//            partitionMaps.put(tableId, paimonTab.partitionPathFields);
//        }
//
////        if (!partitionKey.isEmpty()) {
////            for (String tables : partitionKey.split(";")) {
////                String[] splits = tables.split(":");
////                if (splits.length == 2) {
////                    TableId tableId = TableId.parse(splits[0]);
////                    List<String> partitions = Arrays.asList(splits[1].split(","));
////                    partitionMaps.put(tableId, partitions);
////                } else {
////                    throw new IllegalArgumentException(
////                            PaimonDataSinkOptions.PARTITION_KEY.key()
////                                    + " is malformed, please refer to the documents");
////                }
////            }
////        }
//        PaimonRecordSerializer<Event> serializer = new PaimonRecordEventSerializer(zoneId);
//        return new PaimonDataSink(options, tableOptions, commitUser, partitionMaps, serializer);
//    }
//
//    private Context createDataSinkContext() {
//        Configuration factoryConfiguration = null;
//        Configuration pipelineConfiguration = null;
//        Builder<String, String> paramBuilder = ImmutableMap.<String, String>builder();
//        DataxPaimonWriter writer = (DataxPaimonWriter) dataxProcessor.getWriter(null);
//        writer.catalog.accept(new PaimonCatalogVisitor<Void>() {
//            @Override
//            public Void visit(FileSystemCatalog catalog) {
//                setMetaStore("filesystem");
//                setWarehouse(catalog);
//                return null;
//            }
//
//
//            @Override
//            public Void visit(HiveCatalog catalog) {
//                setMetaStore("hive");
//                setWarehouse(catalog);
//                IHiveConnGetter hiveConnGetter = catalog.getHiveConnGetter();
//                paramBuilder.put(PaimonDataSinkOptions.URI.key(), hiveConnGetter.getMetaStoreUrls());
//                return null;
//            }
//
//            private void setWarehouse(PaimonCatalog catalog) {
//                paramBuilder.put(PaimonDataSinkOptions.WAREHOUSE.key(), catalog.catalogPath);
//            }
//
//            private void setMetaStore(String metaStore) {
//                paramBuilder.put(PaimonDataSinkOptions.METASTORE.key(), metaStore);
//            }
//        });
//        factoryConfiguration = pipelineConfiguration = Configuration.fromMap(paramBuilder.build());
//        ClassLoader classLoader = PipelineEventSinkFunc.class.getClassLoader();
//        return new FactoryHelper.DefaultContext(factoryConfiguration, pipelineConfiguration, classLoader);
//    }
//}

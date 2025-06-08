package com.qlangtech.tis.plugins.incr.flink.connector;

import com.google.common.collect.Maps;
import com.qlangtech.plugins.incr.flink.cdc.FlinkCol;
import com.qlangtech.tis.async.message.client.consumer.IFlinkColCreator;
import com.qlangtech.tis.config.hive.IHiveConnGetter;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.StoreResourceType;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.plugin.paimon.catalog.FileSystemCatalog;
import com.qlangtech.tis.plugin.paimon.catalog.HiveCatalog;
import com.qlangtech.tis.plugin.paimon.catalog.PaimonCatalog;
import com.qlangtech.tis.plugin.paimon.catalog.PaimonCatalogVisitor;
import com.qlangtech.tis.plugin.paimon.datax.DataxPaimonWriter;
import com.qlangtech.tis.plugin.paimon.datax.PaimonSelectedTab;
import com.qlangtech.tis.plugins.incr.flink.pipeline.paimon.sink.PaimonPipelineSinkFactory;
import com.qlangtech.tis.realtime.SelectedTableTransformerRules;
import com.qlangtech.tis.realtime.TabSinkFunc;
import com.qlangtech.tis.realtime.dto.DTOStream;
import com.qlangtech.tis.realtime.transfer.DTO;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.factories.Factory.Context;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.common.pipeline.PipelineOptions;
import org.apache.flink.cdc.common.sink.DataSink;
import org.apache.flink.cdc.common.source.DataSource;
import org.apache.flink.cdc.common.source.EventSourceProvider;
import org.apache.flink.cdc.common.source.MetadataAccessor;
import org.apache.flink.cdc.composer.definition.PipelineDef;
import org.apache.flink.cdc.composer.definition.SinkDef;
import org.apache.flink.cdc.composer.definition.SourceDef;
import org.apache.flink.cdc.composer.flink.FlinkPipelineComposer;
import org.apache.flink.cdc.composer.flink.translator.DataSinkTranslator;
import org.apache.flink.cdc.connectors.paimon.sink.PaimonDataSink;
import org.apache.flink.cdc.connectors.paimon.sink.PaimonDataSinkFactory;
import org.apache.flink.cdc.connectors.paimon.sink.PaimonDataSinkOptions;
import org.apache.flink.cdc.connectors.paimon.sink.v2.PaimonRecordEventSerializer;
import org.apache.flink.cdc.connectors.paimon.sink.v2.PaimonRecordSerializer;
import org.apache.flink.cdc.runtime.typeutils.EventTypeInfo;
import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;
import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap.Builder;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneId;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * 支持flink-cdc pipeline event sink
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-05-22 10:15
 **/
public class PipelineEventSinkFunc extends TabSinkFunc<Sink<Event>, Void, Event> {

    private static final Logger logger = LoggerFactory.getLogger(PipelineEventSinkFunc.class);
    private final IDataxProcessor dataxProcessor;
    private final DataxPaimonWriter paimonWriter;
    private final PaimonPipelineSinkFactory pipelineSinkFactory;
    private final List<ISelectedTab> tabs;

    protected final Map<String /**tabName*/, List<FlinkCol>> sourceColsMetaMapper;
    private final Optional<String> sinkDBName;

    public PipelineEventSinkFunc(IDataxProcessor dataxProcessor
            , PaimonPipelineSinkFactory pipelineSinkFactory //
            , List<ISelectedTab> tabs //
            , IFlinkColCreator<FlinkCol> sourceFlinkColCreator
            , Sink<Event> sinkFunction //
            , int sinkTaskParallelism) {
        super(sinkFunction, sinkTaskParallelism);
        this.dataxProcessor = dataxProcessor;
        this.paimonWriter = Objects.requireNonNull(
                (DataxPaimonWriter) dataxProcessor.getWriter(null), "paimonWriter can not be null");
        this.pipelineSinkFactory = Objects.requireNonNull(pipelineSinkFactory, "pipelineSinkFactory can not be null");
        this.tabs = Objects.requireNonNull(tabs, "tabs can not be null");
        this.sinkDBName = paimonWriter.catalog.getDBName();
        Builder<String /**tabName*/, List<FlinkCol>> sourceColsMetaMapperBuilder = ImmutableMap.builder();
        Optional<SelectedTableTransformerRules> transformerOpt = null;
        for (ISelectedTab selTab : this.tabs) {

            transformerOpt = SelectedTableTransformerRules.createTransformerRules(dataxProcessor.identityValue()
                    , selTab
                    , Objects.requireNonNull(sourceFlinkColCreator, "sourceFlinkColCreator can not be null"));

            sourceColsMetaMapperBuilder.put(selTab.getName()
                    , FlinkCol.createSourceCols(null, selTab, sourceFlinkColCreator, transformerOpt));
        }
        sourceColsMetaMapper = sourceColsMetaMapperBuilder.build();
    }

    @Override
    protected DataStream<Event> streamMap(DTOStream sourceStream) {
        DataStream<Event> result = null;
        if (sourceStream.clazz == DTO.class) {
            TypeInformation<Event> outputType = new EventTypeInfo();
            // Optional<String> dbName = paimonWriter.catalog.getDBName();
            SingleOutputStreamOperator<Event> outputOperator
                    = sourceStream.getStream().map(
                            new DTO2FlinkPipelineEventMapper(sinkDBName, this.sourceColsMetaMapper), outputType)
                    .name(dataxProcessor.identityValue() + "_dto2Event")
                    .setParallelism(this.sinkTaskParallelism);

            result = outputOperator.process(new SchemaEmitterFunction(sinkDBName, this.tabs, this.sourceColsMetaMapper), outputType)
                    .name("schema_emitter");
            // outputOperator;
        } else if (sourceStream.clazz == org.apache.flink.cdc.common.event.Event.class) {
            result = sourceStream.getStream();
        } else if (sourceStream.clazz == RowData.class) {
            // 当chunjun作为source时
            logger.info("create stream directly, source type is RowData");
            // result = sourceStream.getStream();
            throw new UnsupportedOperationException("dataSource is create by chunjun is disabled");
        }
        if (result == null) {
            throw new IllegalStateException("not illegal source Stream class:" + sourceStream.clazz);
        }

        return result;
    }

    /**
     * @param sourceStream
     * @return
     * @see org.apache.flink.cdc.composer.flink.FlinkPipelineComposer# translate
     */
    @Override
    protected Void addSinkToSource(DataStream<Event> sourceStream) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        FlinkPipelineComposer flinkPipelineComposer = FlinkPipelineComposer.ofApplicationCluster(env);
        DataSinkTranslator sinkTranslator = new DataSinkTranslator();
        Context dataSinkContext = createDataSinkContext();
        DataSink dataSink = createDataSink(dataSinkContext);
        PipelineDef pipelineDef = createPipelineDef();
        // StreamExecutionEnvironment env, DataSource dataSource, DataSink dataSink, DataSinkTranslator sinkTranslator, DataStream<Event> stream, PipelineDef pipelineDef
        DataSource dataSource = this.createDataSource();
        flinkPipelineComposer.translate(env, dataSource, dataSink, sinkTranslator, sourceStream, pipelineDef);


        // FIXME create from sinkFactory
//        Configuration configuration = Configuration.fromMap(Maps.newHashMap());
//        SinkDef sinkDef = new SinkDef(dataxProcessor.identityValue(), null, configuration);

        //  PaimonDataSinkFactory paimonDataSinkFactory = new PaimonDataSinkFactory();

//        SchemaChangeBehavior schemaChangeBehavior =
//                configuration.get(PipelineOptions.PIPELINE_SCHEMA_CHANGE_BEHAVIOR);
//
//        SchemaOperatorTranslator schemaOperatorTranslator =
//                new SchemaOperatorTranslator(
//                        schemaChangeBehavior,
//                        configuration.get(PipelineOptions.PIPELINE_SCHEMA_OPERATOR_UID),
//                        configuration.get(PipelineOptions.PIPELINE_SCHEMA_OPERATOR_RPC_TIMEOUT),
//                        configuration.get(PipelineOptions.PIPELINE_LOCAL_TIME_ZONE));
//
//        OperatorIDGenerator schemaOperatorIDGenerator =
//                new OperatorIDGenerator(schemaOperatorTranslator.getSchemaOperatorUid());

//        OperatorID schemaOperatorID = schemaOperatorIDGenerator.generate();
//        sinkTranslator.translate(sinkDef, sourceStream, dataSink, schemaOperatorID);
        return null;
    }

    private PipelineDef createPipelineDef() {
        Configuration configuration = Configuration.fromMap(Maps.newHashMap());
        SinkDef sinkDef = new SinkDef(dataxProcessor.identityValue(), null, configuration);
        SourceDef source = new SourceDef(null, null, null);
        // SourceDef source, SinkDef sink, List< RouteDef > routes, List< TransformDef > transforms, List< UdfDef > udfs, Configuration config
        return new PipelineDef(source, sinkDef, Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), configuration);
    }

    private DataSource createDataSource() {
        return new DataSource() {
            @Override
            public EventSourceProvider getEventSourceProvider() {
                return null;
            }

            @Override
            public MetadataAccessor getMetadataAccessor() {
                return null;
            }
        };
    }


    /**
     * @param context
     * @return
     * @see PaimonDataSinkFactory#createDataSink
     */
    private DataSink createDataSink(Context context) {
        Map<String, String> allOptions = context.getFactoryConfiguration().toMap();
        // Map<String, String> catalogOptions = new HashMap<>();
        final Map<String, String> tableOptions = createTabOpts();
      //  tableOptions.put(CoreOptions.BUCKET.key(), String.valueOf(this.paimonWriter.tableBucket));
        allOptions.forEach(
                (key, value) -> {
                    if (key.startsWith(PaimonDataSinkOptions.PREFIX_TABLE_PROPERTIES)) {
                        tableOptions.put(
                                key.substring(
                                        PaimonDataSinkOptions.PREFIX_TABLE_PROPERTIES.length()),
                                value);
                    } else if (key.startsWith(PaimonDataSinkOptions.PREFIX_CATALOG_PROPERTIES)) {
//                        catalogOptions.put(
//                                key.substring(
//                                        PaimonDataSinkOptions.PREFIX_CATALOG_PROPERTIES.length()),
//                                value);
                    }
                });
        // catalogOptions.put(StoreResourceType.DATAX_NAME, dataxProcessor.identityValue());
        Options options = this.paimonWriter.catalog.createOpts(dataxProcessor.identityValue());// Options.fromMap(catalogOptions);
        options.set(StoreResourceType.DATAX_NAME, dataxProcessor.identityValue());
        ZoneId zoneId = pipelineSinkFactory.getTimeZone();// ZoneId.systemDefault();

        String commitUser = paimonWriter.catalog.tableOwner;
        if (StringUtils.isEmpty(commitUser)) {
            throw new IllegalStateException("commitUser can not be null");
        }
        Map<TableId, List<String>> partitionMaps = new HashMap<>();

        for (ISelectedTab selectedTab : this.tabs) {
            PaimonSelectedTab paimonTab = (PaimonSelectedTab) selectedTab;
            TableId tableId = sinkDBName.isPresent() ? TableId.tableId(sinkDBName.get(), paimonTab.getName()) : TableId.tableId(paimonTab.getName());

            partitionMaps.put(tableId, paimonTab.partitionPathFields);
        }

//        if (!partitionKey.isEmpty()) {
//            for (String tables : partitionKey.split(";")) {
//                String[] splits = tables.split(":");
//                if (splits.length == 2) {
//                    TableId tableId = TableId.parse(splits[0]);
//                    List<String> partitions = Arrays.asList(splits[1].split(","));
//                    partitionMaps.put(tableId, partitions);
//                } else {
//                    throw new IllegalArgumentException(
//                            PaimonDataSinkOptions.PARTITION_KEY.key()
//                                    + " is malformed, please refer to the documents");
//                }
//            }
//        }
        PaimonRecordSerializer<Event> serializer = new PaimonRecordEventSerializer(zoneId);
        String schemaOperatorUid = (String) context.getPipelineConfiguration().get(PipelineOptions.PIPELINE_SCHEMA_OPERATOR_UID);
        return new PaimonDataSink(options, tableOptions, commitUser, partitionMaps, serializer, zoneId, schemaOperatorUid);
    }

    private Map<String, String> createTabOpts() {
        Schema.Builder schemaBuilder = new Schema.Builder();
        this.paimonWriter.initializeSchemaBuilder(schemaBuilder);
        Schema schema = schemaBuilder.build();
        Map<String, String> tableOptions = schema.options();
        return tableOptions;
    }

    private Context createDataSinkContext() {
        Configuration factoryConfiguration = null;
        Configuration pipelineConfiguration = null;
        Builder<String, String> paramBuilder = ImmutableMap.<String, String>builder();
        DataxPaimonWriter writer = (DataxPaimonWriter) dataxProcessor.getWriter(null);

        writer.catalog.accept(new PaimonCatalogVisitor<Void>() {
            @Override
            public Void visit(FileSystemCatalog catalog) {
                setMetaStore("filesystem");
                setWarehouse(catalog);
                return null;
            }


            @Override
            public Void visit(HiveCatalog catalog) {
                setMetaStore(HiveCatalog.HIVE_CATALOG_IDENTIFIER);
                setWarehouse(catalog);
                IHiveConnGetter hiveConnGetter = catalog.getHiveConnGetter();
                paramBuilder.put(PaimonDataSinkOptions.URI.key(), hiveConnGetter.getMetaStoreUrls());
                return null;
            }

            private void setWarehouse(PaimonCatalog catalog) {
                final String catalogPath = writer.catalog.getRootDir();
                paramBuilder.put(PaimonDataSinkOptions.WAREHOUSE.key(), catalogPath);
            }

            private void setMetaStore(String metaStore) {
                paramBuilder.put(PaimonDataSinkOptions.METASTORE.key(), metaStore);
            }
        });
        factoryConfiguration = pipelineConfiguration = Configuration.fromMap(paramBuilder.build());
        ClassLoader classLoader = PipelineEventSinkFunc.class.getClassLoader();
        return new FactoryHelper.DefaultContext(factoryConfiguration, pipelineConfiguration, classLoader);
    }
}

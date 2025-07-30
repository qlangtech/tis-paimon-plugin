package com.qlangtech.tis.plugin.paimon.datax;

import com.alibaba.citrus.turbine.Context;
import com.alibaba.datax.core.job.ISourceTable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.qlangtech.tis.TIS;
import com.qlangtech.tis.assemble.FullbuildPhase;
import com.qlangtech.tis.datax.IDataXBatchPost;
import com.qlangtech.tis.datax.IDataXGenerateCfgs;
import com.qlangtech.tis.datax.IDataxContext;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.IDataxProcessor.TableMap;
import com.qlangtech.tis.datax.StoreResourceType;
import com.qlangtech.tis.datax.impl.DataxWriter;
import com.qlangtech.tis.exec.ExecutePhaseRange;
import com.qlangtech.tis.exec.IExecChainContext;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.extension.impl.IOUtils;
import com.qlangtech.tis.extension.util.OverwriteProps;
import com.qlangtech.tis.fullbuild.indexbuild.IRemoteTaskPostTrigger;
import com.qlangtech.tis.fullbuild.indexbuild.IRemoteTaskPreviousTrigger;
import com.qlangtech.tis.fullbuild.taskflow.IFlatTableBuilderDescriptor;
import com.qlangtech.tis.manage.common.Option;
import com.qlangtech.tis.plugin.IEndTypeGetter.EndType;
import com.qlangtech.tis.plugin.KeyedPluginStore;
import com.qlangtech.tis.plugin.KeyedPluginStore.Key;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.HdfsWriterDescriptor;
import com.qlangtech.tis.plugin.datax.common.AutoCreateTable;
import com.qlangtech.tis.plugin.datax.common.AutoCreateTable.BasicDescriptor;
import com.qlangtech.tis.plugin.datax.transformer.RecordTransformerRules;
import com.qlangtech.tis.plugin.ds.IInitWriterTableExecutor;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.plugin.paimon.catalog.HiveCatalog;
import com.qlangtech.tis.plugin.paimon.catalog.PaimonCatalog;
import com.qlangtech.tis.plugin.paimon.datax.PaimonPropAssist.PaimonOptions;
import com.qlangtech.tis.plugin.paimon.datax.bucket.PaimonBucket;
import com.qlangtech.tis.plugin.paimon.datax.compact.PaimonCompaction;
import com.qlangtech.tis.plugin.paimon.datax.hook.PostExecutor;
import com.qlangtech.tis.plugin.paimon.datax.hook.PreExecutor;
import com.qlangtech.tis.plugin.paimon.datax.utils.PaimonSnapshot;
import com.qlangtech.tis.plugin.paimon.datax.utils.PaimonUtils;
import com.qlangtech.tis.plugin.paimon.datax.writebuffer.PaimonWriteBuffer;
import com.qlangtech.tis.plugin.paimon.datax.writemode.WriteMode;
import com.qlangtech.tis.plugin.paimon.datax.writemode.WriteMode.PaimonTableWriter;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import com.qlangtech.tis.sql.parser.tuple.creator.EntityName;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.hive.RetryingMetaStoreClientFactory;
import org.apache.paimon.hive.RetryingMetaStoreClientFactory.HiveMetastoreProxySupplier;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.Schema.Builder;
import org.apache.paimon.table.Table;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.paimon.CoreOptions.FILE_FORMAT_AVRO;
import static org.apache.paimon.CoreOptions.FILE_FORMAT_ORC;
import static org.apache.paimon.CoreOptions.FILE_FORMAT_PARQUET;

/**
 * paimon 批量数据写入
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-05-15 14:56
 **/
public class DataxPaimonWriter extends DataxWriter implements SchemaBuilderSetter, KeyedPluginStore.IPluginKeyAware, IDataXBatchPost, IInitWriterTableExecutor {

    static {
        try {

            final HiveMetastoreProxySupplier tisProxySupplier = new HiveMetastoreProxySupplier() {
                @Override
                public IMetaStoreClient get(Method getProxyMethod, Configuration conf, String clientClassName)
                        throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {
                    String pipelineName = conf.get(StoreResourceType.DATAX_NAME);
                    if (StringUtils.isEmpty(pipelineName)) {
                        throw new IllegalStateException("param " + StoreResourceType.DATAX_NAME + " can not be empty");
                    }
                    DataxPaimonWriter paimonWriter
                            = (DataxPaimonWriter) DataxWriter.load(null, StoreResourceType.DataApp, pipelineName, true);
                    return ((HiveCatalog) Objects.requireNonNull(paimonWriter, "paimonWriter can not be null").catalog)
                            .getHiveConnGetter().createMetaStoreClient().unwrapClient();
                }
            };

            ImmutableMap.Builder<Class<?>[], HiveMetastoreProxySupplier> proxySuppliersBuilder
                    = ImmutableMap.<Class<?>[], HiveMetastoreProxySupplier>builder()
                    // for hive 2.x
                    .put(
                            new Class<?>[]{
                                    HiveConf.class,
                                    HiveMetaHookLoader.class,
                                    ConcurrentHashMap.class,
                                    String.class,
                                    Boolean.TYPE
                            },
                            tisProxySupplier)
                    .put(
                            new Class<?>[]{
                                    HiveConf.class,
                                    Class[].class,
                                    Object[].class,
                                    ConcurrentHashMap.class,
                                    String.class
                            },
                            tisProxySupplier)
                    // for hive 3.x
                    .put(
                            new Class<?>[]{
                                    Configuration.class,
                                    HiveMetaHookLoader.class,
                                    ConcurrentHashMap.class,
                                    String.class,
                                    Boolean.TYPE
                            },
                            tisProxySupplier)
                    // for hive 3.x,
                    // and some metastore client classes providing constructors only for 2.x
                    .put(
                            new Class<?>[]{
                                    Configuration.class,
                                    Class[].class,
                                    Object[].class,
                                    ConcurrentHashMap.class,
                                    String.class
                            },
                            tisProxySupplier);

            setProxySuppliersToEmptyMap("PROXY_SUPPLIERS", proxySuppliersBuilder.build());

            setProxySuppliersToEmptyMap("PROXY_SUPPLIERS_SHADED", Collections.emptyMap());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void setProxySuppliersToEmptyMap(String fieldName, Map<Class<?>[], HiveMetastoreProxySupplier> proxySuppliers) throws Exception {
        // 1. 获取目标类
        Class<?> clazz = RetryingMetaStoreClientFactory.class;

        // 2. 获取 PROXY_SUPPLIERS 字段
        Field field = clazz.getDeclaredField(fieldName);

        // 3. 解除 private 访问限制
        field.setAccessible(true);

        // 4. 移除 final 修饰符 (关键步骤)
        Field modifiersField = Field.class.getDeclaredField("modifiers");
        modifiersField.setAccessible(true);
        int originalModifiers = field.getModifiers();
        modifiersField.setInt(field, originalModifiers & ~Modifier.FINAL);

        // 5. 设置新值为空 Map
        field.set(null, proxySuppliers);
    }

    @FormField(ordinal = 1, validate = {Validator.require})
    public PaimonCatalog catalog;

    @FormField(ordinal = 2, validate = {Validator.require})
    public PaimonWriteBuffer writeBuffer;

    @FormField(ordinal = 6, type = FormFieldType.ENUM, validate = {Validator.require})
    public String storeFormat;

    /**
     * https://paimon.apache.org/docs/master/maintenance/configurations/#coreoptions
     * Bucket number for file store.
     * It should either be equal to -1 (dynamic bucket mode), -2 (postpone bucket mode), or it must be greater than 0 (fixed bucket mode).
     */
    @FormField(ordinal = 7, validate = {Validator.require})
    public PaimonBucket tableBucket;


    @FormField(ordinal = 11, validate = {Validator.require})
    public WriteMode paimonWriteMode;

    @FormField(ordinal = 12, validate = {Validator.require})
    // 目标源中是否自动创建表，这样会方便不少
    public AutoCreateTable autoCreateTable;

    @Override
    public boolean isGenerateCreateDDLSwitchOff() {
        // 不用创建create table ddl，但是建表还是需要的
        return true;
    }

    @Override
    public AutoCreateTable getAutoCreateTableCanNotBeNull() {
        return Objects.requireNonNull(this.autoCreateTable, "autoCreateTable can not be null");
    }

    @Override
    public void initWriterTable(ISourceTable sourceTable, String sinkTargetTabName, List<String> jdbcUrls) throws Exception {
        throw new UnsupportedOperationException();
    }

    @FormField(ordinal = 13, advance = true, validate = {Validator.require})
    public ChangelogProducer changelog;

    public static List<? extends Descriptor> supportedTableCreator(List<? extends Descriptor> descs) {
        if (CollectionUtils.isEmpty(descs)) {
            // return Collections.emptyList();
            throw new IllegalStateException("supportedTableCreator can not be empty");
        }
        return descs.stream().filter((desc) -> {
                    EndType endType = ((BasicDescriptor) desc).getEndType();
                    return endType != null && endType == EndType.Paimon;
                }
        ).collect(Collectors.toList());
    }

    @FormField(ordinal = 16, validate = {Validator.require})
    public PaimonCompaction compaction;

    @FormField(ordinal = 18, advance = true, validate = {Validator.require})
    public PaimonSnapshot snapshot;


    @FormField(ordinal = 22, advance = false, type = FormFieldType.TEXTAREA, validate = {Validator.require})
    public String template;

    public String dataXName;

    @Override
    public void setKey(Key key) {
        this.dataXName = key.keyVal.getVal();
    }

    @Override
    public void initializeSchemaBuilder(Builder tabSchemaBuilder, PaimonSelectedTab tab) {

        DefaultDescriptor desc = (DefaultDescriptor) this.getDescriptor();
        desc.opts.setTarget((field, val) -> {
            tabSchemaBuilder.option(field.key(), String.valueOf(val));
        }, this);

        this.changelog.initializeSchemaBuilder(tabSchemaBuilder, tab);
        this.writeBuffer.initializeSchemaBuilder(tabSchemaBuilder, tab);
        this.catalog.initializeSchemaBuilder(tabSchemaBuilder, tab);
        this.compaction.initializeSchemaBuilder(tabSchemaBuilder, tab);
        this.snapshot.initializeSchemaBuilder(tabSchemaBuilder, tab);
        this.tableBucket.initializeSchemaBuilder(tabSchemaBuilder, tab);

        tab.partition.initializeSchemaBuilder(tabSchemaBuilder, tab);
        tab.sequenceField.initializeSchemaBuilder(tabSchemaBuilder, tab);
        tab.bucketField.initializeSchemaBuilder(tabSchemaBuilder, tab);
    }


    @Override
    public void startScanDependency() {
        this.catalog.startScanDependency();
    }

    public static String getDftTemplate() {
        return IOUtils.loadResourceFromClasspath(DataxPaimonWriter.class, "paimon-writer-tpl.json");
    }


    @Override
    public IDataxContext getSubTask(Optional<TableMap> tableMap, Optional<RecordTransformerRules> transformerRules) {
        if (!tableMap.isPresent()) {
            throw new IllegalArgumentException("param tableMap shall be present");
        }
        if (StringUtils.isBlank(this.dataXName)) {
            throw new IllegalStateException("param 'dataXName' can not be null");
        }
        return new PaimonFSDataXContext(this.catalog.getDBName().get(), tableMap.get(), this.dataXName, transformerRules);
    }


    public List<PaimonColumn> createPaimonCols(PaimonSelectedTab tab, Optional<RecordTransformerRules> transformerRules) {
        TableMap tabMapper = new TableMap(tab);
        return ((PaimonFSDataXContext) getSubTask(Optional.of(tabMapper), transformerRules)).getPaimonCols();
    }

    public Catalog createCatalog() {
        return this.catalog.createCatalog(this.dataXName);
    }


    public boolean executeIfTableNotExist(EntityName entity, Consumer<Catalog> process) {
        try (Catalog catalog = this.createCatalog()) {
            // 判断表是否存在
            if (!PaimonUtils.tableExists(catalog, entity.getDbName(), entity.getTabName()).getKey()) {
                process.accept(catalog);
                return false;
//                // 创建新表
//                Optional<RecordTransformerRules> transformerRules = getRecordTransformerRules();
//                List<PaimonColumn> paimonCols = tab.getPaimonCols(transformerRules);
//                final IDataxReader reader = execContext.getProcessor().getReader(null);
//                SourceColMetaGetter sourceColMetaGetter =
//                        paimonWriter.autoCreateTable.enabledColumnComment() ? reader.createSourceColMetaGetter() : SourceColMetaGetter.getNone();
//                this.createTable(catalog, this.entity.getDbName(), this.entity.getTabName(), paimonCols, tab, sourceColMetaGetter);
            } else {
                // 表已经存在
                return true;
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public PaimonTableWriter createWriter(Integer taskId, Table table) {
        return Objects.requireNonNull(this.paimonWriteMode).createWriter(Objects.requireNonNull(this.tableBucket), taskId, table);
    }

    public Function<PaimonSelectedTab, Map<String, String>> createTabOpts() {
        return (tab) -> {
            try {
                Builder schemaBuilder = new Builder();
                initializeSchemaBuilder(schemaBuilder, tab);
                Schema schema = schemaBuilder.build();
                Map<String, String> tableOptions = schema.options();
                return tableOptions;
            } catch (Exception e) {
                throw new RuntimeException("table:" + tab.getName(), e);
            }
        };
    }


    public class PaimonFSDataXContext implements IDataxContext {
        protected final IDataxProcessor.TableMap tabMap;
        private final String dataxName;
        private final String targetDataBaseName;
        private final List<String> primaryKeys;
        private final List<PaimonColumn> paimonCols;

        public PaimonFSDataXContext(String targetDataBaseName, TableMap tabMap
                , String dataxName, Optional<RecordTransformerRules> transformerRules) {
            this.tabMap = tabMap;
            this.dataxName = dataxName;
            PaimonSelectedTab sourceTab = (PaimonSelectedTab) tabMap.getSourceTab();
            this.paimonCols = sourceTab.getPaimonCols(tabMap, transformerRules);
            this.primaryKeys = tabMap.getPrimaryKeys();
            if (StringUtils.isEmpty(targetDataBaseName)) {
                throw new IllegalArgumentException("param targetDataBaseName can not be empty");
            }
            this.targetDataBaseName = targetDataBaseName;
        }

        public String getDatabaseName() {
            return this.targetDataBaseName;
        }

        public final String getSourceTableName() {
            return this.tabMap.getFrom();
        }

        public final String getTableName() {
            EntityName to = parseEntity(this.tabMap.getFrom());
            return to.getTableName();
//            String tabName = this.tabMap.getTo();
//            if (StringUtils.isBlank(tabName)) {
//                throw new IllegalStateException("tabName of tabMap can not be null ,tabMap:" + tabMap);
//            }
//            return tabName;
        }

        /**
         * @return
         * @see org.apache.paimon.types.DataTypes
         * @see org.apache.paimon.data.GenericRow
         */
        public List<PaimonColumn> getPaimonCols() {
            return this.paimonCols;
        }
    }

    @Override
    public EntityName parseEntity(ISelectedTab tab) {
        return parseEntity(tab.getName());
    }


    public EntityName parseEntity(String tabName) {
        if (StringUtils.isEmpty(tabName)) {
            throw new IllegalArgumentException("param tabName can not be null");
        }
        Optional<String> dbName = this.catalog.getDBName();
        return dbName.map((db) -> EntityName.create(db, this.autoCreateTable.appendTabPrefix(tabName)))
                .orElseThrow(() -> new IllegalStateException("catalog must have dbName"));
    }

    @Override
    public ExecutePhaseRange getPhaseRange() {
        return new ExecutePhaseRange(FullbuildPhase.FullDump, FullbuildPhase.JOIN);
    }

    @Override
    public IRemoteTaskPreviousTrigger createPreExecuteTask(IExecChainContext execContext, EntityName entity, ISelectedTab tab) {
        return new PreExecutor(execContext, this, entity, (PaimonSelectedTab) tab);
    }

    @Override
    public IRemoteTaskPostTrigger createPostTask(IExecChainContext execContext, EntityName entity, ISelectedTab tab, IDataXGenerateCfgs cfgFileNames) {
        return new PostExecutor(execContext, this, entity, (PaimonSelectedTab) tab);
    }

    @Override
    public String getTemplate() {
        return Objects.requireNonNull(template, "template can not be null");
    }


    /**
     * impl End: IDataXBatchPost
     * ========================================================
     */
    @TISExtension()
    public static class DefaultDescriptor extends HdfsWriterDescriptor implements IFlatTableBuilderDescriptor, DataxWriter.IRewriteSuFormProperties {

        PaimonOptions<DataxWriter> opts;

        public DefaultDescriptor() {
            super();
            opts = PaimonPropAssist.createOpts(this);
            //opts.add("tableBucket", CoreOptions.BUCKET);
            OverwriteProps fileFormatOverwriteProps = new OverwriteProps();
            fileFormatOverwriteProps.setEnumOpts(
                    Lists.newArrayList(new Option(FILE_FORMAT_ORC), new Option(FILE_FORMAT_AVRO), new Option(FILE_FORMAT_PARQUET)));
            opts.add("storeFormat", CoreOptions.FILE_FORMAT, fileFormatOverwriteProps);
        }

        @Override
        public Descriptor getRewriterSelectTabDescriptor() {
            Class targetClass = PaimonSelectedTab.class;
            return Objects.requireNonNull(TIS.get().getDescriptor(targetClass), "subForm clazz:" + targetClass + " "
                    + "can not find relevant Descriptor");
        }

        @Override
        public boolean isSupportTabCreate() {
            return true;
        }

        @Override
        public boolean isSupportIncr() {
            return true;
        }

        public boolean validateTableBucket(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {
            Integer bucket = Integer.valueOf(value);
            if (bucket < 1) {
                switch (bucket) {
                    case -2:
                    case -1:
                        return true;
                    default:
                        msgHandler.addFieldError(context, fieldName, "必须为-2，-1或大于0的整数");
                        return false;
                }
            }
            return true;
        }

        @Override
        public boolean isRdbms() {
            return false;
        }

        @Override
        public EndType getEndType() {
            return EndType.Paimon;
        }

        @Override
        public String getDisplayName() {
            return getEndType().name();
        }
    }
}

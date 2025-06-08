package com.qlangtech.tis.plugin.paimon.datax;

import com.alibaba.citrus.turbine.Context;
import com.alibaba.datax.common.element.Column;
import com.google.common.collect.Lists;
import com.qlangtech.tis.TIS;
import com.qlangtech.tis.datax.IDataxContext;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.IDataxProcessor.TableMap;
import com.qlangtech.tis.datax.impl.DataxWriter;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.extension.impl.IOUtils;
import com.qlangtech.tis.extension.util.AbstractPropAssist.Options;
import com.qlangtech.tis.extension.util.OverwriteProps;
import com.qlangtech.tis.fullbuild.taskflow.IFlatTableBuilderDescriptor;
import com.qlangtech.tis.manage.common.Option;
import com.qlangtech.tis.plugin.KeyedPluginStore;
import com.qlangtech.tis.plugin.KeyedPluginStore.Key;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.BasicFSWriter;
import com.qlangtech.tis.plugin.datax.BasicFSWriter.FSDataXContext;
import com.qlangtech.tis.plugin.datax.HdfsWriterDescriptor;
import com.qlangtech.tis.plugin.datax.transformer.RecordTransformerRules;
import com.qlangtech.tis.plugin.ds.DataType;
import com.qlangtech.tis.plugin.ds.DataType.TypeVisitor;
import com.qlangtech.tis.plugin.ds.IColMetaGetter;
import com.qlangtech.tis.plugin.paimon.catalog.PaimonCatalog;
import com.qlangtech.tis.plugin.paimon.datax.compact.PaimonCompaction;
import com.qlangtech.tis.plugin.paimon.datax.utils.PaimonSnapshot;
import com.qlangtech.tis.plugin.paimon.datax.writemode.WriteMode;
import com.qlangtech.tis.plugin.paimon.datax.writemode.WriteMode.PaimonTableWriter;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.options.ConfigOptions;
import org.apache.paimon.schema.Schema.Builder;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.BinaryType;
import org.apache.paimon.types.DateType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.types.VarBinaryType;
import org.apache.paimon.types.VarCharType;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

import static org.apache.paimon.CoreOptions.FILE_FORMAT_AVRO;
import static org.apache.paimon.CoreOptions.FILE_FORMAT_ORC;
import static org.apache.paimon.CoreOptions.FILE_FORMAT_PARQUET;

/**
 * paimon 批量数据写入
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-05-15 14:56
 **/
public class DataxPaimonWriter extends DataxWriter implements SchemaBuilderSetter, KeyedPluginStore.IPluginKeyAware {


    @FormField(ordinal = 1, validate = {Validator.require})
    public PaimonCatalog catalog;

    @FormField(ordinal = 6, type = FormFieldType.ENUM, validate = {Validator.require})
    public String storeFormat;

    /**
     * https://paimon.apache.org/docs/master/maintenance/configurations/#coreoptions
     * Bucket number for file store.
     * It should either be equal to -1 (dynamic bucket mode), -2 (postpone bucket mode), or it must be greater than 0 (fixed bucket mode).
     */
    @FormField(ordinal = 7, type = FormFieldType.INT_NUMBER, validate = {Validator.require})
    public Integer tableBucket;


    @FormField(ordinal = 11, validate = {Validator.require})
    public WriteMode paimonWriteMode;

    @FormField(ordinal = 16, validate = {Validator.require})
    public PaimonCompaction compaction;

    @FormField(ordinal = 18, validate = {Validator.require})
    public PaimonSnapshot snapshot;

//    @FormField(ordinal = 20, validate = {Validator.require})
//    public PaimonHiveCfg hiveCfg;


    @FormField(ordinal = 22, type = FormFieldType.TEXTAREA, advance = false, validate = {Validator.require})
    public String template;

    public String dataXName;

    @Override
    public void setKey(Key key) {
        this.dataXName = key.keyVal.getVal();
    }

    @Override
    public void initializeSchemaBuilder(Builder tabSchemaBuilder) {

        DefaultDescriptor desc = (DefaultDescriptor) this.getDescriptor();
        desc.opts.setTarget((field, val) -> {
            tabSchemaBuilder.option(field.key(), String.valueOf(val));
        }, this);

        this.compaction.initializeSchemaBuilder(tabSchemaBuilder);
        this.snapshot.initializeSchemaBuilder(tabSchemaBuilder);
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
        return ((PaimonFSDataXContext) getSubTask(Optional.of(new TableMap(tab)), transformerRules)).getPaimonCols();
    }

    public Catalog createCatalog() {
        return this.catalog.createCatalog(this.dataXName);
    }

    public PaimonTableWriter createWriter(Table table) {
        return Objects.requireNonNull(this.paimonWriteMode).createWriter(table);
    }

    private static final ThreadLocal<DateFormat> dateFormat = new ThreadLocal<>() {
        @Override
        protected DateFormat initialValue() {
            return new SimpleDateFormat("yyyy-MM-dd");
        }
    };


    private static final ThreadLocal<DateFormat> dateTimeFormat = new ThreadLocal<>() {
        @Override
        protected DateFormat initialValue() {
            return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        }
    };


    public class PaimonFSDataXContext implements IDataxContext {
        protected final IDataxProcessor.TableMap tabMap;
        private final String dataxName;
        private final List<IColMetaGetter> cols;
        private final String targetDataBaseName;

        public PaimonFSDataXContext(String targetDataBaseName, TableMap tabMap, String dataxName, Optional<RecordTransformerRules> transformerRules) {
            //super(tabMap, dataxName, transformerRules);
            this.tabMap = tabMap;
            this.dataxName = dataxName;
            this.cols = tabMap.appendTransformerRuleCols(transformerRules);
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
            String tabName = this.tabMap.getTo();
            if (StringUtils.isBlank(tabName)) {
                throw new IllegalStateException("tabName of tabMap can not be null ,tabMap:" + tabMap);
            }
            return tabName;
        }

        public final List<IColMetaGetter> getCols() {
            return this.cols;
        }

        /**
         * @return
         * @see org.apache.paimon.types.DataTypes
         * @see org.apache.paimon.data.GenericRow
         */
        public List<PaimonColumn> getPaimonCols() {
            List<PaimonColumn> paimonCols = Lists.newArrayList();
            List<IColMetaGetter> cols = this.getCols();
            for (IColMetaGetter col : cols) {
                paimonCols.add(new PaimonColumn(col.getName()
                        , col.getType().accept(new TypeVisitor<Pair<org.apache.paimon.types.DataType, Function<Column, Object>>>() {

                    @Override
                    public Pair<org.apache.paimon.types.DataType, Function<Column, Object>> decimalType(DataType type) {
                        return Pair.of(new DecimalType()
                                , (col) -> org.apache.paimon.data.Decimal.fromBigDecimal(
                                        col.asBigDecimal(), type.getColumnSize(), type.getDecimalDigits()));
                    }

                    @Override
                    public Pair<org.apache.paimon.types.DataType, Function<Column, Object>> bigInt(DataType type) {
                        return Pair.of(new BigIntType(), (col) -> col.asLong());
                    }

                    @Override
                    public Pair<org.apache.paimon.types.DataType, Function<Column, Object>> doubleType(DataType type) {
                        return Pair.of(new DoubleType(), Column::asDouble);
                    }

                    @Override
                    public Pair<org.apache.paimon.types.DataType, Function<Column, Object>> dateType(DataType type) {
                        return Pair.of(new DateType(), (col) -> dateFormat.get().format(col.asDate()));
                    }

                    @Override
                    public Pair<org.apache.paimon.types.DataType, Function<Column, Object>> timestampType(DataType type) {
                        return Pair.of(new TimestampType(type.getColumnSize()), (col) -> org.apache.paimon.data.Timestamp.fromEpochMillis(col.asLong()));
                    }

                    @Override
                    public Pair<org.apache.paimon.types.DataType, Function<Column, Object>> bitType(DataType type) {
                        return Pair.of(new BinaryType(1), Column::asBytes);
                    }

                    @Override
                    public Pair<org.apache.paimon.types.DataType, Function<Column, Object>> blobType(DataType type) {
                        return Pair.of(new VarBinaryType(type.getColumnSize()), Column::asBytes);
                    }

                    @Override
                    public Pair<org.apache.paimon.types.DataType, Function<Column, Object>> varcharType(DataType type) {
                        return Pair.of(new VarCharType(type.getColumnSize()), (col) -> BinaryString.fromString(col.asString()));
                    }
                })));
            }
            return paimonCols;
        }
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

        Options<DataxWriter, ConfigOption> opts;

        public DefaultDescriptor() {
            super();
            opts = PaimonPropAssist.createOpts(this);
            opts.add("tableBucket", CoreOptions.BUCKET);
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
        public EndType getEndType() {
            return EndType.Paimon;
        }

        @Override
        public String getDisplayName() {
            return getEndType().name();
        }
    }
}

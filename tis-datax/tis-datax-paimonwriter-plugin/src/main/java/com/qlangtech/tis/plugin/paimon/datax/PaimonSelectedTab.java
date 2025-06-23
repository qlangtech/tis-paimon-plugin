package com.qlangtech.tis.plugin.paimon.datax;

import com.alibaba.citrus.turbine.Context;
import com.alibaba.datax.common.element.Column;
import com.google.common.collect.Lists;
import com.qlangtech.tis.datax.IDataxProcessor.TableMap;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.manage.common.Option;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.SelectedTab;
import com.qlangtech.tis.plugin.datax.transformer.RecordTransformerRules;
import com.qlangtech.tis.plugin.ds.ColumnMetaData;
import com.qlangtech.tis.plugin.ds.DataType.TypeVisitor;
import com.qlangtech.tis.plugin.ds.DataTypeMeta;
import com.qlangtech.tis.plugin.ds.IColMetaGetter;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.BooleanType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.DateType;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.VarCharType;

import java.sql.Date;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-05-17 00:53
 **/
public class PaimonSelectedTab extends SelectedTab {


    @FormField(ordinal = 4, validate = {Validator.require})
    public PaimonPartition partition;

    @FormField(ordinal = 5, validate = {Validator.require})
    public PaimonSequenceFields sequenceField;

    public final List<String> getPartitionKeys() {
        return this.partition.getPartitionKeys();
    }

    public static List<Option> getPtCandidateFields() {
        return SelectedTab.getContextOpts((cols) -> cols.stream().filter((col) -> {
            switch (col.getType().getCollapse()) {
                // case STRING:
                case INT:
                case Long:
                case Date:
                    return true;
            }
            return false;
        }));
    }

    /**
     * 主键候选字段
     *
     * @return
     */
    public static List<Option> getPaimonPrimaryKeys() {
        return getPaimonKeys((col) -> true);
    }

    private static List<Option> getPaimonKeys(Predicate<ColumnMetaData> predicate) {
        return SelectedTab.getContextTableCols((cols) -> cols.stream() //
                .filter(predicate)).stream().map((c) -> new Option(c.getName())).collect(Collectors.toList());
    }

    public static List<String> getDeftRecordKeys() {
        //return getPaimonPrimaryKeys().stream().map((pk) -> String.valueOf(pk.getValue())).collect(Collectors.toList());
        return getPaimonKeys((col) -> col.isPk()).stream().map((pk) -> String.valueOf(pk.getValue())).collect(Collectors.toList());
    }

    public List<PaimonColumn> getPaimonCols(Optional<RecordTransformerRules> transformerRules) {
        return this.getPaimonCols(new TableMap(this), transformerRules);
    }

    /**
     * @return
     * @see org.apache.paimon.types.DataTypes
     * @see org.apache.paimon.data.GenericRow
     * @see org.apache.paimon.utils.InternalRowUtils#get
     */
    public List<PaimonColumn> getPaimonCols(TableMap tableMap, Optional<RecordTransformerRules> transformerRules) {
        List<PaimonColumn> paimonCols = Lists.newArrayList();

        List<IColMetaGetter> cols = tableMap.appendTransformerRuleCols(transformerRules);

        for (IColMetaGetter col : cols) {
            paimonCols.add(new PaimonColumn(col.getName(), this.primaryKeys.contains(col.getName())
                    , col.getType().accept(new TypeVisitor<Pair<DataType, Function<Column, Object>>>() {

                @Override
                public Pair<DataType, Function<Column, Object>> intType(com.qlangtech.tis.plugin.ds.DataType type) {
                    return Pair.of(DataTypes.INT(), (col) -> col.asLong().intValue());
                }

                @Override
                public Pair<DataType, Function<Column, Object>> floatType(com.qlangtech.tis.plugin.ds.DataType type) {
                    return Pair.of(DataTypes.FLOAT(), (col) -> col.asDouble().floatValue());
                }

                @Override
                public Pair<DataType, Function<Column, Object>> timeType(com.qlangtech.tis.plugin.ds.DataType type) {
                    return Pair.of(DataTypes.TIME(), (col) -> org.apache.paimon.utils.DateTimeUtils.toInternal(new Date(col.asDate().getTime())));
                }

                @Override
                public Pair<DataType, Function<Column, Object>> tinyIntType(com.qlangtech.tis.plugin.ds.DataType dataType) {
                    return Pair.of(DataTypes.TINYINT(), (col) -> col.asLong().byteValue());
                }

                @Override
                public Pair<DataType, Function<Column, Object>> smallIntType(com.qlangtech.tis.plugin.ds.DataType dataType) {
                    return Pair.of(DataTypes.SMALLINT(), (col) -> col.asLong().shortValue());
                }

                @Override
                public Pair<DataType, Function<Column, Object>> boolType(com.qlangtech.tis.plugin.ds.DataType dataType) {
                    return Pair.of(DataTypes.BOOLEAN(), (col) -> col.asBoolean());
                }

                /**
                 * 需要与 com.qlangtech.tis.plugins.incr.flink.cdc.BasicFlinkDataMapper.DefaultTypeVisitor#decimalType 相一致
                 * @param type
                 * @return
                 */
                @Override
                public Pair<org.apache.paimon.types.DataType, Function<Column, Object>> decimalType(com.qlangtech.tis.plugin.ds.DataType type) {
                    int[] normalize = DataTypeMeta.normalizeDecimalPrecisionAndScale(type);
                    assert normalize.length == 2;
                    int precision = normalize[0];
                    int scale = normalize[1];
                    return Pair.of(DataTypes.DECIMAL(precision, scale)
                            , (col) -> org.apache.paimon.data.Decimal.fromBigDecimal(
                                    col.asBigDecimal(), precision, scale));
                }

                @Override
                public Pair<org.apache.paimon.types.DataType, Function<Column, Object>> bigInt(com.qlangtech.tis.plugin.ds.DataType type) {
                    return Pair.of(new BigIntType(), (col) -> col.asLong());
                }

                @Override
                public Pair<org.apache.paimon.types.DataType, Function<Column, Object>> doubleType(com.qlangtech.tis.plugin.ds.DataType type) {
                    return Pair.of(new DoubleType(), Column::asDouble);
                }

                // 毫秒数/天的毫秒数 = 天数
                private static final long MILLIS_PER_DAY = 24 * 60 * 60 * 1000L;

                @Override
                public Pair<org.apache.paimon.types.DataType, Function<Column, Object>> dateType(com.qlangtech.tis.plugin.ds.DataType type) {

                    return Pair.of(new DateType(), (col) -> {
//                            Instant instant = ;
//                            LocalDate localDate = instant.atZone(ZoneOffset.UTC).toLocalDate();
                        return (int) (col.asDate().getTime() / MILLIS_PER_DAY);
                    });
                }

                @Override
                public Pair<org.apache.paimon.types.DataType, Function<Column, Object>> timestampType(com.qlangtech.tis.plugin.ds.DataType type) {
                    return Pair.of(DataTypes.TIMESTAMP(), (col) -> org.apache.paimon.data.Timestamp.fromEpochMillis(col.asLong()));
                }

                @Override
                public Pair<org.apache.paimon.types.DataType, Function<Column, Object>> bitType(com.qlangtech.tis.plugin.ds.DataType type) {
                    return Pair.of(new BooleanType(true), (colVal) -> {
                        return colVal.asLong() > 0;
                    });
                }

                @Override
                public Pair<org.apache.paimon.types.DataType, Function<Column, Object>> blobType(com.qlangtech.tis.plugin.ds.DataType type) {
                    return Pair.of(DataTypes.VARBINARY(type.getColumnSize()), Column::asBytes);
                }

                @Override
                public Pair<org.apache.paimon.types.DataType, Function<Column, Object>> varcharType(com.qlangtech.tis.plugin.ds.DataType type) {
                    return Pair.of(new VarCharType(type.getColumnSize()), (col) -> BinaryString.fromString(col.asString()));
                }
            })));
        }
        return paimonCols;
    }


    @TISExtension
    public static final class DefaultDescriptor extends Descriptor<SelectedTab> {
        public DefaultDescriptor() {
            super();
        }


        @Override
        protected boolean verify(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {
            return this.validateAll(msgHandler, context, postFormVals);
        }


        @Override
        protected boolean validateAll(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {

//            PaimonSelectedTab tab = postFormVals.newInstance();
//            List<String> pks = tab.primaryKeys;
//            List<String> pts = tab.partition.getPartitionKeys();
//
//            if (CollectionUtils.isNotEmpty(pts) && !CollectionUtils.isSubCollection(pts, pks)) {
//                msgHandler.addFieldError(context, "partition", "按照Paimon表设置规则，分区键须要包含在主键中");
//                return false;
//            }

            return true;
        }
    }
}

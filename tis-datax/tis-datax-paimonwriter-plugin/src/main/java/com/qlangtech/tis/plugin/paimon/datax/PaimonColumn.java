package com.qlangtech.tis.plugin.paimon.datax;

import com.alibaba.datax.common.element.Column;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.paimon.types.DataType;

import java.util.function.Function;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-05-16 11:56
 **/
public class PaimonColumn {
    private final String name;
    public final org.apache.paimon.types.DataType type;
    private final Function<Column, Object> paimonValGetter;
    private final boolean primaryKey;


    public PaimonColumn(String name, boolean primaryKey, Pair<DataType, Function<Column, Object>> typeAndValGetter) {
        this.name = name;
        this.primaryKey = primaryKey;
        this.type = typeAndValGetter.getKey();
        this.paimonValGetter = typeAndValGetter.getValue();
    }

    public boolean isPrimaryKey() {
        return primaryKey;
    }

    public Object getPaimonFieldVal(Column colVal) {
        try {
            return this.paimonValGetter.apply(colVal);
        } catch (Exception e) {
            throw new RuntimeException("colname:" + name + ",paimonType:" + getType(), e);
        }
    }

    public String getName() {
        return name;
    }

    public String getType() {
        return type.asSQLString();
    }

    @Override
    public String toString() {
        return "name='" + name + '\'' +
                ", type=" + type;
    }
}

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


    public PaimonColumn(String name, Pair<DataType, Function<Column, Object>> typeAndValGetter) {
        this.name = name;
        this.type = typeAndValGetter.getKey();
        this.paimonValGetter = typeAndValGetter.getValue();
    }

    public Object getPaimonFieldVal(Column colVal) {
        return this.paimonValGetter.apply(colVal);
    }

    public String getName() {
        return name;
    }

    public String getType() {
        return type.asSQLString();
    }
}

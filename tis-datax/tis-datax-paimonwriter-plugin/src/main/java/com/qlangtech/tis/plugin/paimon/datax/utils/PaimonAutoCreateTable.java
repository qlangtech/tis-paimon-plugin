package com.qlangtech.tis.plugin.paimon.datax.utils;

import com.qlangtech.tis.datax.IDataxProcessor.TableMap;
import com.qlangtech.tis.datax.SourceColMetaGetter;
import com.qlangtech.tis.datax.impl.DataxWriter;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.IEndTypeGetter.EndType;
import com.qlangtech.tis.plugin.datax.CreateTableSqlBuilder;
import com.qlangtech.tis.plugin.datax.CreateTableSqlBuilder.ColWrapper;
import com.qlangtech.tis.plugin.datax.common.impl.ParamsAutoCreateTable;

import java.util.Optional;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-06-20 14:37
 **/
public class PaimonAutoCreateTable extends ParamsAutoCreateTable<ColWrapper> {

    @Override
    public CreateTableSqlBuilder createSQLDDLBuilder(DataxWriter rdbmsWriter
            , SourceColMetaGetter sourceColMetaGetter, TableMap tableMapper, Optional transformers) {
        throw new UnsupportedOperationException();
    }

    @TISExtension
    public static class Desc extends ParamsAutoCreateTable.DftDesc {
        public Desc() {
            super();
        }

        @Override
        public EndType getEndType() {
            return EndType.Paimon;
        }
    }
}

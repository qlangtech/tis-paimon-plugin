package com.qlangtech.tis.plugin.paimon.datax.utils;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Catalog.TableNotExistException;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.table.Table;

import java.util.Objects;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-06-23 11:21
 **/
public class PaimonUtils {
    public static Pair<Boolean, Table> tableExists(Catalog catalog, String dbName, String tableName) {
        if (StringUtils.isEmpty(dbName)) {
            throw new IllegalArgumentException("dbName can not be null null");
        }
        if (StringUtils.isEmpty(tableName)) {
            throw new IllegalArgumentException("tableName can not be null null");
        }
        Identifier identifier = Identifier.create(dbName, tableName);
        try {
            return Pair.of(true, Objects.requireNonNull(catalog, "catalog can not be null")
                    .getTable(identifier));
        } catch (TableNotExistException e) {
            return Pair.of(false, null);
        }
//            boolean exists = catalog.tableExists(identifier);
//            return exists;
    }
}

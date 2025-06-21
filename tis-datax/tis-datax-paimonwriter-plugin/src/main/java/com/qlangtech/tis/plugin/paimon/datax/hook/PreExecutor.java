package com.qlangtech.tis.plugin.paimon.datax.hook;

import com.alibaba.datax.plugin.writer.paimonwriter.PaimonWriter.Task;
import com.qlangtech.tis.exec.IExecChainContext;
import com.qlangtech.tis.fullbuild.indexbuild.IRemoteTaskPreviousTrigger;
import com.qlangtech.tis.plugin.datax.transformer.RecordTransformerRules;
import com.qlangtech.tis.plugin.paimon.datax.DataxPaimonWriter;
import com.qlangtech.tis.plugin.paimon.datax.PaimonColumn;
import com.qlangtech.tis.plugin.paimon.datax.PaimonSelectedTab;
import com.qlangtech.tis.sql.parser.tuple.creator.EntityName;
import org.apache.commons.collections.CollectionUtils;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.schema.Schema;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * 负责创建创建paimon 表
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-06-12 11:27
 **/
public class PreExecutor implements IRemoteTaskPreviousTrigger {
    private final DataxPaimonWriter paimonWriter;
    private final EntityName entity;
    private final PaimonSelectedTab tab;
    private final IExecChainContext execContext;

    public PreExecutor(IExecChainContext execContext, DataxPaimonWriter paimonWriter, EntityName entity, PaimonSelectedTab tab) {
        this.paimonWriter = Objects.requireNonNull(paimonWriter, "paimonWriter can not be null");
        this.entity = Objects.requireNonNull(entity, "entity can not be null");
        this.tab = Objects.requireNonNull(tab, "tab can not be null");
        this.execContext = Objects.requireNonNull(execContext, "execContext can not be null");
    }

    @Override
    public String getTaskName() {
        return "Initialize_" + this.entity.getTabName();
    }

    @Override
    public void run() {


        SessionStateUtil.execute(paimonWriter.catalog, () -> {
            try (Catalog catalog = paimonWriter.createCatalog()) {
                // 判断表是否存在
                if (!Task.tableExists(catalog, this.entity.getDbName(), this.entity.getTabName()).getKey()) {
                    // 创建新表
                    Optional<RecordTransformerRules> transformerRules = getRecordTransformerRules();
                    List<PaimonColumn> paimonCols = tab.getPaimonCols(transformerRules);
                    this.createTable(catalog, this.entity.getDbName(), this.entity.getTabName(), paimonCols, tab);
                }

            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });


    }

    Optional<RecordTransformerRules> getRecordTransformerRules() {
        Optional<RecordTransformerRules> transformerRules =
                RecordTransformerRules.loadTransformerRules(
                        null
                        , Objects.requireNonNull(execContext, "execContext can not be null").getProcessor()
                        , this.tab.getName());
        return transformerRules;
    }

    private void createTable(Catalog catalog, String dbName, String tableName
            , List<PaimonColumn> cols, PaimonSelectedTab tab) {
        List<String> pks = tab.getPrimaryKeys();
        List<String> partKeys = tab.getPartitionKeys();
        Schema.Builder schemaBuilder = Schema.newBuilder();
        Objects.requireNonNull(this.paimonWriter, "paimonWriter can not be null")
                .initializeSchemaBuilder(schemaBuilder, tab);

        for (PaimonColumn columnConfig : cols) {
            schemaBuilder.column(columnConfig.getName(), columnConfig.type, null);
        }
        if (CollectionUtils.isNotEmpty(pks)) {
            schemaBuilder.primaryKey(pks);

        }
        Schema schema = schemaBuilder.build();
        if (CollectionUtils.isNotEmpty(partKeys)) {
            schemaBuilder.partitionKeys(partKeys);
            schema = schemaBuilder.option("metastore.partitioned-table", "true").build();
        }

        Identifier identifier = Identifier.create(dbName, tableName);
        try {
            catalog.createTable(identifier, schema, false);
        } catch (Catalog.TableAlreadyExistException e) {
            throw new RuntimeException("table not exist", e);
        } catch (Catalog.DatabaseNotExistException e) {
            throw new RuntimeException("database: '" + dbName + "' not exist", e);
        }

    }
}

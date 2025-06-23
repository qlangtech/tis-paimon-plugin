package com.qlangtech.tis.plugin.paimon.datax.hook;

import com.qlangtech.tis.exec.IExecChainContext;
import com.qlangtech.tis.fullbuild.indexbuild.IRemoteTaskPostTrigger;
import com.qlangtech.tis.plugin.paimon.datax.DataxPaimonWriter;
import com.qlangtech.tis.plugin.paimon.datax.PaimonSelectedTab;
import com.qlangtech.tis.plugin.paimon.datax.utils.PaimonUtils;
import com.qlangtech.tis.plugin.paimon.datax.writemode.WriteMode.PaimonTableWriter;
import com.qlangtech.tis.sql.parser.tuple.creator.EntityName;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.table.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * 负责任务commit，多文件compact合并
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-06-12 11:28
 **/
public class PostExecutor implements IRemoteTaskPostTrigger {

    private final DataxPaimonWriter paimonWriter;
    private final EntityName entity;
    private final PaimonSelectedTab tab;
    private final IExecChainContext execContext;
    private static final Logger logger = LoggerFactory.getLogger(PostExecutor.class);

    public PostExecutor(IExecChainContext execContext, DataxPaimonWriter paimonWriter, EntityName entity, PaimonSelectedTab tab) {
        this.paimonWriter = Objects.requireNonNull(paimonWriter, "paimonWriter can not be null");
        this.entity = Objects.requireNonNull(entity, "entity can not be null");
        this.tab = Objects.requireNonNull(tab, "tab can not be null");
        this.execContext = Objects.requireNonNull(execContext, "execContext can not be null");
    }

    @Override
    public String getTaskName() {
        return "PostCommit_" + this.entity.getTabName();
    }

    @Override
    public void run() {
        SessionStateUtil.execute(paimonWriter.catalog, () -> {
            try (Catalog catalog = paimonWriter.createCatalog()) {
                logger.info("start batch commit paimon table:" + this.entity.getTabName());
                // 判断表是否存在
                Pair<Boolean, Table> existTab = PaimonUtils.tableExists(catalog, this.entity.getDbName(), this.entity.getTabName());
                if (!existTab.getKey()) {
                    throw new IllegalStateException("table:" + entity.getFullName() + " must be exist");
                }
                // this.execContext.getTaskId()
                PaimonTableWriter writer = paimonWriter.createWriter(this.execContext.getTaskId(), existTab.getValue());
                // commit and final execute compaction
                writer.offlineFlushCache();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }
}

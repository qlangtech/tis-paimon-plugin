package com.qlangtech.tis.plugin.paimon.catalog;

import com.qlangtech.tis.TIS;
import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.util.OverwriteProps;
import com.qlangtech.tis.fs.ITISFileSystemFactory;
import com.qlangtech.tis.hdfs.impl.HdfsFileSystemFactory;
import com.qlangtech.tis.offline.FileSystemFactory;
import com.qlangtech.tis.plugin.IRepositoryResourceScannable;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.paimon.datax.PaimonPropAssist;
import com.qlangtech.tis.plugin.paimon.datax.PaimonPropAssist.PaimonOptions;
import com.qlangtech.tis.plugin.paimon.datax.PaimonSelectedTab;
import com.qlangtech.tis.plugin.paimon.datax.SchemaBuilderSetter;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.schema.Schema.Builder;

import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-05-18 11:51
 * @see com.qlangtech.tis.plugin.paimon.catalog.HiveCatalog
 * @see com.qlangtech.tis.plugin.paimon.catalog.FileSystemCatalog
 **/
public abstract class PaimonCatalog implements Describable<PaimonCatalog>, IRepositoryResourceScannable, SchemaBuilderSetter {
//  "catalogPath": {
//        "help": "paimon创建的catalog路径，可以包含文件系统的和hdfs系统的路径",
//                "placeholder": "example:'/app/hive/warehouse'"
//    }
//    @FormField(identity = false, ordinal = 0, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.absolute_path})
//    public String catalogPath;

    public static final String FIELD_TABLE_OWNER = "tableOwner";

    @FormField(ordinal = 1, type = FormFieldType.SELECTABLE, validate = {Validator.require})
    public String fsName;


    @FormField(ordinal = 5, advance = true, validate = {Validator.require})
    public CatalogLock catalogLock;

    @FormField(ordinal = 6, advance = true, validate = {Validator.require})
    public CatalogCache catalogCache;

    @FormField(ordinal = 2, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.db_col_name})
    public String tableOwner;


    private transient FileSystemFactory fileSystem;

    protected FileSystemFactory getFs() {
        if (fileSystem == null) {
            if (StringUtils.isEmpty(this.fsName)) {
                throw new IllegalStateException("prop field:" + this.fsName + " can not be empty");
            }
            this.fileSystem = FileSystemFactory.getFsFactory(fsName);
        }
        Objects.requireNonNull(this.fileSystem, "fileSystem has not be initialized");
        return fileSystem;
    }

    @Override
    public final void initializeSchemaBuilder(Builder tabSchemaBuilder, PaimonSelectedTab tab) {
        BasicCatalogDescriptor desc = (BasicCatalogDescriptor) this.getDescriptor();
        desc.opts.setTarget((field, val) -> {
            tabSchemaBuilder.option(field.key(), String.valueOf(val));
        }, this);
    }

    public abstract org.apache.paimon.options.Options createOpts(String pipelineName);


    public abstract Optional<String> getDBName();

    public abstract <T> T accept(PaimonCatalogVisitor<T> visitor);

    public abstract Catalog createCatalog(String pipelineName);

    public String getRootDir() {
        return this.getFs().getRootDir();
    }

    public Configuration getConfiguration() {
        return this.getFs().getConfiguration();
    }

    @Override
    public Descriptor<PaimonCatalog> getDescriptor() {
        Descriptor<PaimonCatalog> descriptor = Describable.super.getDescriptor();
        if (!BasicCatalogDescriptor.class.isAssignableFrom(descriptor.getClass())) {
            throw new IllegalStateException(descriptor.getClass().getSimpleName() + " must extend from " + BasicCatalogDescriptor.class.getSimpleName());
        }
        return descriptor;
    }

    protected static abstract class BasicCatalogDescriptor extends Descriptor<PaimonCatalog> {
        public final PaimonOptions<PaimonCatalog> opts;

        public BasicCatalogDescriptor() {
            super();


            this.registerSelectOptions(ITISFileSystemFactory.KEY_FIELD_NAME_FS_NAME
                    , () -> TIS.getPluginStore(FileSystemFactory.class)
                            .getPlugins().stream().filter(((f) -> f instanceof HdfsFileSystemFactory)).collect(Collectors.toList()));
            this.opts = PaimonPropAssist.createOpts(this);
            // ref: PaimonDataSinkOptions.COMMIT_USER


            OverwriteProps commitUserOwerwrite = new OverwriteProps();
            commitUserOwerwrite.setDftVal("admin");
            commitUserOwerwrite.setAppendHelper("在 Paimon 中的作用是标识提交操作的来源，虽然看似可以配置任意值，但它在分布式环境中对运维监控至关重要。\n 以下是详细解析：\n 1. 核心作用：提交者身份标识 \n 2. 分布式环境追踪,在 Flink 集群中，不同 TaskManager 上的写入任务会产生各自的提交记录。通过前缀可快速区分");
            this.opts.add(FIELD_TABLE_OWNER, CoreOptions.COMMIT_USER_PREFIX, commitUserOwerwrite);
        }
    }

}

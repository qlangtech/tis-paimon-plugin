package com.qlangtech.tis.plugin.paimon.catalog;

import com.qlangtech.tis.TIS;
import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.fs.ITISFileSystemFactory;
import com.qlangtech.tis.hdfs.impl.HdfsFileSystemFactory;
import com.qlangtech.tis.offline.FileSystemFactory;
import com.qlangtech.tis.plugin.IRepositoryResourceScannable;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.paimon.datax.SchemaBuilderSetter;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.paimon.catalog.Catalog;

import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-05-18 11:51
 **/
public abstract class PaimonCatalog implements Describable<PaimonCatalog>, IRepositoryResourceScannable, SchemaBuilderSetter {
//  "catalogPath": {
//        "help": "paimon创建的catalog路径，可以包含文件系统的和hdfs系统的路径",
//                "placeholder": "example:'/app/hive/warehouse'"
//    }
//    @FormField(identity = false, ordinal = 0, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.absolute_path})
//    public String catalogPath;

    @FormField(ordinal = 1, type = FormFieldType.SELECTABLE, validate = {Validator.require})
    public String fsName;


    @FormField(ordinal = 2, advance = true, validate = {Validator.require})
    public CatalogLock catalogLock;

    @FormField(ordinal = 3, advance = true, validate = {Validator.require})
    public CatalogCache catalogCache;


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

    public abstract org.apache.paimon.options.Options createOpts(String pipelineName);

    @FormField(ordinal = 1, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.db_col_name})
    public String tableOwner;

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
        public BasicCatalogDescriptor() {
            super();
            this.registerSelectOptions(ITISFileSystemFactory.KEY_FIELD_NAME_FS_NAME
                    , () -> TIS.getPluginStore(FileSystemFactory.class)
                            .getPlugins().stream().filter(((f) -> f instanceof HdfsFileSystemFactory)).collect(Collectors.toList()));
        }
    }

}

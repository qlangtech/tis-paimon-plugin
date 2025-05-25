package com.qlangtech.tis.plugin.paimon.catalog;

import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.offline.FileSystemFactory;
import com.qlangtech.tis.plugin.IRepositoryResourceScannable;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.paimon.datax.SchemaBuilderSetter;
import org.apache.paimon.catalog.Catalog;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-05-18 11:51
 **/
public abstract class PaimonCatalog implements Describable<PaimonCatalog>, IRepositoryResourceScannable, SchemaBuilderSetter {
    /**
     * catalogPath是paimon创建的catalog路径，可以包含文件系统的和hdfs系统的路径。
     */
    @FormField(identity = false, ordinal = 0, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.absolute_path})
    public String catalogPath;

    @FormField(ordinal = 1, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.db_col_name})
    public String tableOwner;


    public abstract <T> T accept(PaimonCatalogVisitor<T> visitor);

    public abstract Catalog createCatalog(FileSystemFactory fileSystemFactory);
}

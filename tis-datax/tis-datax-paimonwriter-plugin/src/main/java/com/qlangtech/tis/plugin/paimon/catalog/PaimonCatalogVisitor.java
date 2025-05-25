package com.qlangtech.tis.plugin.paimon.catalog;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-05-24 12:03
 **/
public interface PaimonCatalogVisitor<T> {
    public T visit(FileSystemCatalog catalog);

    public T visit(HiveCatalog catalog);
}

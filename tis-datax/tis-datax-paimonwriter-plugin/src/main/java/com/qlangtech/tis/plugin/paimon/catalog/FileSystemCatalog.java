package com.qlangtech.tis.plugin.paimon.catalog;

import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.offline.FileSystemFactory;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema.Builder;

import java.util.Optional;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-05-18 11:52
 **/
public class FileSystemCatalog extends PaimonCatalog {

    @Override
    public Optional<String> getDBName() {
        return Optional.empty();
    }

    @Override
    public Options createOpts() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void startScanDependency() {

    }

    @Override
    public <T> T accept(PaimonCatalogVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public Catalog createCatalog() {
        CatalogContext context = CatalogContext.create(new org.apache.paimon.fs.Path(this.getRootDir()));
        return CatalogFactory.createCatalog(context);
    }

    @Override
    public void initializeSchemaBuilder(Builder schemaBuilder) {

    }

    /**
     * 暂时先不启动，使用hive的catalog就行了
     */
    // @TISExtension
    public static final class DefaultDescriptor extends Descriptor<PaimonCatalog> {
        public DefaultDescriptor() {
            super();
        }

        @Override
        public String getDisplayName() {
            return "FileSystem";
        }
    }
}

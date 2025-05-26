package com.qlangtech.tis.plugin.paimon.catalog;

import com.alibaba.datax.common.exception.DataXException;
import com.qlangtech.tis.config.hive.IHiveConnGetter;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.offline.FileSystemFactory;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ds.BasicDataSourceFactory;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema.Builder;
import org.apache.paimon.table.CatalogTableType;

import static com.alibaba.datax.plugin.writer.paimonwriter.Key.PAIMON_HIVE_CONF_DIR;
import static com.alibaba.datax.plugin.writer.paimonwriter.Key.PAIMON_METASTORE_URI;
import static com.alibaba.datax.plugin.writer.paimonwriter.PaimonWriterErrorCode.PAIMON_PARAM_LOST;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-05-18 11:52
 **/
public class HiveCatalog extends PaimonCatalog {

    @FormField(identity = false, ordinal = 0, type = FormFieldType.ENUM, validate = {Validator.require})
    public String dbName;

    @Override
    public void startScanDependency() {
        this.getDataSourceFactory();
    }

    @Override
    public <T> T accept(PaimonCatalogVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public Catalog createCatalog(FileSystemFactory fileSystemFactory) {

//        metastoreUri = sliceConfig.getString(PAIMON_METASTORE_URI);
//        hiveConfDir = sliceConfig.getString(PAIMON_HIVE_CONF_DIR);
//        hadoopConfDir = sliceConfig.getString(PAIMON_HADOOP_CONF_DIR);
        return this.createHiveCatalog(this.getHiveConnGetter(), fileSystemFactory);
    }


    public IHiveConnGetter getHiveConnGetter() {
        return (IHiveConnGetter) getDataSourceFactory();
    }


    //@Override
    public BasicDataSourceFactory getDataSourceFactory() {
        if (org.apache.commons.lang.StringUtils.isBlank(this.dbName)) {
            throw new IllegalStateException("prop dbName can not be null");
        }
        return BasicDataSourceFactory.getDs(this.dbName);
    }

    private Catalog createHiveCatalog(IHiveConnGetter hiveConnGetter, FileSystemFactory fsFactory) {
        // Paimon Hive catalog relies on Hive jars
        // You should add hive classpath or hive bundled jar.
        Options options = new Options();
        //   context;
        if (StringUtils.isEmpty(catalogPath)) {
            throw new IllegalStateException("prop catalogPath can not be empty");
        }
        options.set(CatalogOptions.WAREHOUSE, catalogPath);
        options.set(CatalogOptions.METASTORE, "hive");
        //默认设置为外部表
        options.set(CatalogOptions.TABLE_TYPE, CatalogTableType.EXTERNAL);

        /**
         * 1.如果metastore uri 存在，则不需要设置 hiveConfDir
         * 2.如果metastore uri 不存在，读取 hiveConfDir下的hive-site.xml也可以
         */
        String metastoreUri = hiveConnGetter.getMetaStoreUrls();
        if (StringUtils.isNotBlank(metastoreUri)) {
            options.set(CatalogOptions.URI, metastoreUri);
//        } else if (StringUtils.isNotBlank(hiveConfDir)) {
//            options.set("hive-conf-dir", hiveConfDir);
        } else {
            throw DataXException.asDataXException(PAIMON_PARAM_LOST,
                    String.format("您提供配置文件有误，[%s]和[%s]参数，至少需要配置一个，不允许为空或者留白 .", PAIMON_METASTORE_URI, PAIMON_HIVE_CONF_DIR));
        }

//        /**
//         * 1：通过配置hadoop-conf-dir(目录中必须包含hive-site.xml,core-site.xml文件)来创建catalog
//         * 2：通过配置hadoopConf(指定：coreSitePath：/path/core-site.xml,hdfsSitePath: /path/hdfs-site.xml)的方式来创建catalog
//         */
//        if (StringUtils.isNotBlank(hadoopConfDir)) {
//            options.set("hadoop-conf-dir", hadoopConfDir);
//            context = CatalogContext.create(options);
//        } else if (StringUtils.isNotBlank(coreSitePath) && StringUtils.isNotBlank(hdfsSitePath)) {
//            context = CatalogContext.create(options, hadoopConf);
//        } else {
//            throw DataXException.asDataXException(PAIMON_PARAM_LOST,
//                    String.format("您提供配置文件有误，[%s]和[%s]参数，至少需要配置一个，不允许为空或者留白 ."
//                            , PAIMON_HADOOP_CONF_DIR, "hadoopConfig:coreSiteFile&&hdfsSiteFile"));
//        }

        Configuration hadoopConfig = fsFactory.getConfiguration();
        CatalogContext context = CatalogContext.create(options, hadoopConfig);

        return CatalogFactory.createCatalog(context);

    }

    @Override
    public void initializeSchemaBuilder(Builder schemaBuilder) {

    }

    @TISExtension
    public static final class DefaultDescriptor extends Descriptor<PaimonCatalog> {
        public DefaultDescriptor() {
            super();
        }

        @Override
        public String getDisplayName() {
            return "HiveMetaStore";
        }
    }
}

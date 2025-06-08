package com.qlangtech.tis.plugin.paimon.catalog;

import com.qlangtech.tis.datax.StoreResourceType;
import com.qlangtech.tis.datax.impl.DataxWriter;
import com.qlangtech.tis.plugin.paimon.datax.DataxPaimonWriter;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.hive.HiveCatalogFactory;
import org.apache.paimon.options.Options;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-05-29 13:51
 **/
public class TISPaimonCatalogFactory extends HiveCatalogFactory {

    @Override
    public Catalog create(CatalogContext context) {

        Options options = context.options();
        String dataXName = options.get(StoreResourceType.DATAX_NAME);
        if (StringUtils.isEmpty(dataXName)) {
            throw new IllegalStateException("prop " + StoreResourceType.DATAX_NAME + " relevant val can not be null");
        }

        //  options
        DataxPaimonWriter paimonWriter
                = (DataxPaimonWriter) DataxWriter.load(null, StoreResourceType.DataApp, dataXName, true);
       // return paimonWriter.catalog.createCatalog();
        Configuration hadoopConf = paimonWriter.catalog.getConfiguration();
        CatalogContext catalogContext = CatalogContext.create(context.options(), hadoopConf);
//
        return org.apache.paimon.hive.HiveCatalog.createHiveCatalog(catalogContext);
    }

    @Override
    public String identifier() {
        return com.qlangtech.tis.plugin.paimon.catalog.HiveCatalog.HIVE_CATALOG_IDENTIFIER;
    }
}

package com.alibaba.datax.plugin.writer.paimonwriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.job.IJobContainerContext;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.IDataxReader;
import com.qlangtech.tis.datax.impl.DataxProcessor;
import com.qlangtech.tis.plugin.paimon.datax.DataxPaimonWriter;
import com.qlangtech.tis.plugin.paimon.datax.PaimonColumn;
import com.qlangtech.tis.plugin.paimon.datax.PaimonSelectedTab;
import com.qlangtech.tis.plugin.paimon.datax.writemode.WriteMode.PaimonTableWriter;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Catalog.TableNotExistException;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.table.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static com.alibaba.datax.plugin.writer.paimonwriter.Key.HADOOP_CONFIG;
import static com.alibaba.datax.plugin.writer.paimonwriter.Key.HADOOP_SECURITY_AUTHENTICATION_KEY;
import static com.alibaba.datax.plugin.writer.paimonwriter.Key.HAVE_KERBEROS;
import static com.alibaba.datax.plugin.writer.paimonwriter.Key.KERBEROS_KEYTAB_FILE_PATH;
import static com.alibaba.datax.plugin.writer.paimonwriter.Key.KERBEROS_PRINCIPAL;
import static com.alibaba.datax.plugin.writer.paimonwriter.Key.PAIMON_DB_NAME;
import static com.alibaba.datax.plugin.writer.paimonwriter.Key.PAIMON_TABLE_NAME;
import static com.alibaba.datax.plugin.writer.paimonwriter.Key.SOURCE_TABLE_NAME;
import static com.alibaba.datax.plugin.writer.paimonwriter.PaimonWriterErrorCode.KERBEROS_LOGIN_ERROR;
import static com.alibaba.datax.plugin.writer.paimonwriter.PaimonWriterErrorCode.PAIMON_ERROR_DB;
import static com.alibaba.datax.plugin.writer.paimonwriter.PaimonWriterErrorCode.PAIMON_ERROR_TABLE;

public class PaimonWriter extends Writer {

    private static final Logger LOG = LoggerFactory.getLogger(PaimonWriter.class);

    public static class Job extends Writer.Job {
        private Configuration originalConfig;

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            List<Configuration> list = new ArrayList<>();
            for (int i = 0; i < mandatoryNumber; i++) {
                list.add(originalConfig.clone());
            }
            return list;
        }

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();
        }

        @Override
        public void destroy() {

        }
    }


    public static class Task extends Writer.Task {
        // private String primaryKey;
        //private String partitionFields;
        // private String writeOption;
        //  private int batchSize;
        private Configuration sliceConfig;
        // private List<Configuration> columnsList;

        private String catalogPath;
        private String catalogType;
        //  private  catalog;
        private Table table;
        private PaimonTableWriter paimonTableWriter;
        private int bucket;
        private String hiveConfDir;
        private String hadoopConfDir;
        private String metastoreUri;
        private String coreSitePath;
        private String hdfsSitePath;
        private org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
        private List<PaimonColumn> paimonCols;
        private DataxPaimonWriter paimonWriter;
        private List<String> primaryKeys;

        @Override
        public void init() {
            IJobContainerContext context = Objects.requireNonNull(this.containerContext);
            IDataxProcessor processor = DataxProcessor.load(null, context.getCollectionName());
            paimonWriter = (DataxPaimonWriter) processor.getWriter(null);
            IDataxReader reader = processor.getReader(null);
            //获取与本task相关的配置
            this.sliceConfig = super.getPluginJobConf();
            String sourceTableName = sliceConfig.getNecessaryValue(SOURCE_TABLE_NAME, PAIMON_ERROR_TABLE);
            PaimonSelectedTab selectedTab = (PaimonSelectedTab) Objects.requireNonNull(
                    reader.getSelectedTab(sourceTableName), "sourceTableName:" + sourceTableName + " can not be null");

            this.primaryKeys = selectedTab.getPrimaryKeys();

            this.paimonCols
                    = paimonWriter.createPaimonCols(
                    selectedTab, context.getTransformerBuildCfg().map((transformer) -> transformer.getTransformersRules()));

            String tableName = sliceConfig.getNecessaryValue(PAIMON_TABLE_NAME, PAIMON_ERROR_TABLE);
            String dbName = sliceConfig.getNecessaryValue(PAIMON_DB_NAME, PAIMON_ERROR_DB);


//            catalogPath = sliceConfig.getNecessaryValue(PAIMON_CATALOG_PATH, PAIMON_PARAM_LOST);
//            catalogType = sliceConfig.getNecessaryValue(PAIMON_CATALOG_TYPE, PAIMON_PARAM_LOST);
            bucket = paimonWriter.tableBucket; //sliceConfig.getInt(PAIMON_TABLE_BUCKET, 2);
            // batchSize = paimonWriter. sliceConfig.getInt(PAIMON_BATCH_SIZE, 10);


            // writeOption = sliceConfig.getNecessaryValue(PAIMON_WRITE_OPTION, PAIMON_PARAM_LOST);


            //partitionFields = sliceConfig.getString(PAIMON_PARTITION_FIELDS);
            // primaryKey = sliceConfig.getString(PAIMON_PRIMARY_KEY);
            //  columnsList = sliceConfig.getListConfiguration(PAIMON_COLUMN);

            Configuration hadoopSiteParams = sliceConfig.getConfiguration(HADOOP_CONFIG);
            JSONObject hadoopSiteParamsAsJsonObject = JSON.parseObject(sliceConfig.getString(HADOOP_CONFIG));
            if (null != hadoopSiteParams) {
                Set<String> paramKeys = hadoopSiteParams.getKeys();
                for (String each : paramKeys) {
                    if (each.equals("hdfsUser")) {
                        System.setProperty("HADOOP_USER_NAME", hadoopSiteParamsAsJsonObject.getString(each));
                    } else if (each.equals("coreSitePath")) {
                        coreSitePath = hadoopSiteParamsAsJsonObject.getString(each);
                    } else if (each.equals("hdfsSitePath")) {
                        hdfsSitePath = hadoopSiteParamsAsJsonObject.getString(each);
                    } else {
                        hadoopConf.set(each, hadoopSiteParamsAsJsonObject.getString(each));
                    }
                }
            }

            try {
                //是否有Kerberos认证
                Boolean haveKerberos = sliceConfig.getBool(HAVE_KERBEROS, false);
                if (haveKerberos) {
                    String kerberosKeytabFilePath = sliceConfig.getString(KERBEROS_KEYTAB_FILE_PATH);
                    String kerberosPrincipal = sliceConfig.getString(KERBEROS_PRINCIPAL);
                    hadoopConf.set(HADOOP_SECURITY_AUTHENTICATION_KEY, "kerberos");
                    this.kerberosAuthentication(kerberosPrincipal, kerberosKeytabFilePath, hadoopConf);
                }


                try (Catalog catalog = paimonWriter.createCatalog()) {
                    Pair<Boolean, Table> tabExist = tableExists(catalog, dbName, tableName);
                    if (!tabExist.getKey()) {
                        throw new IllegalStateException(dbName.concat("." + tableName) + " must be exist");
                    } else {
                        table = tabExist.getValue();
                    }
                }

                paimonTableWriter = paimonWriter.createWriter(this.containerContext.getTaskId(), Objects.requireNonNull(table));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void prepare() {

        }

        @Override
        public void startWrite(RecordReceiver recordReceiver) {
            Record record;
            long num = 0;
            long commitIdentifier = 0;
            PaimonColumn pcol = null;
            Column column = null;
            int[] pksHash = new int[this.primaryKeys.size()];
            int pkIdx = 0;
            Object paimonFieldVal = null;
            while ((record = recordReceiver.getFromReader()) != null) {
                Arrays.fill(pksHash, 0);
                pkIdx = 0;
                GenericRow row = new GenericRow(paimonCols.size());
                for (int i = 0; i < paimonCols.size(); i++) {
                    pcol = paimonCols.get(i);
                    // String columnType = configuration.getString("type");
                    column = record.getColumn(i);
                    Object rawData = column.getRawData();

                    if (rawData == null) {
                        row.setField(i, null);
                        continue;
                    }
                    paimonFieldVal = pcol.getPaimonFieldVal(column);
                    row.setField(i, paimonFieldVal);
                    if (pcol.isPrimaryKey()) {
                        pksHash[pkIdx++] = paimonFieldVal.hashCode();
                    }
                }


                try {
                    this.paimonTableWriter.writeRow(row, this.bucket > 0 ? (Objects.hash(pksHash) % this.bucket) : 0);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }


            try {
                this.paimonTableWriter.instantCommitAfter();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }


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

        private void kerberosAuthentication(String kerberosPrincipal, String kerberosKeytabFilePath, org.apache.hadoop.conf.Configuration hadoopConf) {
            if (StringUtils.isNotBlank(kerberosPrincipal) && StringUtils.isNotBlank(kerberosKeytabFilePath)) {
                UserGroupInformation.setConfiguration(hadoopConf);
                try {
                    UserGroupInformation.loginUserFromKeytab(kerberosPrincipal, kerberosKeytabFilePath);
                } catch (Exception e) {
                    String message = String.format("kerberos认证失败,请确定kerberosKeytabFilePath[%s]和kerberosPrincipal[%s]填写正确",
                            kerberosKeytabFilePath, kerberosPrincipal);
                    LOG.error(message);
                    throw DataXException.asDataXException(KERBEROS_LOGIN_ERROR, e);
                }
            }
        }

        @Override
        public void post() {

        }

        @Override
        public void destroy() {

        }
    }

}

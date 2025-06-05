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
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Catalog.TableNotExistException;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static com.alibaba.datax.plugin.writer.paimonwriter.Key.HADOOP_CONFIG;
import static com.alibaba.datax.plugin.writer.paimonwriter.Key.HADOOP_SECURITY_AUTHENTICATION_KEY;
import static com.alibaba.datax.plugin.writer.paimonwriter.Key.HAVE_KERBEROS;
import static com.alibaba.datax.plugin.writer.paimonwriter.Key.KERBEROS_KEYTAB_FILE_PATH;
import static com.alibaba.datax.plugin.writer.paimonwriter.Key.KERBEROS_PRINCIPAL;
import static com.alibaba.datax.plugin.writer.paimonwriter.Key.PAIMON_CONFIG;
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
        private Catalog catalog;
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

                catalog = paimonWriter.createCatalog();

                if (!tableExists(catalog, dbName, tableName)) {
                    LOG.info("{} 表不存在，开始创建...", dbName.concat("." + tableName));
                    createTable(catalog, dbName, tableName, paimonCols, selectedTab.getPrimaryKeys(), selectedTab.partitionPathFields);
                }

                table = getTable(catalog, dbName, tableName);
                paimonTableWriter = paimonWriter.createWriter(Objects.requireNonNull(table));
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
            PaimonColumn configuration = null;
            Column column = null;
            while ((record = recordReceiver.getFromReader()) != null) {

                GenericRow row = new GenericRow(paimonCols.size());
                for (int i = 0; i < paimonCols.size(); i++) {
                    configuration = paimonCols.get(i);
                    // String columnType = configuration.getString("type");
                    column = record.getColumn(i);
                    Object rawData = column.getRawData();

                    if (rawData == null) {
                        row.setField(i, null);
                        continue;
                    }

                    row.setField(i, configuration.getPaimonFieldVal(column));
                }
                try {
                    this.paimonTableWriter.writeRow(row, bucket);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }


            try {
                this.paimonTableWriter.flushCache();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }


        }


        public void createTable(Catalog catalog, String dbName, String tableName, List<PaimonColumn> cols, List<String> pks, List<String> partKeys) {

            Configuration paimonTableParams = sliceConfig.getConfiguration(PAIMON_CONFIG);
            JSONObject paimonParamsAsJsonObject = JSON.parseObject(sliceConfig.getString(PAIMON_CONFIG));

            Schema.Builder schemaBuilder = Schema.newBuilder();

            if (null != paimonTableParams) {
                Set<String> paramKeys = paimonTableParams.getKeys();
                for (String each : paramKeys) {
                    schemaBuilder.option(each, paimonParamsAsJsonObject.getString(each));
                }
            }
            Objects.requireNonNull(this.paimonWriter, "paimonWriter can not be null")
                    .initializeSchemaBuilder(schemaBuilder);

            for (PaimonColumn columnConfig : cols) {
//                String columnName = columnConfig.getString("name");
//                DataType columnType = getPaimonDataType(columnConfig.getString("type"));
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
                throw new RuntimeException("table not exist");
            } catch (Catalog.DatabaseNotExistException e) {
                throw new RuntimeException("database: '" + dbName + "' not exist");
            }

        }

//        public int getMatchValue(String typeName) {
//
//            //获取长度
//            String regex = "\\((\\d+)\\)";
//            Pattern pattern = Pattern.compile(regex);
//            Matcher matcher = pattern.matcher(typeName);
//            int res = 0;
//
//            if (matcher.find()) {
//                res = Integer.parseInt(matcher.group(1));
//            } else {
//                LOG.error("{}:类型错误，请检查！", typeName);
//            }
//            return res;
//        }

//        public Pair<Integer, Integer> getDecValue(String typeName) {
//
//            String regex = "dd\\((\\d+), (\\d+)\\)";
//
//            Pattern pattern = Pattern.compile(regex);
//            Matcher matcher = pattern.matcher(typeName.trim());
//            int left = 0;
//            int right = 0;
//
//            if (matcher.find()) {
//                left = Integer.parseInt(matcher.group(1));
//                right = Integer.parseInt(matcher.group(2));
//            } else {
//                LOG.error("{}:类型错误，请检查！", typeName);
//            }
//
//            return Pair.of(left, right);
//
//        }

//        public DataType getPaimonDataType(String typeName) {
//
//            String type = typeName.toUpperCase();
//            DataType dt = DataTypes.STRING();
//
//            if (type.equals("BINARY") && !type.contains("VARBINARY")) {
//                dt = type.contains("(") ? new BinaryType(getMatchValue(type.trim())) : new BinaryType();
//            } else if (type.contains("VARBINARY")) {
//                dt = type.contains("(") ? new VarBinaryType(getMatchValue(type.trim())) : new VarBinaryType();
//            } else if (type.contains("STRING")) {
//                dt = VarCharType.STRING_TYPE;
//            } else if (type.contains("VARCHAR")) {
//                dt = type.contains("(") ? new VarCharType(getMatchValue(type.trim())) : new VarCharType();
//            } else if (type.contains("CHAR")) {
//                if (type.contains("NOT NULL")) {
//                    dt = new CharType().copy(false);
//                } else if (type.contains("(")) {
//                    dt = new CharType(getMatchValue(type.trim()));
//                } else {
//                    dt = new CharType();
//                }
//            } else if (type.contains("BOOLEAN")) {
//                dt = new BooleanType();
//            } else if (type.contains("BYTES")) {
//                dt = new VarBinaryType(VarBinaryType.MAX_LENGTH);
//            } else if (type.contains("DEC")) { // 包含 DEC 和 DECIMAL
//                if (type.contains(",")) {
//                    dt = new DecimalType(getDecValue(type).getLeft(), getDecValue(type).getRight());
//                } else if (type.contains("(")) {
//                    dt = new DecimalType(getMatchValue(type.trim()));
//                } else {
//                    dt = new DecimalType();
//                }
//            } else if (type.contains("NUMERIC") || type.contains("DECIMAL")) {
//                if (type.contains(",")) {
//                    dt = new DecimalType(getDecValue(type).getLeft(), getDecValue(type).getRight());
//                } else if (type.contains("(")) {
//                    dt = new DecimalType(getMatchValue(type.trim()));
//                } else {
//                    dt = new DecimalType();
//                }
//            } else if (type.equals("INT")) {
//                dt = new IntType();
//            } else if (type.equals("BIGINT") || type.equals("LONG")) {
//                dt = new BigIntType();
//            } else if (type.equals("TINYINT")) {
//                dt = new TinyIntType();
//            } else if (type.equals("SMALLINT")) {
//                dt = new SmallIntType();
//            } else if (type.equals("INTEGER")) {
//                dt = new IntType();
//            } else if (type.contains("FLOAT")) {
//                dt = new FloatType();
//            } else if (type.contains("DOUBLE")) {
//                dt = new DoubleType();
//            } else if (type.contains("DATE")) {
//                dt = new DateType();
//            } else if (type.contains("TIME")) {
//                dt = type.contains("(") ? new TimeType(getMatchValue(type.trim())) : new TimeType();
//            } else if (type.contains("TIMESTAMP")) {
//                switch (type) {
//                    case "TIMESTAMP":
//                    case "TIMESTAMP WITHOUT TIME ZONE":
//                        dt = new TimestampType();
//                        break;
//                    case "TIMESTAMP(3)":
//                    case "TIMESTAMP(3) WITHOUT TIME ZONE":
//                        dt = new TimestampType(3);
//                        break;
//                    case "TIMESTAMP WITH LOCAL TIME ZONE":
//                    case "TIMESTAMP_LTZ":
//                        dt = new LocalZonedTimestampType();
//                        break;
//                    case "TIMESTAMP(3) WITH LOCAL TIME ZONE":
//                    case "TIMESTAMP_LTZ(3)":
//                        dt = new LocalZonedTimestampType(3);
//                        break;
//                    default:
//                        LOG.error("{}:类型错误，请检查！", type);
//                }
//            } else {
//                throw new UnsupportedOperationException(
//                        "Not a supported type: " + typeName);
//            }
//
//            return dt;
//
//        }

        public Table getTable(Catalog catalog, String dbName, String tableName) {
            try {
                Identifier identifier = Identifier.create(dbName, tableName);
                return catalog.getTable(identifier);
            } catch (Catalog.TableNotExistException e) {
                throw new RuntimeException("table not exist", e);
            }
        }

        public boolean tableExists(Catalog catalog, String dbName, String tableName) {
            Identifier identifier = Identifier.create(dbName, tableName);
            try {
                catalog.getTable(identifier);
                return true;
            } catch (TableNotExistException e) {
                return false;
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

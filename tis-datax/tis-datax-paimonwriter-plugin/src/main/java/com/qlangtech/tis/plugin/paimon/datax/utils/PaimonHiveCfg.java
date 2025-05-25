//package com.qlangtech.tis.plugin.paimon.datax.utils;
//
//import com.qlangtech.tis.extension.Describable;
//import com.qlangtech.tis.extension.Descriptor;
//import com.qlangtech.tis.extension.TISExtension;
//import com.qlangtech.tis.plugin.annotation.FormField;
//import com.qlangtech.tis.plugin.annotation.FormFieldType;
//import com.qlangtech.tis.plugin.annotation.Validator;
//
///**
// * <pre>
// * 参数	说明	默认值
// * hive.table.owner	Hive 表的所有者（用于 Hive 元数据同步）	无
// * hive.storage.format	Hive 表存储格式（如 ORC、Parquet）	ORC
// * hive.partition.extractor	分区字段提取方式（如 default 或自定义类）	default
// * </pre>
// *
// * @author: 百岁（baisui@qlangtech.com）
// * @create: 2025-05-15 15:53
// * @see org.apache.paimon.hive.HiveCatalogOptions
// **/
//public class PaimonHiveCfg implements Describable<PaimonHiveCfg> {
//
////    @FormField(ordinal = 1, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.db_col_name})
////    public String tableOwner;
//
//
//
//    @TISExtension
//    public static final class DefaultDescriptor extends Descriptor<PaimonHiveCfg> {
//        public DefaultDescriptor() {
//            super();
//        }
//    }
//}

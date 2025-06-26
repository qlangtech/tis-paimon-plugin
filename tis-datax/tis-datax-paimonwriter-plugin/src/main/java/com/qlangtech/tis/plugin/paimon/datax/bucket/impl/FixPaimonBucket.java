package com.qlangtech.tis.plugin.paimon.datax.bucket.impl;

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.extension.util.OverwriteProps;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.paimon.datax.bucket.PaimonBucket;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import org.apache.paimon.CoreOptions;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-06-25 14:50
 **/
public class FixPaimonBucket extends PaimonBucket {

    @FormField(ordinal = 1, type = FormFieldType.INT_NUMBER, validate = {Validator.require, Validator.integer})
    public Integer bucketNum;

    @Override
    public int getBucketCount() {
        return this.bucketNum;
    }

    @TISExtension
    public static class Desc extends BasicDesc {

        public Desc() {
            super();
            OverwriteProps overwriteProps = new OverwriteProps();
            overwriteProps.setDftVal(null);
            this.opts.add("bucketNum", CoreOptions.BUCKET, overwriteProps);
        }

        public boolean validateBucketNum(IFieldErrorHandler msgHandler, Context context, String fieldName, String val) {
            int bucketNum = Integer.parseInt(val);
            if (bucketNum < 1) {
                msgHandler.addFieldError(context, fieldName, "必须大于0");
                return false;
            }
            return true;
        }

        @Override
        public String getDisplayName() {
            return "Fixed";
        }
    }
}

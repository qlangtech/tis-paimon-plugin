package com.qlangtech.tis.plugin.paimon.datax.pt;

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.paimon.datax.PaimonPartition;
import com.qlangtech.tis.plugin.paimon.datax.PaimonSelectedTab;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import org.apache.commons.collections.CollectionUtils;

import java.util.List;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-06-12 10:06
 **/
public class OnPaimonPartition extends PaimonPartition {
    /**
     * 分区字段
     */
    @FormField(ordinal = 4, type = FormFieldType.ENUM, validate = {Validator.require})
    public List<String> partitionPathFields;

    @Override
    public List<String> getPartitionKeys() {
        return this.partitionPathFields;
    }

    @TISExtension
    public static final class Desc extends Descriptor<PaimonPartition> {
        public Desc() {
            super();
        }

        @Override
        public boolean secondVerify(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals, PostFormVals parentPostFormVals) {

            PaimonSelectedTab tab = parentPostFormVals.newInstance();
            List<String> pks = tab.primaryKeys;

            OnPaimonPartition paimonPartition = postFormVals.newInstance();

            List<String> pts = paimonPartition.getPartitionKeys();

            if (CollectionUtils.isNotEmpty(pts) && !CollectionUtils.isSubCollection(pts, pks)) {
                msgHandler.addFieldError(context
                        , "partitionPathFields"
                        , "按照Paimon表设置规则，分区键须要包含在主键中(" + String.join(",", pks) + ")");
                return false;
            }


            return true;
        }

        @Override
        public String getDisplayName() {
            return SWITCH_ON;
        }
    }
}

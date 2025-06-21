package com.qlangtech.tis.plugin.paimon.datax.sequence;

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.extension.util.OverwriteProps;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.paimon.datax.PaimonPropAssist;
import com.qlangtech.tis.plugin.paimon.datax.PaimonPropAssist.PaimonOptions;
import com.qlangtech.tis.plugin.paimon.datax.PaimonSelectedTab;
import com.qlangtech.tis.plugin.paimon.datax.PaimonSequenceFields;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import org.apache.commons.collections.CollectionUtils;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.schema.Schema.Builder;

import java.util.List;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-06-21 09:31
 **/
public class PaimonSequenceFieldsOn extends PaimonSequenceFields {

    public static final String FIELD_SORT_ORDER = "sortOrder";

    @FormField(ordinal = 1, type = FormFieldType.ENUM, validate = {Validator.require})
    public List<String> field;

    @FormField(ordinal = 2, advance = true, type = FormFieldType.ENUM, validate = {Validator.require})
    public String sortOrder;

    @Override
    public void initializeSchemaBuilder(Builder schemaBuilder, PaimonSelectedTab tab) {
        Desc descriptor = (Desc) this.getDescriptor();
        descriptor.opts.setTarget((f, val) -> {
            schemaBuilder.option(f.key(), (val instanceof List) ? String.join(",", (List) val) : String.valueOf(val));
        }, this);
    }

    @TISExtension
    public static class Desc extends Descriptor<PaimonSequenceFields> {
        public PaimonOptions<PaimonSequenceFields> opts;

        @Override
        public String getDisplayName() {
            return SWITCH_ON;
        }

        @Override
        public boolean secondVerify(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals, PostFormVals parentPostFormVals) {
            // return super.secondVerify(msgHandler, context, postFormVals, parentPostFormVals);
            PaimonSelectedTab tab = parentPostFormVals.newInstance();
            List<String> pks = tab.primaryKeys;

            PaimonSequenceFieldsOn sequenceFields = postFormVals.newInstance();

            List<String> fields = sequenceFields.field;

            if (CollectionUtils.isNotEmpty(fields) && !CollectionUtils.isSubCollection(fields, pks)) {
                msgHandler.addFieldError(context
                        , FIELD_SORT_ORDER
                        , "按照Paimon表设置规则，sequence键须要包含在主键中(" + String.join(",", pks) + ")");
                return false;
            }
            return true;
        }

        public Desc() {
            super();
            this.opts = PaimonPropAssist.createOpts(this);
            OverwriteProps fieldPropOverWrite = new OverwriteProps();
            fieldPropOverWrite.setAppendHelper("\n 一个单调递增的字段（如时间戳、版本号），用于解决更新冲突。当主键相同时");
            opts.add("field", CoreOptions.SEQUENCE_FIELD, fieldPropOverWrite);
            opts.add(FIELD_SORT_ORDER, CoreOptions.SEQUENCE_FIELD_SORT_ORDER);
        }
    }
}

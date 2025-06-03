package com.qlangtech.tis.plugin.paimon.datax;

import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.manage.common.Option;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.SelectedTab;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-05-17 00:53
 **/
public class PaimonSelectedTab extends SelectedTab {

    /**
     * 分区字段
     */
    @FormField(ordinal = 4, type = FormFieldType.ENUM, validate = {Validator.require})
    public List<String> partitionPathFields;


    public static List<Option> getPtCandidateFields() {
        return SelectedTab.getContextOpts((cols) -> cols.stream().filter((col) -> {
            switch (col.getType().getCollapse()) {
                // case STRING:
                case INT:
                case Long:
                case Date:
                    return true;
            }
            return false;
        }));
    }

    /**
     * 主键候选字段
     *
     * @return
     */
    public static List<Option> getPaimonPrimaryKeys() {
        return SelectedTab.getContextTableCols((cols) -> cols.stream() //
                .filter((col) -> col.isPk())).stream().map((c) -> new Option(c.getName())).collect(Collectors.toList());
    }

    public static List<String> getDeftRecordKeys() {
        return getPaimonPrimaryKeys().stream().map((pk) -> String.valueOf(pk.getValue())).collect(Collectors.toList());
    }

    @TISExtension
    public static final class DefaultDescriptor extends Descriptor<SelectedTab> {
        public DefaultDescriptor() {
            super();
        }
    }
}

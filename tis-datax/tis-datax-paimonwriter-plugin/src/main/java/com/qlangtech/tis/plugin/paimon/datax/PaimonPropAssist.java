package com.qlangtech.tis.plugin.paimon.datax;

import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.util.AbstractPropAssist;
import com.qlangtech.tis.manage.common.Option;
import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.options.description.Description;
import org.apache.paimon.options.description.HtmlFormatter;

import java.util.List;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-05-16 10:48
 **/
public class PaimonPropAssist<T extends Describable> extends AbstractPropAssist<T, org.apache.paimon.options.ConfigOption> {

    public PaimonPropAssist(Descriptor<T> descriptor) {
        super(descriptor);
    }

    public static <T extends Describable, PLUGIN extends T> Options<PLUGIN, org.apache.paimon.options.ConfigOption>
    createOpts(Descriptor<T> descriptor) {
        PaimonPropAssist props = new PaimonPropAssist(descriptor);
        return props.createFlinkOptions();
    }

    @Override
    protected String getDescription(ConfigOption configOption) {
        Description desc = configOption.description();
        HtmlFormatter formatter = new HtmlFormatter();
        return formatter.format(desc);
    }

    @Override
    protected Object getDefaultValue(ConfigOption configOption) {
        return configOption.defaultValue();
    }

    @Override
    protected List<Option> getOptEnums(ConfigOption configOption) {
        return null;
    }

    @Override
    protected String getDisplayName(ConfigOption configOption) {
        return configOption.key();
    }
}

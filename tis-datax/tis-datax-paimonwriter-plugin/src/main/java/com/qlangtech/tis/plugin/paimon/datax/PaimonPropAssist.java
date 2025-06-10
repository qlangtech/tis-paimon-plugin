package com.qlangtech.tis.plugin.paimon.datax;

import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.util.AbstractPropAssist;
import com.qlangtech.tis.extension.util.OverwriteProps;
import com.qlangtech.tis.extension.util.PropValFilter;
import com.qlangtech.tis.manage.common.Option;
import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.options.description.Description;
import org.apache.paimon.options.description.HtmlFormatter;
import org.apache.paimon.utils.TimeUtils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.List;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-05-16 10:48
 **/
public class PaimonPropAssist<T extends Describable> extends AbstractPropAssist<T, org.apache.paimon.options.ConfigOption> {

    public PaimonPropAssist(Descriptor<T> descriptor) {
        super(descriptor);
    }

    public static <PLUGIN extends Describable> PaimonOptions<PLUGIN>
    createOpts(Descriptor<PLUGIN> descriptor) {
        PaimonPropAssist props = new PaimonPropAssist(descriptor);
        return props.createOptions();
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

    @Override
    protected PaimonOptions<T> createOptions() {
        return new PaimonOptions(this);
    }

    public static class PaimonOptions<T extends Describable> extends Options<T, org.apache.paimon.options.ConfigOption> {

        private static final Method getClazzMehod;

        static {
            try {
                getClazzMehod = ConfigOption.class.getDeclaredMethod("getClazz", null);
                getClazzMehod.setAccessible(true);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        public PaimonOptions(AbstractPropAssist<T, org.apache.paimon.options.ConfigOption> propsAssist) {
            super(propsAssist);
        }

        @Override
        public void add(String fieldName, TISAssistProp<org.apache.paimon.options.ConfigOption> option, PropValFilter propValFilter) {
            ConfigOption configOption = option.getConfigOption();

            try {
                Class<?> fieldType = (Class<?>) getClazzMehod.invoke(configOption);
                if (fieldType == Duration.class && propValFilter == null) {
                    propValFilter = new PropValFilter() {
                        @Override
                        public Object apply(Object o) {
                            // 值将会在解析 org.apache.paimon.utils.TimeUtils.parseDuration(TimeUtils.java:78)
                            return TimeUtils.formatWithHighestUnit((Duration) o);
                        }
                    };
                } else if (fieldType == org.apache.paimon.options.MemorySize.class && propValFilter == null) {
                    propValFilter = new PropValFilter() {
                        @Override
                        public Object apply(Object o) {
                            // 值将会在解析 org.apache.paimon.utils.TimeUtils.parseDuration(TimeUtils.java:78)
                            return ((org.apache.paimon.options.MemorySize) o).toString();
                        }
                    };
                }
            } catch (Exception e) {
                throw new RuntimeException("fieldName:" + fieldName, e);
            }


            super.add(fieldName, option, propValFilter);
        }

    }
}

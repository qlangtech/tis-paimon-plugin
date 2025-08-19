package com.qlangtech.tis.plugin.paimon.datax;

import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.util.AbstractPropAssist;
import com.qlangtech.tis.extension.util.OverwriteProps;
import com.qlangtech.tis.extension.util.PropValFilter;
import com.qlangtech.tis.manage.common.Option;
import com.qlangtech.tis.plugin.paimon.catalog.cache.CatalogCacheON;
import org.apache.commons.lang3.EnumUtils;
import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.description.DescribedEnum;
import org.apache.paimon.options.description.Description;
import org.apache.paimon.options.description.HtmlFormatter;
import org.apache.paimon.utils.TimeUtils;

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

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
    protected MarkdownHelperContent getDescription(ConfigOption configOption) {
        Description desc = configOption.description();
        HtmlFormatter formatter = new HtmlFormatter();
        return new MarkdownHelperContent(formatter.format(desc));
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

        public void addMemorySize(String fieldName, ConfigOption<MemorySize> option) {
            this.addMemorySize(fieldName, option, null);
        }

        public void addMemorySize(String fieldName, ConfigOption<MemorySize> option, Consumer<OverwriteProps> memoryOverwriteConsumer) {

            OverwriteProps memoryOverwrite = new OverwriteProps();
            memoryOverwrite.dftValConvert = (val) -> {
                return com.qlangtech.tis.plugin.MemorySize.ofBytes(((org.apache.paimon.options.MemorySize) val).getBytes());
            };
            if (memoryOverwriteConsumer != null) {
                //  memoryOverwrite.setLabelRewrite(dftLableRewrite);
                memoryOverwriteConsumer.accept(memoryOverwrite);
            }
            this.add(fieldName, TISAssistProp.create(option).setOverwriteProp(memoryOverwrite), CatalogCacheON.paimonMemorySizeProcess);
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
                } else if (fieldType == Boolean.class) {
                    option.overwriteBooleanEnums();
                } else if (DescribedEnum.class.isAssignableFrom(fieldType)) {
                    List<Enum> enums = EnumUtils.getEnumList((Class<Enum>) fieldType);
                    List<Option> opts = enums.stream().map((e) -> new Option(e.name())).collect(Collectors.toList());
                    TISAssistProp.set(option, opts, configOption.defaultValue());
                }
            } catch (Exception e) {
                throw new RuntimeException("fieldName:" + fieldName, e);
            }


            super.add(fieldName, option, propValFilter);
        }

    }
}

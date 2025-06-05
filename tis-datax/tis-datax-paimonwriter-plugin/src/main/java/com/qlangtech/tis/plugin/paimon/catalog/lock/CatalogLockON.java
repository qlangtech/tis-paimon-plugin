package com.qlangtech.tis.plugin.paimon.catalog.lock;

import com.beust.jcommander.internal.Lists;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.extension.util.AbstractPropAssist.Options;
import com.qlangtech.tis.extension.util.AbstractPropAssist.TISAssistProp;
import com.qlangtech.tis.extension.util.OverwriteProps;
import com.qlangtech.tis.extension.util.PropValFilter;
import com.qlangtech.tis.manage.common.Option;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.paimon.catalog.CatalogLock;
import com.qlangtech.tis.plugin.paimon.datax.PaimonPropAssist;
import org.apache.commons.lang.StringUtils;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.ConfigOption;

import java.time.Duration;
import java.util.function.Function;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-06-03 13:22
 **/
public class CatalogLockON extends CatalogLock {

    @FormField(ordinal = 0, type = FormFieldType.ENUM, validate = {})
    public String type;
    @FormField(ordinal = 1, type = FormFieldType.DURATION_OF_SECOND, validate = {Validator.require})
    public Duration checkMaxSleep;
    @FormField(ordinal = 2, type = FormFieldType.DURATION_OF_MINUTE, validate = {Validator.require})
    public Duration acquireTimeout;

    @Override
    public void setOptions(org.apache.paimon.options.Options options) {
        options.set(CatalogOptions.LOCK_ENABLED, true);
        Desc desc = (Desc) this.getDescriptor();
        desc.opts.setTarget((field, val) -> {
            options.set(field, (val));
        }, this);
    }

    @TISExtension
    public static class Desc extends Descriptor<CatalogLock> {
        Options<CatalogLock, ConfigOption> opts;

        @Override
        public String getDisplayName() {
            return SWITCH_ON;
        }

        public Desc() {
            super();
            Function<String, String> dftLableRewrite = (label) -> {
                return StringUtils.substringAfter(label, "lock-");
            };
            opts = PaimonPropAssist.createOpts(this);
            OverwriteProps overwriteType = new OverwriteProps();
            overwriteType.setLabelRewrite(dftLableRewrite);
            overwriteType.setEnumOpts(Lists.newArrayList(new Option("hive"), new Option("zookeeper")));
            opts.add("type", TISAssistProp.create(CatalogOptions.LOCK_TYPE).setOverwriteProp(overwriteType));

            OverwriteProps overwriteCheckMaxSleep = OverwriteProps.withAppendHelper("unit：Second").setLabelRewrite(dftLableRewrite);
//            overwriteCheckMaxSleep.dftValConvert = (val) -> {
//                Duration dur = (Duration) val;
//                return dur.toSeconds();
//            };

            opts.add("checkMaxSleep"
                    , TISAssistProp.create(CatalogOptions.LOCK_CHECK_MAX_SLEEP).setOverwriteProp(overwriteCheckMaxSleep)
//                    , new PropValFilter() {
//                        @Override
//                        public Object apply(Object lock) {
//                            Long val = ((CatalogLockON) lock).checkMaxSleep;
//                            return Duration.ofSeconds(val);
//                        }
//                    }
            );

            OverwriteProps overwriteAcquireTimeout = OverwriteProps.withAppendHelper("unit：Minute").setLabelRewrite(dftLableRewrite);
//            overwriteAcquireTimeout.dftValConvert = (val) -> {
//                Duration dur = (Duration) val;
//                return dur.toMinutes();
//            };
            opts.add("acquireTimeout"
                    , TISAssistProp.create(CatalogOptions.LOCK_ACQUIRE_TIMEOUT).setOverwriteProp(overwriteAcquireTimeout)
//                    , new PropValFilter() {
//                        @Override
//                        public Object apply(Object lock) {
//                            Long val = ((CatalogLockON) lock).acquireTimeout;
//                            return Duration.ofMinutes(val);
//                        }
//                    }
            );
        }
    }
}

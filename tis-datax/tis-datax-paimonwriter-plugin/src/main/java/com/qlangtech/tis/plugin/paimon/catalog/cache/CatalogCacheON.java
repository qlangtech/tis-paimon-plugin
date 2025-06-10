package com.qlangtech.tis.plugin.paimon.catalog.cache;

import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.extension.util.AbstractPropAssist;
import com.qlangtech.tis.extension.util.AbstractPropAssist.TISAssistProp;
import com.qlangtech.tis.extension.util.OverwriteProps;
import com.qlangtech.tis.extension.util.PropValFilter;
import com.qlangtech.tis.plugin.MemorySize;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.paimon.catalog.CatalogCache;
import com.qlangtech.tis.plugin.paimon.datax.PaimonPropAssist;
import com.qlangtech.tis.plugin.paimon.datax.PaimonPropAssist.PaimonOptions;
import org.apache.commons.lang.StringUtils;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.options.Options;

import java.time.Duration;
import java.util.function.Function;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-06-03 14:41
 **/
public class CatalogCacheON extends CatalogCache {

    public static final String KEY_FIELD_MANIFEST_SMALL_FIELD_MEMORY = "manifestSmallFileMemory";

    @FormField(ordinal = 1, type = FormFieldType.DURATION_OF_MINUTE, validate = {Validator.require, Validator.integer})
    public Duration expirationInterval;

    @FormField(ordinal = 2, type = FormFieldType.INT_NUMBER, validate = {Validator.require, Validator.integer})
    public Long partitionMaxNum;

    @FormField(ordinal = 3, type = FormFieldType.MEMORY_SIZE_OF_MEGA, validate = {Validator.require, Validator.integer})
    public MemorySize manifestSmallFileMemory;

    @FormField(ordinal = 4, type = FormFieldType.MEMORY_SIZE_OF_MEGA, validate = {Validator.require, Validator.integer})
    public MemorySize manifestSmallFileThreshold;

    @FormField(ordinal = 5, type = FormFieldType.MEMORY_SIZE_OF_MEGA, validate = {Validator.integer})
    public MemorySize manifestMaxMemory;

    @FormField(ordinal = 6, type = FormFieldType.INT_NUMBER, validate = {Validator.require, Validator.integer})
    public Integer snapshotMaxNumPerTable;

    @Override
    public void setOptions(Options options) {
        options.set(CatalogOptions.CACHE_ENABLED, true);
        Desc desc = (Desc) this.getDescriptor();
        desc.opts.setTarget((field, val) -> {
            options.set(field, (val));
        }, this);
    }

    @TISExtension
    public static class Desc extends Descriptor<CatalogCache> {
        PaimonOptions<CatalogCache> opts;

        public Desc() {
            super();
            Function<String, String> dftLableRewrite = (label) -> {
                return StringUtils.substringAfter(label, "cache.");
            };
            this.opts = PaimonPropAssist.createOpts(this);
            this.opts.add("expirationInterval"
                    , TISAssistProp.create(CatalogOptions.CACHE_EXPIRATION_INTERVAL_MS).overwriteLabel(dftLableRewrite));

            this.opts.add("partitionMaxNum"
                    , TISAssistProp.create(CatalogOptions.CACHE_PARTITION_MAX_NUM).overwriteLabel(dftLableRewrite));

            final PropValFilter paimonMemorySizeProcess = new PropValFilter() {
                @Override
                public Object apply(Object o) {
                    MemorySize ms = (MemorySize) o;
                    org.apache.paimon.options.MemorySize result = new org.apache.paimon.options.MemorySize(ms.getBytes());
                    return result;
                }
            };

            OverwriteProps memoryOverwrite = new OverwriteProps();
            memoryOverwrite.setLabelRewrite(dftLableRewrite);
            memoryOverwrite.dftValConvert = (val) -> {
                return MemorySize.ofBytes(((org.apache.paimon.options.MemorySize) val).getBytes());
            };

            this.opts.add(KEY_FIELD_MANIFEST_SMALL_FIELD_MEMORY
                    , TISAssistProp.create(CatalogOptions.CACHE_MANIFEST_SMALL_FILE_MEMORY).setOverwriteProp(memoryOverwrite)
                    , paimonMemorySizeProcess);

            this.opts.add("manifestSmallFileThreshold"
                    , TISAssistProp.create(CatalogOptions.CACHE_MANIFEST_SMALL_FILE_THRESHOLD).setOverwriteProp(memoryOverwrite)
                    , paimonMemorySizeProcess);

            this.opts.add("manifestMaxMemory"
                    , TISAssistProp.create(CatalogOptions.CACHE_MANIFEST_MAX_MEMORY).setOverwriteProp(memoryOverwrite)
                    , paimonMemorySizeProcess);

            this.opts.add("snapshotMaxNumPerTable"
                    , TISAssistProp.create(CatalogOptions.CACHE_SNAPSHOT_MAX_NUM_PER_TABLE).overwriteLabel(dftLableRewrite));

        }

        @Override
        public String getDisplayName() {
            return SWITCH_ON;
        }


    }
}

package com.qlangtech.tis.plugin.paimon.datax;

import org.apache.paimon.schema.Schema;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-05-18 17:01
 **/
public interface SchemaBuilderSetter {

    public void initializeSchemaBuilder(Schema.Builder schemaBuilder);

}

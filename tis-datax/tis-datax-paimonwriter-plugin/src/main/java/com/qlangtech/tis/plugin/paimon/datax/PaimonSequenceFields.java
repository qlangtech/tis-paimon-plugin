package com.qlangtech.tis.plugin.paimon.datax;

import com.qlangtech.tis.extension.Describable;

/**
 * 在 Apache Paimon 中配置 Sequence ID 主要用于处理多路写入（如 CDC 数据）时的更新顺序问题，确保在相同主键下保留最新版本的数据。以下是详细配置步骤和注意事项：
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-06-21 01:02
 **/
public abstract class PaimonSequenceFields implements Describable<PaimonSequenceFields>, SchemaBuilderSetter {


}

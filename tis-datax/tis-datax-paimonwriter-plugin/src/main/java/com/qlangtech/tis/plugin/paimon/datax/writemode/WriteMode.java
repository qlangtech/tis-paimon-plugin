package com.qlangtech.tis.plugin.paimon.datax.writemode;

import com.qlangtech.tis.extension.Describable;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.table.Table;

/**
 * paimon写入数据的方式，目前支持2种方式：1.batch_insert(按照官方的定义模式,每次只能有一次提交)，2.stream_insert(支持多次提交) <br />
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-05-15 15:21
 * @see com.qlangtech.tis.plugin.paimon.datax.writemode.BatchInsertWriteMode
 * @see StreamInsertWriteMode
 **/
public abstract class WriteMode implements Describable<WriteMode> {

    public abstract PaimonTableWriter createWriter(Table table);

    public interface PaimonTableWriter {
        public void writeRow(GenericRow row, int bucket) throws Exception;

        public void flushCache() throws Exception;
    }
}

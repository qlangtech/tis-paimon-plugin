package com.qlangtech.tis.plugin.paimon.datax.writemode;

import com.qlangtech.tis.plugin.paimon.datax.bucket.PaimonBucket;
import com.qlangtech.tis.plugin.paimon.datax.writemode.WriteMode.PaimonTableWriter;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.table.DataTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.TableWrite;

import java.util.Objects;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-06-26 13:17
 **/
public abstract class BasicPaimonTableWriter<TABLE_WRITER extends TableWrite> implements PaimonTableWriter {

    protected final TABLE_WRITER write;
    private final PaimonBucket tableBucket;
    private final DataTable table;

    public BasicPaimonTableWriter(TABLE_WRITER write, PaimonBucket tableBucket, Table table) {
        this.write = write;
        this.tableBucket = Objects.requireNonNull(tableBucket, "tableBucket can not be null");
        this.table = Objects.requireNonNull((DataTable) table);
    }

    @Override
    public void writeRow(GenericRow row) throws Exception {
        int bucket = tableBucket.getBucket(table, write, row); // write.getBucket(row);
        write.write(row, bucket);
    }
}

package com.qlangtech.tis.plugin.paimon.datax.writemode;

import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import org.apache.commons.collections.CollectionUtils;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;

import java.util.List;
import java.util.Objects;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-05-15 15:28
 **/
public class BatchInsertWriteMode extends WriteMode {

    @Override
    public PaimonTableWriter createWriter(Table table) {
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder().withOverwrite();
        BatchTableWrite write = writeBuilder.newWrite();
        return new PaimonTableWrite(write, writeBuilder);
    }

    private static class PaimonTableWrite implements PaimonTableWriter {
        private final BatchTableWrite write;
        private final BatchWriteBuilder writeBuilder;

        public PaimonTableWrite(BatchTableWrite write, BatchWriteBuilder writeBuilder) {
            this.write = write;
            this.writeBuilder = Objects.requireNonNull(writeBuilder);
        }

        @Override
        public void writeRow(GenericRow row, int bucket) throws Exception {
            write.write(row, bucket);
        }

        @Override
        public void flushCache() throws Exception {
            List<CommitMessage> messages = (write).prepareCommit();
            //Collect all CommitMessages to a global node and commit
            if (CollectionUtils.isEmpty(messages)) {
                throw new IllegalStateException("messages can not be empty");
            }
            BatchTableCommit commit = null;
            try {
                commit = writeBuilder.newCommit();
                if (commit == null) {
                    throw new RuntimeException("commit or messages info not exist");
                }
                commit.commit(messages);
            } catch (RuntimeException e) {
                if (commit != null) {
                    commit.abort(messages);
                }
                throw (e);
            }
        }
    }

    @TISExtension
    public static final class DefaultDescriptor extends Descriptor<WriteMode> {
        public DefaultDescriptor() {
            super();
        }

        @Override
        public String getDisplayName() {
            return "Batch";
        }
    }
}

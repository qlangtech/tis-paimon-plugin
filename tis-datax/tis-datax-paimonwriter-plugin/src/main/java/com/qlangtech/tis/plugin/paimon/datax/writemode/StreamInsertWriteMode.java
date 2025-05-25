package com.qlangtech.tis.plugin.paimon.datax.writemode;

import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.StreamWriteBuilder;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-05-15 15:29
 **/
public class StreamInsertWriteMode extends WriteMode {

    @FormField(ordinal = 0, type = FormFieldType.INT_NUMBER, validate = {Validator.require, Validator.integer})
    public Integer batchSize;


    @Override
    public PaimonTableWriter createWriter(Table table) {
        StreamWriteBuilder writeBuilder = table.newStreamWriteBuilder();
        StreamTableWrite write = writeBuilder.newWrite();
        return new PaimonStreamTableWrite(write, writeBuilder, this.batchSize);
    }

    private static class PaimonStreamTableWrite implements PaimonTableWriter {
        private final StreamTableWrite write;
        private final Integer batchSize;
        private AtomicLong counter = new AtomicLong(0);
        private AtomicLong commitIdentifier = new AtomicLong(0);
        private final StreamWriteBuilder writeBuilder;

        public PaimonStreamTableWrite(StreamTableWrite write, StreamWriteBuilder writeBuilder, Integer batchSize) {
            this.write = write;
            this.batchSize = batchSize;
            this.writeBuilder = writeBuilder;
        }

        @Override
        public void writeRow(GenericRow row, int bucket) throws Exception {
            write.write(row, bucket);

            long num = counter.incrementAndGet();
            if (num >= batchSize) {
                flush();
                counter.set(0L);
            }
        }

        private void flush() throws Exception {
            commitIdentifier.incrementAndGet();
            List<CommitMessage> streamMsgs = write.prepareCommit(false, commitIdentifier.get());
            // Collect all CommitMessages to a global node and commit
            StreamTableCommit commit = null;
            try {
                commit = writeBuilder.newCommit();
                commit.commit(commitIdentifier.get(), streamMsgs);
            } catch (Exception e) {
                if (commit != null) {
                    commit.abort(streamMsgs);
                }
                throw (e);
            }
        }

        @Override
        public void flushCache() throws Exception {
            if (counter.incrementAndGet() > 0) {
                this.flush();
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
            return "Stream";
        }
    }
}

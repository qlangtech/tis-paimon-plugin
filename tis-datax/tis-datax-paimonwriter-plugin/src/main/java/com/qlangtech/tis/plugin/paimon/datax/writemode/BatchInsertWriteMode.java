package com.qlangtech.tis.plugin.paimon.datax.writemode;

import com.beust.jcommander.internal.Lists;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import org.apache.commons.collections.CollectionUtils;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.io.DataInputViewStreamWrapper;
import org.apache.paimon.io.DataOutputView;
import org.apache.paimon.io.DataOutputViewStreamWrapper;
import org.apache.paimon.table.DataTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-05-15 15:28
 **/
public class BatchInsertWriteMode extends WriteMode {

    @Override
    public PaimonTableWriter createWriter(Integer taskId, Table table) {
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder().withOverwrite();
        BatchTableWrite write = writeBuilder.newWrite();
        // 只进行insert操作，最终不会执行compact操作
        write.withInsertOnly(true);
        return new PaimonBatchTableWrite(taskId, table, write, writeBuilder);
    }

    static class PaimonBatchTableWrite implements PaimonTableWriter {
        private static final Logger logger = LoggerFactory.getLogger(PaimonBatchTableWrite.class);
        private final BatchTableWrite write;
        private final BatchWriteBuilder writeBuilder;
        private final FileIO fileIO;
        private final Path tabLocation;
        private final Integer taskId;
        private final CommitMessageSerializer messageSerializer = new CommitMessageSerializer();

        public PaimonBatchTableWrite(Integer taskId, Table table, BatchTableWrite write, BatchWriteBuilder writeBuilder) {
            this.write = write;
            this.writeBuilder = Objects.requireNonNull(writeBuilder);
            this.fileIO = Objects.requireNonNull(table.fileIO(), "fileIO can not be null");
            this.tabLocation = ((DataTable) table).location();
            this.taskId = taskId;
        }

        @Override
        public void writeRow(GenericRow row, int bucket) throws Exception {
            write.write(row, bucket);
        }

        /**
         * 取得单次task下某表的全部commit message文件
         *
         * @return
         */
        private Path taskBatchRoot() {
            return new Path(tabLocation, "batch-sync/" + this.taskId);
        }

        @Override
        public void instantCommitAfter() throws Exception {
            List<CommitMessage> messages = (write).prepareCommit();
            Path hdfsPath = new Path(taskBatchRoot(), String.valueOf(UUID.randomUUID()));

            try (DataOutputViewStreamWrapper output = HdfsDataOutputViewUtils.getDataOutputView(this.fileIO, hdfsPath, true)) {
                messageSerializer.serializeList(messages, output);
            }

        }


        @Override
        public void offlineFlushCache() throws Exception {

            List<CommitMessage> commitMessages = Lists.newArrayList();
            FileStatus[] fileStatuses = this.fileIO.listFiles(taskBatchRoot(), false);
            for (FileStatus stat : fileStatuses) {
                try (DataInputViewStreamWrapper commitMessage = new DataInputViewStreamWrapper(this.fileIO.newInputStream(stat.getPath()))) {
                    commitMessages.addAll(messageSerializer.deserializeList(messageSerializer.getVersion(), commitMessage));
                }
            }

            if (CollectionUtils.isEmpty(commitMessages)) {
                throw new IllegalStateException("commitMessages can not be empty");
            }
            logger.info("batch commit files, commit message size:" + commitMessages.size());
            BatchTableCommit commit = null;
            try {
                commit = writeBuilder.newCommit();

                commit.commit(commitMessages);
                logger.info("batch commit success");
                this.fileIO.delete(taskBatchRoot(), true);
            } catch (Exception e) {
                commit.abort(commitMessages);
                throw e;
            } finally {
                try {
                    commit.close();
                } catch (Throwable e) {
                }
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

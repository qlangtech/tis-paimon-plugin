package com.qlangtech.tis.plugin.paimon.datax.writemode;

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.hadoop.HadoopFileIO;
import org.apache.paimon.io.DataOutputView;
import org.apache.paimon.io.DataOutputViewStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.paimon.io.DataOutputViewStreamWrapper;

import java.io.IOException;

public class HdfsDataOutputViewUtils {

    /**
     * 通过 HDFS 路径获取 DataOutputView 实例
     *
     * @param path  HDFS 路径 (e.g. "hdfs://namenode:9000/path/to/file")
     * @param overwrite 是否覆盖已存在文件
     * @return DataOutputView 实例
     */
    public static DataOutputViewStreamWrapper getDataOutputView(FileIO fileIO, Path path, boolean overwrite) throws IOException {
        // 1. 创建 Hadoop 配置
        // Configuration hadoopConf = new Configuration();
        // 根据需要配置 HDFS 参数（可选）
        // hadoopConf.set("dfs.replication", "3");

        // 2. 创建 Paimon 的 HadoopFileIO
        //  FileIO fileIO = new HadoopFileIO();
//        CatalogContext context = null;
//        fileIO.configure(context);

        // 3. 创建 Paimon Path 对象

        // 4. 创建输出流（支持覆盖选项）
        PositionOutputStream out = fileIO.newOutputStream(path, overwrite);

        // 5. 包装成 DataOutputView
        return new DataOutputViewStreamWrapper(out);
    }

    /**
     * 使用示例
     */
    public static void main(String[] args) throws IOException {
//        String hdfsPath = "hdfs://mycluster/data/paimon/commit_messages.bin";
//
//        try (DataOutputView outputView = getDataOutputView(hdfsPath, true)) {
//            // 写入示例数据
//            outputView.writeInt(42);
//            outputView.writeUTF("Hello Paimon");
//            outputView.writeBoolean(true);
//
//            // 写入 CommitMessage 列表
//            List<CommitMessage> messages = ...;
//            CommitMessageSerializer serializer = ...;
//            for (CommitMessage msg : messages) {
//                byte[] bytes = serializer.serialize(msg);
//                outputView.writeInt(bytes.length);
//                outputView.write(bytes);
//            }
//        }
    }
}

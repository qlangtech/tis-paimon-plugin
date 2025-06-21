package com.qlangtech.tis.plugin.paimon.datax.hook;

import com.qlangtech.tis.plugin.paimon.catalog.FileSystemCatalog;
import com.qlangtech.tis.plugin.paimon.catalog.HiveCatalog;
import com.qlangtech.tis.plugin.paimon.catalog.PaimonCatalog;
import com.qlangtech.tis.plugin.paimon.catalog.PaimonCatalogVisitor;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.session.SessionState;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-06-13 13:26
 **/
public class SessionStateUtil {

    /**
     * 设置当前会话session 防止以下异常
     * <pre>
     * 2025-06-13 13:49:32 ERROR mysql_paimon hive.ql.metadata.Hive- org.apache.hadoop.hive.ql.metadata.HiveException: Error in loading storage handler.org.apache.paimon.hive.PaimonStorageHandler
     * 	at org.apache.hadoop.hive.ql.metadata.HiveUtils.getStorageHandler(HiveUtils.java:310)
     * 	at org.apache.hadoop.hive.ql.metadata.Hive$5.getHook(Hive.java:3802)
     * 	at org.apache.hadoop.hive.metastore.HiveMetaStoreClient.getHook(HiveMetaStoreClient.java:2045)
     * 	at org.apache.hadoop.hive.metastore.HiveMetaStoreClient.createTable(HiveMetaStoreClient.java:820)
     * 	at org.apache.hadoop.hive.metastore.HiveMetaStoreClient.createTable(HiveMetaStoreClient.java:813)
     * 	at java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
     * 	at java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
     * 	at java.base/jdk.internal.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
     * 	at java.base/java.lang.reflect.Method.invoke(Method.java:566)
     * 	at org.apache.hadoop.hive.metastore.RetryingMetaStoreClient.invoke(RetryingMetaStoreClient.java:154)
     * 	at com.sun.proxy.$Proxy19.createTable(Unknown Source)
     * 	at java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
     * 	at java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
     * 	at java.base/jdk.internal.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
     * 	at java.base/java.lang.reflect.Method.invoke(Method.java:566)
     * 	at org.apache.hadoop.hive.metastore.HiveMetaStoreClient$SynchronizedHandler.invoke(HiveMetaStoreClient.java:2562)
     * 	at com.sun.proxy.$Proxy19.createTable(Unknown Source)
     * 	at java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
     * 	at java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
     * 	at java.base/jdk.internal.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
     * 	at java.base/java.lang.reflect.Method.invoke(Method.java:566)
     * 	at org.apache.hadoop.hive.metastore.HiveMetaStoreClient$SynchronizedHandler.invoke(HiveMetaStoreClient.java:2562)
     * 	at com.sun.proxy.$Proxy19.createTable(Unknown Source)
     * 	at org.apache.paimon.hive.HiveCatalog.lambda$createTableImpl$26(HiveCatalog.java:1020)
     * 	at org.apache.paimon.client.ClientPool$ClientPoolImpl.lambda$execute$0(ClientPool.java:80)
     * 	at org.apache.paimon.client.ClientPool$ClientPoolImpl.run(ClientPool.java:68)
     * 	at org.apache.paimon.client.ClientPool$ClientPoolImpl.execute(ClientPool.java:77)
     * 	at org.apache.paimon.hive.pool.CachedClientPool.execute(CachedClientPool.java:139)
     * 	at org.apache.paimon.hive.HiveCatalog.createTableImpl(HiveCatalog.java:1018)
     * 	at org.apache.paimon.catalog.AbstractCatalog.createTable(AbstractCatalog.java:356)
     * 	at com.qlangtech.tis.plugin.paimon.datax.hook.PreExecutor.createTable(PreExecutor.java:96)
     * 	at com.qlangtech.tis.plugin.paimon.datax.hook.PreExecutor.lambda$run$0(PreExecutor.java:64)
     * 	at com.qlangtech.tis.plugin.paimon.datax.hook.SessionStateUtil.execute(SessionStateUtil.java:76)
     * 	at com.qlangtech.tis.plugin.paimon.datax.hook.PreExecutor.run(PreExecutor.java:53)
     * 	at com.qlangtech.tis.fullbuild.taskflow.DumpTask.run(DumpTask.java:56)
     * 	at com.qlangtech.tis.fullbuild.taskflow.TISReactor$TaskImpl.run(TISReactor.java:159)
     * 	at org.jvnet.hudson.reactor.Reactor.runTask(Reactor.java:296)
     * 	at org.jvnet.hudson.reactor.Reactor$2.run(Reactor.java:214)
     * 	at org.jvnet.hudson.reactor.Reactor$Node.run(Reactor.java:117)
     * 	at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1128)
     * 	at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:628)
     * 	at com.qlangtech.tis.manage.impl.DataFlowAppSource$1.lambda$newThread$0(DataFlowAppSource.java:133)
     * 	at java.base/java.lang.Thread.run(Thread.java:834)
     * Caused by: java.lang.ClassNotFoundException: org.apache.paimon.hive.PaimonStorageHandler
     * 	at java.base/java.net.URLClassLoader.findClass(URLClassLoader.java:471)
     * 	at java.base/java.lang.ClassLoader.loadClass(ClassLoader.java:588)
     * 	at java.base/java.lang.ClassLoader.loadClass(ClassLoader.java:521)
     * 	at java.base/java.lang.Class.forName0(Native Method)
     * 	at java.base/java.lang.Class.forName(Class.java:398)
     * 	at org.apache.hadoop.hive.ql.metadata.HiveUtils.getStorageHandler(HiveUtils.java:305)
     * 	... 42 more
     * </pre>
     *
     * @param catalog
     * @param runnable
     */
    public static void execute(PaimonCatalog catalog, Runnable runnable) {
        HiveConf conf = catalog.accept(new PaimonCatalogVisitor<>() {
            @Override
            public HiveConf visit(FileSystemCatalog catalog) {
                throw new UnsupportedOperationException();
            }

            @Override
            public HiveConf visit(HiveCatalog catalog) {
                return catalog.getHiveConnGetter().createMetaStoreClient().getHiveCfg();
            }
        });
        SessionState oldHiveSessionState = SessionState.get();
        SessionState sessionState = SessionState.start(conf);
        conf.setClassLoader(SessionStateUtil.class.getClassLoader());
        try {
            runnable.run();
        } finally {
            if (oldHiveSessionState != null) {
                SessionState.setCurrentSessionState(oldHiveSessionState);
            } else {
                SessionState.detachSession();
            }

        }
    }
}

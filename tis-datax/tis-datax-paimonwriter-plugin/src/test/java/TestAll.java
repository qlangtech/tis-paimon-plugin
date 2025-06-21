import com.qlangtech.tis.plugin.paimon.catalog.TestHiveCatalog;
import com.qlangtech.tis.plugin.paimon.catalog.cache.TestCatalogCacheON;
import com.qlangtech.tis.plugin.paimon.catalog.lock.TestCatalogLockON;
import com.qlangtech.tis.plugin.paimon.datax.TestDataxPaimonWriter;
import com.qlangtech.tis.plugin.paimon.datax.changelog.ChangelogProducerONTest;
import com.qlangtech.tis.plugin.paimon.datax.compact.TestPaimonCompaction;
import com.qlangtech.tis.plugin.paimon.datax.hook.TestPreExecutor;
import com.qlangtech.tis.plugin.paimon.datax.sequence.PaimonSequenceFieldsOnTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-06-03 14:21
 **/
@RunWith(Suite.class)
@Suite.SuiteClasses({ChangelogProducerONTest.class,TestCatalogLockON.class, TestCatalogCacheON.class
        , TestHiveCatalog.class, TestDataxPaimonWriter.class, TestPaimonCompaction.class, TestPreExecutor.class, PaimonSequenceFieldsOnTest.class})
public class TestAll {


}

import com.qlangtech.tis.plugin.paimon.catalog.TestHiveCatalog;
import com.qlangtech.tis.plugin.paimon.catalog.lock.TestCatalogLockON;
import com.qlangtech.tis.plugin.paimon.datax.TestDataxPaimonWriter;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-06-03 14:21
 **/
@RunWith(Suite.class)
@Suite.SuiteClasses({TestCatalogLockON.class, TestHiveCatalog.class, TestDataxPaimonWriter.class})
public class TestAll {
}

import com.qlangtech.tis.plugin.paimon.catalog.TestHiveCatalog;
import com.qlangtech.tis.plugin.paimon.catalog.cache.TestCatalogCacheON;
import com.qlangtech.tis.plugin.paimon.catalog.lock.TestCatalogLockON;
import com.qlangtech.tis.plugin.paimon.datax.TestDataxPaimonWriter;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import java.net.URL;
import java.util.Enumeration;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-06-03 14:21
 **/
@RunWith(Suite.class)
@Suite.SuiteClasses({TestCatalogLockON.class, TestCatalogCacheON.class , TestHiveCatalog.class, TestDataxPaimonWriter.class})
public class TestAll {


}

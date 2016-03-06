package org.apache.ignite.testframework.junits;

/**
 * Created by ognen_000 on 3/5/2016.
 */
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import static org.apache.ignite.cache.CacheMode.LOCAL;
import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;

import java.util.concurrent.Callable;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.GridStringLogger;

/**
 * Created by oddo on 3/4/2016.
 */
@SuppressWarnings("unchecked")
public class CacheWithKeepBinarySelfTest extends GridCommonAbstractTest {

    private boolean binaryCache;

    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setName(gridName);
        cacheCfg.setCacheMode(LOCAL);
        cacheCfg.setAtomicityMode(ATOMIC);
        if (!binaryCache) {
            cacheCfg.setStoreKeepBinary(false);
            cfg.setMarshaller(new JdkMarshaller());
        } else {
            // by default we use BinaryMarshaller
            cacheCfg.setStoreKeepBinary(true);
        }

        cfg.setCacheConfiguration(cacheCfg);

        return cfg;
    }

    public void testWithKeepBinaryOnNonBinaryCacheMarshaller() throws Exception {

        /* test with a non-binary marshaller - should induce an exception */
        binaryCache = false;
        final Ignite ignite = startGrid("non_binary");

        GridTestUtils.assertThrows(
                new GridStringLogger(false),
                new Callable<IgniteCache<String,String>>() {
                    @Override public IgniteCache<String,String> call() throws Exception {
                        IgniteCache<String, String> cacheNonBin = ignite.cache("non_binary").withKeepBinary();
                        return null;
                    }
                },
                IgniteException.class,
                "error: withKeepBinary() cannot be invoked on a cache that has no binary marshaller"
        );

        stopGrid();

        /* now test with a binary marshaller - should return a valid cache */
        binaryCache = true;
        Ignite ignite2 = startGrid("binary");
        IgniteCache<String, String> cacheNormal = ignite2.cache("binary");
        cacheNormal.put("1","1");
        IgniteCache<String, BinaryObject> cacheBin = cacheNormal.withKeepBinary();
        BinaryObject binOb = cacheBin.get("1");

        stopGrid();
    }
}

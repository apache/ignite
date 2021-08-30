package org.apache.ignite.internal.processors.cache;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashSet;
import org.apache.commons.io.IOUtils;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.managers.communication.GridIoManager;
import org.apache.ignite.internal.processors.cache.query.CacheQuery;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryType;
import org.apache.ignite.internal.util.lang.GridCloseableIterator;
import org.apache.ignite.internal.util.lang.IgniteOutClosureX;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.CacheGetRemoveSkipStoreTest.TEST_CACHE;

/**
 *
 */
public class GridCacheIoManagerTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName)
            .setPeerClassLoadingLocalClassPathExclude(TestFilter.class.getName())
            .setClassLoader(new TestClassLoaderDiffJavaVersion(TestFilter.class.getName()));

        return cfg;
    }

    /** */
    @Test
    public void unmarshallTest() throws Exception {
        Thread.currentThread().setContextClassLoader(new TestClassLoaderDiffJavaVersion(TestFilter.class.getName()));


        IgniteEx server = startGrid();
        IgniteEx clientGrid = startClientGrid(1);

        GridKernalContext ctx = server.context();
        GridIoManager io = ctx.io();
        GridCacheProcessor cachePrc = ctx.cache();

        io.addMessageListener(GridTopic.TOPIC_CACHE, (nodeId, msg, plc) -> {
                System.err.println(msg);
            Thread.currentThread().setContextClassLoader(
                new TestClassLoaderDiffJavaVersion(TestFilter.class.getName()));
            }

        );

        IgniteCache<Object, Object> cache = clientGrid.createCache(TEST_CACHE);

        GatewayProtectedCacheProxy<?,?> cacheProxy = (GatewayProtectedCacheProxy<?, ?>)cache;
        GridCacheContext<?, ?> cacheCtx = cacheProxy.context();

        for (int i = 0; i < 10; i++)
            cache.put(i, i);

//        final List<Cache.Entry<String, String>> all = cache.query(new ScanQuery<String, String>(new TestFilter<>()))
//            .getAll();

        CacheQuery<Object> qry1 = cacheCtx.queries().createScanQuery(
            new TestFilter<>(), null, null, false, false, null);

        GridCloseableIterator iter = server.context().query().executeQuery(GridCacheQueryType.SCAN,
            TEST_CACHE, cacheCtx, new IgniteOutClosureX<GridCloseableIterator>() {
                @Override public GridCloseableIterator applyx() throws IgniteCheckedException {
                    return qry1.executeScanQuery();
                }
            }, true);


        Assert.assertNotNull(server.cache(TEST_CACHE));
        Assert.assertEquals(ClusterState.ACTIVE, server.cluster().state());
    }

    /** */
    private static class TestClassLoaderDiffJavaVersion extends ClassLoader {
        /** */
        private final HashSet<String> clsNames;

        /** */
        private TestClassLoaderDiffJavaVersion(String... clsNames) {
            this.clsNames = new HashSet<>(Arrays.asList(clsNames));
        }

        /** {@inheritDoc} */
        @Override protected synchronized Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            System.err.println(name);
            if (clsNames.contains(name)) {
                String path = U.classNameToResourceName(name);

                InputStream in = getResourceAsStream(path);

                if (in != null) {
                    byte[] bytes;

                    try {
                        bytes = IOUtils.toByteArray(in);

                        bytes[7] = ++bytes[7];
                    }
                    catch (IOException e) {
                        throw new ClassNotFoundException("Failed to upload class ", e);
                    }

                    return defineClass(name, bytes, 0, bytes.length);
                }

                throw new ClassNotFoundException("Failed to upload resource [class=" + path + ", parent classloader="
                    + getParent() + ']');
            }

            // Maybe super knows.
            return super.loadClass(name, resolve);
        }
    }

    /** */
    private static class TestFilter<E1, E2> implements IgniteBiPredicate<E1, E2> {
        /** {@inheritDoc} */
        @Override public boolean apply(Object o, Object o2) {
            System.err.println("TestFilter: " + o + ", "+ o2);
            return true;
        }
    }
}

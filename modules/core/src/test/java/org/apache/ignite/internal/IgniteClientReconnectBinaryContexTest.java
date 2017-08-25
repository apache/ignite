package org.apache.ignite.internal;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.Callable;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;

/**
 *
 */
public class IgniteClientReconnectBinaryContexTest extends IgniteClientReconnectAbstractTest{
    /** {@inheritDoc} */
    @Override protected int serverCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override protected int clientCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setPeerClassLoadingEnabled(false);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testBinaryContexReconnectClusterRestart() throws Exception {
        Ignite client = grid(serverCount());

        assertTrue(client.cluster().localNode().isClient());

        Ignite srv = grid(0);

        CacheConfiguration<TestClass1, TestClass1> personCache = new CacheConfiguration<TestClass1, TestClass1>();
        personCache.setName(DEFAULT_CACHE_NAME);
        personCache.setCacheMode(CacheMode.REPLICATED);
        personCache.setQueryEntities(new ArrayList<QueryEntity>());

        srv.createCache(personCache);

        client.cache(DEFAULT_CACHE_NAME).put(new TestClass1("1"), new TestClass1("1"));
        client.cache(DEFAULT_CACHE_NAME).put(new TestClass1("2"), new TestClass1("2"));

        Collection<Ignite> ignites = reconnectServersRestart(log, client, Collections.singleton(srv), new Callable<Collection<Ignite>>() {
            @Override public Collection<Ignite> call() throws Exception {
                return Collections.singleton((Ignite)startGrid(0));
            }
        });
        ignites.iterator().next().createCache(personCache);

        client.cache(DEFAULT_CACHE_NAME).put(new TestClass1("2"), new TestClass1("2"));
        client.cache(DEFAULT_CACHE_NAME).put(new TestClass1("3"), new TestClass1("3"));

        int size = 0;
        for (Cache.Entry<TestClass1, TestClass1> entry: client.<TestClass1, TestClass1>cache(DEFAULT_CACHE_NAME)){
            size++;
        }
        assertTrue(size == 2);
    }

    /**
     *
     */
    static class TestClass1{
        final String field;

        TestClass1(String field) {
            this.field = field;
        }
    }
}

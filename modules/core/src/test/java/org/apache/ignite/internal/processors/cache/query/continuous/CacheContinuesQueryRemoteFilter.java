package org.apache.ignite.internal.processors.cache.query.continuous;

import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.configuration.Factory;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryEventFilter;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.log4j.BasicConfigurator;
import org.junit.Test;

public class CacheContinuesQueryRemoteFilter extends GridCommonAbstractTest {
    /**
     *
     */
    protected static final String CLIENT = "_client";

    /**
     *
     */
    protected static final String SERVER = "_server";

    private static final int DATA_AMOUNT = 10;
    private static final long TIMEOUT = 10_000L;

    public static AtomicInteger counter = new AtomicInteger(0);

    @Test
    public void test() throws Exception {
        BasicConfigurator.configure();

        Ignite grid1 = startGrid(1 + SERVER);
        Ignite grid2 = startGrid(2 + SERVER);

        AtomicInteger cacheSize = new AtomicInteger(0);

        IgniteCache<Object, Object> cacheCounter = grid1.getOrCreateCache(new CacheConfiguration<Object, Object>()
            .setName("counter")
            .setCacheMode(CacheMode.REPLICATED));

        try (Ignite client = startGrid("1" + CLIENT)) {
            IgniteCache<String, String> cache = client.getOrCreateCache(new CacheConfiguration<String, String>()
                .setName("myCache")
                .setCacheMode(CacheMode.REPLICATED));

            ContinuousQuery<String, String> qry = new ContinuousQuery<>();
            qry.setAutoUnsubscribe(false);

//            qry.setRemoteFilterFactory(() -> event -> {
//                counter.incrementAndGet();
//                System.out.println("RemoteFilter=" + event.getKey() + " " + event.getEventType() + " counter " + counter.get());
//                return true;
//            });
            qry.setRemoteFilterFactory(new RemoteFactory<String, String>());

            qry.setLocalListener(events -> {
                events.forEach(event -> {
                    System.out.println("LocalListener=" + event.getEventType() + " " + event.getKey());
                });
            });

            startGrid(3 + SERVER);

            cache.query(qry);
            for (int i = 0; i < DATA_AMOUNT; i++)
                cache.put("k" + i, "v" + i);
            cache.forEach(a -> cacheSize.incrementAndGet());
        }

        assertTrue("Counter:" + counter.get(), (DATA_AMOUNT) == counter.get());

        counter.set(0);

        Ignite grid4 = startGrid(4 + SERVER);

        try (Ignite client = startGrid("1" + CLIENT)) {
            IgniteCache cache = client.getOrCreateCache(new CacheConfiguration<String, String>()
                .setName("myCache")
                .setCacheMode(CacheMode.REPLICATED));

            for (int i = 0; i < DATA_AMOUNT; i++)
                cache.put("k" + i, "v" + i);
        }

        Thread.sleep(TIMEOUT);

        assertTrue("CacheSize:" + cacheSize.get(), DATA_AMOUNT == cacheSize.get());

        assertTrue("Counter:" + counter.get(), DATA_AMOUNT == counter.get());


    }

    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (igniteInstanceName.endsWith(CLIENT))
            cfg.setClientMode(true);
        return cfg;
    }
}

class RemoteFactory<T,E> implements Factory<CacheEntryEventFilter<T, E>> {

    @IgniteInstanceResource Ignite ignite;

    private ClusterNode node;

    public RemoteFactory(ClusterNode node) {
        this.node = node;
    }

    public RemoteFactory() {
    }

    @Override
    public CacheEntryEventFilter<T, E> create() {
        return new CacheEntryEventFilter<T, E>() {
            @IgniteInstanceResource
            private Ignite ignite;

            private ClusterNode node;

            @Override
            public boolean evaluate(CacheEntryEvent<? extends T, ? extends E> evt) {
                if (nodesPrimaryForThisKey(evt)){
                    CacheContinuesQueryRemoteFilter.counter.incrementAndGet();
                    return true;
                }else

                return false;
            }

            boolean nodesPrimaryForThisKey(CacheEntryEvent evt) {
                Affinity aff = ignite.affinity(evt.getSource().getName());
                node = ignite.cluster().localNode();
                ClusterNode primary = aff.mapKeyToNode(evt.getKey());
                return primary.id().equals(node.id());
            }


        };
    }
}
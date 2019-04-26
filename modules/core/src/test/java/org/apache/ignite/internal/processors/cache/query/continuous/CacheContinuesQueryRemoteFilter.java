package org.apache.ignite.internal.processors.cache.query.continuous;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.log4j.BasicConfigurator;
import org.junit.Test;

public class CacheContinuesQueryRemoteFilter extends GridCommonAbstractTest {
    /** */
    protected static final String CLIENT = "_client";

    /** */
    protected static final String SERVER = "server";

    protected static final String SERVER2 = "server2";


    private static final int DATA_AMOUNT = 10;
    private static final int TIMEOUT = 30_000;

    @Test
    public void test() throws Exception {
        BasicConfigurator.configure();

        Ignite grid1 = startGrid(1+SERVER);
        Ignite grid2 =startGrid(2+SERVER);

        CacheConfiguration<String, String> cfg = new CacheConfiguration<>();
        cfg.setCacheMode(CacheMode.REPLICATED);
        cfg.setName("myCache");

        IgniteCache<String, String> cache = grid1.getOrCreateCache(cfg);

        AtomicInteger counter = new AtomicInteger(0);
        AtomicInteger cacheSize = new AtomicInteger(0);

        Auditor auditor = new Auditor(counter, cache);

        try(Ignite client=startGrid("1" + CLIENT)) {
            client.compute().run(auditor);

            for (long i = 0; i < 10; i++) {
                cache.put("k" + i, "v" + i);
            }
        }

        Thread.sleep(TIMEOUT);

        cache.forEach(a->cacheSize.incrementAndGet());

        assertTrue("CacheSize:"+cacheSize.get(), DATA_AMOUNT==cacheSize.get());

        assertTrue("Counter:"+counter.get(),(DATA_AMOUNT*2)==counter.get());


    }

    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (igniteInstanceName.endsWith(CLIENT)) {
            cfg.setClientMode(true);

            cfg.setCommunicationSpi(new CacheContinuousBatchAckTest.FailedTcpCommunicationSpi(true, false));
        }
        else if (igniteInstanceName.endsWith(SERVER2))
            cfg.setCommunicationSpi(new CacheContinuousBatchAckTest.FailedTcpCommunicationSpi(false, true));
        else
            cfg.setCommunicationSpi(new CacheContinuousBatchAckTest.FailedTcpCommunicationSpi(false, false));

        return cfg;
    }
}

class Auditor<T, E> implements IgniteRunnable {

//    @IgniteInstanceResource Ignite ignite;

    AtomicInteger cntr;

    IgniteCache cache;

    public Auditor(AtomicInteger cntr, IgniteCache<T, E> cache) {
        this.cntr = cntr;
        this.cache = cache;

    }

    @Override public void run() {
        ContinuousQuery<String, String> qry = new ContinuousQuery<>();
        qry.setAutoUnsubscribe(false);

        qry.setRemoteFilterFactory(() -> event -> {
            cntr.incrementAndGet();
            System.out.println("RemoteFilter=" + event.getKey() + " " + event.getEventType());
            return true;
        });

//        qry.setRemoteFilter(evt -> {
//            cntr.incrementAndGet();
//            System.out.println("RemoteFilter: type{" + evt.getEventType() + "}, key{" + evt.getKey() + "}, value{" + evt.getValue() + "}");
//            return true;
//        });

        qry.setLocalListener(events -> {
            events.forEach(event -> {
                System.out.println("LocalListener=" + event.getEventType() + " " + event.getKey());
            });
        });
    }
}

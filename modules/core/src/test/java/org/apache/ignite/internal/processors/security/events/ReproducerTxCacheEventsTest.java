package org.apache.ignite.internal.processors.security.events;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.CacheEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.security.AbstractSecurityTest;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_READ;

@RunWith(Parameterized.class)
public class ReproducerTxCacheEventsTest extends AbstractSecurityTest {
    private static final AtomicInteger COUNTER = new AtomicInteger();
    private static final AtomicInteger rmtCnt = new AtomicInteger();

    @Parameterized.Parameter()
    public TransactionConcurrency txCncr;
    @Parameterized.Parameter(1)
    public TransactionIsolation txIsl;

    @Parameterized.Parameters(name = "txCncr={0}, txIsl={1}")
    public static Iterable<Object[]> data() {
        List<Object[]> res = new ArrayList<>();

        for (TransactionConcurrency tc : TransactionConcurrency.values()) {
            for (TransactionIsolation ti : TransactionIsolation.values())
                res.add(new Object[] {tc, ti});
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridAllowAll("listener_node");
        startGridAllowAll("srv");
        startGridAllowAll("additional_srv");
        startClientAllowAll("client");
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setConnectorConfiguration(new ConnectorConfiguration())
            .setIncludeEventTypes(EVT_CACHE_OBJECT_READ);
    }

    @Test
    public void testCacheReadEvent() throws Exception {
        rmtCnt.set(0);

        final String cacheName = "test_cache_" + COUNTER.incrementAndGet();

        IgniteCache<String, String> cache = grid("listener_node").createCache(
            new CacheConfiguration<String, String>(cacheName)
                .setBackups(2)
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL));

        cache.put("KEY", "INIT_VAL");

        UUID lsnrId = grid("listener_node").events().remoteListen(null,
            new IgnitePredicate<Event>() {
                @IgniteInstanceResource IgniteEx ign;

                @Override public boolean apply(Event evt) {
                    CacheEvent ce = (CacheEvent)evt;

                    if (cacheName.equals(ce.cacheName())) {
                        System.out.println(
                            "Remote listener locNode=" + ign.name()
                            + ", oldVal=" + ((CacheEvent)evt).oldValue()
                            + ", newVal=" + ((CacheEvent)evt).newValue()
                        );

                        rmtCnt.incrementAndGet();
                    }

                    return true;
                }
            }, EVT_CACHE_OBJECT_READ);

        try {

            Ignite clntNode = grid("client");
            IgniteCache<String, String> clntCache = clntNode.cache(cacheName);
            IgniteTransactions transactions = clntNode.transactions();

            try (Transaction tx = transactions.txStart(txCncr, txIsl)) {
                clntCache.get("KEY");

                tx.commit();
            }
            // Waiting for events.
            TimeUnit.SECONDS.sleep(3);

            assertEquals("Remote filter.", 1, rmtCnt.get());
        }
        finally {
            grid("listener_node").events().stopRemoteListen(lsnrId);
        }
    }
}

package org.apache.ignite.stream.flume;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.channel.PseudoTxnMemoryChannel;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.EventBuilder;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.events.CacheEvent;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_PUT;

/**
 * Created by shtykh_roman on 10/20/15.
 */
public class IgniteFlumeSinkTest extends GridCommonAbstractTest {
    private static final int CNT = 100;
    private IgniteSink sink;
    private Context ctx;

    /** Constructor. */
    public IgniteFlumeSinkTest() {
        super(true);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected void beforeTest() throws Exception {
        grid().<String, Integer>getOrCreateCache(defaultCacheConfiguration());
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        // NOOP
    }

    public void testFlumeSink() throws Exception {
        initContext();
        Channel channel = new PseudoTxnMemoryChannel();
        Configurables.configure(channel, new Context());
        sink = new IgniteSink();
        Configurables.configure(sink, ctx);
        sink.setChannel(channel);
        sink.specifyGrid(grid());
        sink.start();

        final CountDownLatch latch = new CountDownLatch(CNT);
        IgniteBiPredicate<UUID, CacheEvent> callback = new IgniteBiPredicate<UUID, CacheEvent>() {
            @Override public boolean apply(UUID uuid, CacheEvent evt) {
                latch.countDown();
                return true;
            }
        };
        grid().events(grid().cluster().forCacheNodes(sink.getCacheName())).remoteListen(callback, null, EVT_CACHE_OBJECT_PUT);

        for (int i = 0; i < CNT; i++) {
            Event event = EventBuilder.withBody((String.valueOf(i) + ": " + i).getBytes());
            channel.put(event);
            sink.process();
        }
        latch.await();

        // check with Ignite
        IgniteCache<String, Integer> cache = grid().cache(sink.getCacheName());
        for (int i = 0; i < CNT; i++) {
            assertEquals(i, (int)cache.get(String.valueOf(i)));
        }
        sink.stop();
    }

    private void initContext() {
        ctx = new Context();
        ctx.put(IgniteSinkConstants.CFG_CACHE_NAME, "testCache");
        ctx.put(IgniteSinkConstants.CFG_STREAMER_TYPE, "org.apache.ignite.stream.flume.TestFlumeStreamer");
    }
}

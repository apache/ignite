package org.apache.ignite.stream.akka;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.events.CacheEvent;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_PUT;

public class AkkaIgniteStreamerTest extends GridCommonAbstractTest {

    ActorSystem actorSystem = null;
    ActorRef actorRef = null;

    /** Count. */
    private static final int CNT = 10;

    /** Topic message key prefix. */
    private static final String KEY_PREFIX = "192.168.2.";

    /** Topic message value URL. */
    private static final String VALUE_URL = ",www.example.com,";

    public AkkaIgniteStreamerTest() {
        super(true);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected void beforeTest() throws Exception {
        grid().<Integer, String>getOrCreateCache(defaultCacheConfiguration());

        /** Creates a new ActorSystem with the specified name. */
        actorSystem = ActorSystem.create("AkkaIgnite");

        /**
         * Create new actor as child of this context with the given name, which must
         * not be null, empty or start with “$”.
         */
        actorRef = actorSystem.actorOf(Props.create(AkkaActor.class), "AkkaActor");
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        grid().cache(null).clear();

        /** Stop child actor processing. */
        actorSystem.stop(actorRef);

        //** Shutdown actor system. */
        actorSystem.shutdown();
    }

    /**
     * Test Akka streamer
     *
     * @throws TimeoutException
     * @throws InterruptedException
     */
    public void testAkkaStreamer() throws TimeoutException, InterruptedException {
        HashMap<String, String> keyValMap = produceActorStream();
        consumeActorStream(keyValMap);
    }

    /**
     * Send entry values to akka streamer
     * @return
     */
    private HashMap<String, String> produceActorStream() {
        /** Generate random subnets. */
        List<Integer> subnet = new ArrayList<>();

        for (int i = 1; i <= CNT; i++)
            subnet.add(i);

        Collections.shuffle(subnet);

        HashMap<String, String> keyValMap = new HashMap<>();

        for (int evt = 0; evt < CNT; evt++) {
            long runtime = System.currentTimeMillis();

            String ip = KEY_PREFIX + subnet.get(evt);

            String msg = runtime + VALUE_URL + ip;
            keyValMap.put(ip, msg);
        }

        /** Emit hash map to akka streaming actor. */
        actorRef.tell(keyValMap, ActorRef.noSender());

        return keyValMap;
    }

    private void consumeActorStream(HashMap<String, String> keyValMap) throws TimeoutException, InterruptedException {
        AkkaStreamer<String, String, String> AkkaStmr = null;

        Ignite ignite = grid();
        try (IgniteDataStreamer<String, String> stmr = ignite.dataStreamer(null)) {
            stmr.allowOverwrite(true);
            stmr.autoFlushFrequency(10);

            /** Configure Akka streamer. */
            AkkaStmr = new AkkaStreamer<>(keyValMap);

            /** Get the cache. */
            IgniteCache<String, String> cache = ignite.cache(null);

            /** Set Ignite instance. */
            AkkaStmr.setIgnite(ignite);

            /** Set data streamer instance. */
            AkkaStmr.setStreamer(stmr);

            /** Set the number of threads. */
            AkkaStmr.setThreads(1);

            /** Start Akka streamer. */
            AkkaStmr.start();

            final CountDownLatch latch = new CountDownLatch(CNT);

            IgniteBiPredicate<UUID, CacheEvent> locLsnr = new IgniteBiPredicate<UUID, CacheEvent>() {
                @Override public boolean apply(UUID uuid, CacheEvent evt) {
                    latch.countDown();

                    return true;
                }
            };

            ignite.events(ignite.cluster().forCacheNodes(null)).remoteListen(locLsnr, null, EVT_CACHE_OBJECT_PUT);

            /** Checks all events successfully processed in 10 seconds. */
            assertTrue(latch.await(10, TimeUnit.SECONDS));

            for (Map.Entry<String, String> entry : keyValMap.entrySet()) {
                assertEquals(entry.getValue(), cache.get(entry.getKey()));
            }
        }
        finally {
            if (AkkaStmr != null)
                AkkaStmr.stop();
        }
    }

}

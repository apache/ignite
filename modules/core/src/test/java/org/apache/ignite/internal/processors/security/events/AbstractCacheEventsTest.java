package org.apache.ignite.internal.processors.security.events;

import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import javax.cache.Cache;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.CacheEvent;
import org.apache.ignite.events.CacheQueryExecutedEvent;
import org.apache.ignite.events.CacheQueryReadEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.client.GridClientData;
import org.apache.ignite.internal.processors.rest.GridRestCommand;
import org.apache.ignite.internal.processors.security.AbstractSecurityTest;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static java.util.Collections.singleton;
import static org.apache.ignite.events.EventType.EVT_CACHE_ENTRY_CREATED;
import static org.apache.ignite.events.EventType.EVT_CACHE_ENTRY_DESTROYED;
import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_LOCKED;
import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_PUT;
import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_READ;
import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_REMOVED;
import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_UNLOCKED;
import static org.apache.ignite.events.EventType.EVT_CACHE_QUERY_EXECUTED;
import static org.apache.ignite.events.EventType.EVT_CACHE_QUERY_OBJECT_READ;

/**
 * Security subject id of cache events have to refer to subject that initiates cache operation.
 */
public abstract class AbstractCacheEventsTest extends AbstractSecurityTest {
    /** Counter to name caches. */
    private static final AtomicInteger COUNTER = new AtomicInteger();

    /** Node that registers event listeners. */
    protected static final String LISTENER_NODE = "listener_node";

    /** Client node. */
    protected static final String CLNT = "client";

    /** Server node. */
    protected static final String SRV = "server";

    /** Key. */
    protected static final String KEY = "key";

    /** Initiate value. */
    public static final String INIT_VAL = "init_val";

    /** Value. */
    protected static final String VAL = "val";

    /** Events latch. */
    private static CountDownLatch evtsLatch;

    /** Local events count. */
    private static final AtomicInteger evtCnt = new AtomicInteger();

    /** Error message - actual subject is not expected. */
    private static final AtomicReference<String> error = new AtomicReference<>();

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridAllowAll(LISTENER_NODE);
        startGridAllowAll(SRV);
        startGridAllowAll("additional_srv");

        startClientAllowAll(CLNT);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setConnectorConfiguration(new ConnectorConfiguration())
            .setIncludeEventTypes(EVT_CACHE_ENTRY_CREATED,
                EVT_CACHE_ENTRY_DESTROYED,
                EVT_CACHE_OBJECT_PUT,
                EVT_CACHE_OBJECT_READ,
                EVT_CACHE_OBJECT_REMOVED,
                EVT_CACHE_OBJECT_LOCKED,
                EVT_CACHE_OBJECT_UNLOCKED,
                EVT_CACHE_QUERY_EXECUTED,
                EVT_CACHE_QUERY_OBJECT_READ);
    }

    /**
     *
     */
    protected abstract CacheAtomicityMode cacheAtomicityMode();

    /**
     *
     */
    protected abstract GridTestUtils.ConsumerX<String> operation();

    /**
     *
     */
    protected abstract String expectedLogin();

    /**
     *
     */
    protected abstract int eventType();

    /**
     *
     */
    protected abstract TestOperation testOperation();

    /**
     *
     */
    @Test
    public void testCacheEvent() throws Exception {
        final String cacheName = "test_cache_" + COUNTER.incrementAndGet();

        IgniteCache<String, String> cache = grid(LISTENER_NODE).createCache(
            new CacheConfiguration<String, String>(cacheName)
                .setBackups(2)
                .setAtomicityMode(cacheAtomicityMode()));

        if (testOperation().valIsRequired)
            cache.put(KEY, INIT_VAL);

        int expTimes = eventType() == EVT_CACHE_OBJECT_READ || eventType() == EVT_CACHE_QUERY_OBJECT_READ ? 1 : 3;

        evtsLatch = new CountDownLatch(expTimes);
        evtCnt.set(0);
        error.set(null);

        UUID lsnrId = grid(LISTENER_NODE).events().remoteListen(
            new IgniteBiPredicate<UUID, Event>() {
                @IgniteInstanceResource IgniteEx ign;

                @Override public boolean apply(UUID uuid, Event evt) {
                    processEvent(ign, evt, cacheName, true);

                    return true;
                }
            },
            new IgnitePredicate<Event>() {
                @IgniteInstanceResource IgniteEx ign;

                @Override public boolean apply(Event evt) {
                    processEvent(ign, evt, cacheName, false);

                    return true;
                }
            }, eventType());

        try {
            operation().accept(cacheName);
            // Waiting for events.
            evtsLatch.await(10, TimeUnit.SECONDS);

            if (cacheAtomicityMode() == CacheAtomicityMode.TRANSACTIONAL &&
                (eventType() == EVT_CACHE_OBJECT_READ || eventType() == EVT_CACHE_QUERY_OBJECT_READ))
                assertTrue(String.format("Expected <%s>, actual <%s>", expTimes, evtCnt.get()), evtCnt.get() >= expTimes);
            else
                assertEquals(expTimes, evtCnt.get());

            if (error.get() != null)
                fail(error.get());

        }
        finally {
            grid(LISTENER_NODE).events().stopRemoteListen(lsnrId);
            grid(LISTENER_NODE).destroyCache(cacheName);
        }
    }

    /** */
    private void processEvent(IgniteEx ign, Event input, String cacheName, boolean isLoc) {
        TestEventAdapter evt = new TestEventAdapter(input);

        if (cacheName.equals(evt.cacheName()) &&
            (evt.type() != EVT_CACHE_OBJECT_PUT || !F.eq(INIT_VAL, evt.newValue()))) {
            if (isLoc) {
                evtCnt.incrementAndGet();

                evtsLatch.countDown();
            }

            try {
                String actualLogin = ign.context().security().authenticatedSubject(evt.subjectId()).login().toString();

                if (!F.eq(expectedLogin(), actualLogin)) {
                    error.compareAndSet(null, String.format("[local_node=%s] expected <%s> but was <%s>",
                        expectedLogin(), actualLogin, ign.context().igniteInstanceName()));
                }
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        }
    }

    /** Test opeartions. */
    public enum TestOperation {
        /** Put. */
        PUT(c -> c.put(KEY, VAL), c -> c.put(KEY, VAL), d -> d.put(KEY, VAL), GridRestCommand.CACHE_PUT, false),

        /** Put async. */
        PUT_ASYNC(c -> c.putAsync(KEY, VAL).get(), d -> d.putAsync(KEY, VAL).get(), false),

        /** Put all. */
        PUT_ALL(c -> c.putAll(F.asMap(KEY, VAL)), c -> c.putAll(F.asMap(KEY, VAL)), d -> d.putAll(F.asMap(KEY, VAL)),
            GridRestCommand.CACHE_PUT_ALL, false),

        /** Put all async. */
        PUT_ALL_ASYNC(c -> c.putAll(F.asMap(KEY, VAL)), d -> d.putAllAsync(F.asMap(KEY, VAL)), false),

        /** Put if absent. */
        PUT_IF_ABSENT(c -> c.putIfAbsent(KEY, VAL), c -> c.putIfAbsent(KEY, VAL),
            null, GridRestCommand.CACHE_PUT_IF_ABSENT, false),

        /** Put if absent async. */
        PUT_IF_ABSENT_ASYNC(c -> c.putIfAbsentAsync(KEY, VAL), null, null,
            GridRestCommand.CACHE_ADD, false),

        /** Get. */
        GET(c -> c.get(KEY), c -> c.get(KEY), d -> d.get(KEY), GridRestCommand.CACHE_GET),

        /** Get async. */
        GET_ASYNC(c -> c.getAsync(KEY).get(), d -> d.getAsync(KEY).get(), true),

        /** Get entry. */
        GET_ENTRY(c -> c.getEntry(KEY)),

        /** Get entry async. */
        GET_ENTRY_ASYNC(c -> c.getEntryAsync(KEY).get()),

        /** Get entries. */
        GET_ENTRIES(c -> c.getEntries(singleton(KEY))),

        /** Get entries async. */
        GET_ENTRIES_ASYNC(c -> c.getEntriesAsync(singleton(KEY))),

        /** Get all. */
        GET_ALL(c -> c.getAll(singleton(KEY)), c -> c.getAll(singleton(KEY)),
            d -> d.getAll(singleton(KEY)), GridRestCommand.CACHE_GET_ALL),

        /** Get all async. */
        GET_ALL_ASYNC(c -> c.getAllAsync(singleton(KEY)).get(), d -> d.getAllAsync(singleton(KEY)).get(), true),

        /** Get all out tx. */
        GET_ALL_OUT_TX(c -> c.getAllOutTx(singleton(KEY))),

        /** Get all out tx async. */
        GET_ALL_OUT_TX_ASYNC(c -> c.getAllOutTxAsync(singleton(KEY)).get()),

        /** Get and put. */
        GET_AND_PUT(c -> c.getAndPut(KEY, VAL), c -> c.getAndPut(KEY, VAL), null, GridRestCommand.CACHE_GET_AND_PUT),

        /** Get and put async. */
        GET_AND_PUT_ASYNC(c -> c.getAndPutAsync(KEY, VAL).get()),

        /** Get and put if absent. */
        GET_AND_PUT_IF_ABSENT(c -> c.getAndPutIfAbsent(KEY, VAL), null, null, GridRestCommand.CACHE_GET_AND_PUT_IF_ABSENT),

        /** Get and put if absent async. */
        GET_AND_PUT_IF_ABSENT_ASYNC(c -> c.getAndPutIfAbsentAsync(KEY, VAL).get()),

        /** Get and put if absent put case. */
        GET_AND_PUT_IF_ABSENT_PUT_CASE(c -> c.getAndPutIfAbsent(KEY, VAL), null, null, GridRestCommand.CACHE_GET_AND_PUT_IF_ABSENT, false),

        /** Get and put if absent put case async. */
        GET_AND_PUT_IF_ABSENT_PUT_CASE_ASYNC(c -> c.getAndPutIfAbsentAsync(KEY, VAL).get(), false),

        /** Remove. */
        REMOVE(c -> c.remove(KEY), c -> c.remove(KEY), d -> d.remove(KEY), GridRestCommand.CACHE_REMOVE),

        /** Remove async. */
        REMOVE_ASYNC(c -> c.removeAsync(KEY).get(), d -> d.removeAsync(KEY).get(), true),

        /** Remove value. */
        REMOVE_VAL(c -> c.remove(KEY, INIT_VAL), c -> c.remove(KEY, INIT_VAL), null, GridRestCommand.CACHE_REMOVE_VALUE),

        /** Remove value async. */
        REMOVE_VAL_ASYNC(c -> c.removeAsync(KEY, INIT_VAL).get()),

        /** Get and remove. */
        GET_AND_REMOVE(c -> c.getAndRemove(KEY), c -> c.getAndRemove(KEY), null, GridRestCommand.CACHE_GET_AND_REMOVE),

        /** Get and remove async. */
        GET_AND_REMOVE_ASYNC(c -> c.getAndRemoveAsync(KEY).get()),

        /** Remove all. */
        REMOVE_ALL(c -> c.removeAll(singleton(KEY)), c -> c.removeAll(singleton(KEY)),
            d -> d.removeAll(singleton(KEY)), GridRestCommand.CACHE_REMOVE_ALL),

        /** Remove all async. */
        REMOVE_ALL_ASYNC(c -> c.removeAllAsync(singleton(KEY)).get(), d -> d.removeAllAsync(singleton(KEY)).get(), true),

        /** Replace. */
        REPLACE(c -> c.replace(KEY, VAL), c -> c.replace(KEY, VAL),
            d -> d.replace(KEY, VAL), GridRestCommand.CACHE_REPLACE, true),

        /** Replace async. */
        REPLACE_ASYNC(c -> c.replaceAsync(KEY, VAL).get(), d -> d.replaceAsync(KEY, VAL).get(), true),

        /** Replace value. */
        REPLACE_VAL(c -> c.replace(KEY, INIT_VAL, VAL), c -> c.replace(KEY, INIT_VAL, VAL),
            d -> d.cas(KEY, VAL, INIT_VAL), GridRestCommand.CACHE_REPLACE_VALUE),

        /** Replace value async. */
        REPLACE_VAL_ASYNC(c -> c.replaceAsync(KEY, INIT_VAL, VAL).get(), d -> d.casAsync(KEY, VAL, INIT_VAL).get(), true),

        /** Get and replace. */
        GET_AND_REPLACE(c -> c.getAndReplace(KEY, VAL), c -> c.getAndReplace(KEY, VAL), null, GridRestCommand.CACHE_GET_AND_REPLACE),

        /** Get and replace async. */
        GET_AND_REPLACE_ASYNC(c -> c.getAndReplaceAsync(KEY, VAL).get()),

        /** Append. */
        APPEND(null, d -> d.append(KEY, VAL), true),

        /** Append async. */
        APPEND_ASYNC(null, d -> d.appendAsync(KEY, VAL).get(), true),

        /** Prepend. */
        PREPEND(null, d -> d.prepend(KEY, VAL), true),

        /** Prepend async. */
        PREPEND_ASYNC(null, d -> d.prependAsync(KEY, VAL), true),

        /** Invoke. */
        INVOKE(c -> c.invoke(KEY, (entry, o) -> {
            entry.setValue(VAL);

            return null;
        }), true),

        /** Invoke async. */
        INVOKE_ASYNC(c -> c.invokeAsync(KEY, (entry, o) -> {
            entry.setValue(VAL);

            return null;
        }).get(), true),

        /** Invoke all with set. */
        INVOKE_ALL_SET(c -> c.invokeAll(Collections.singleton(KEY), (entry, o) -> {
            entry.setValue(VAL);

            return null;
        }), true),

        /** Invoke all async with set. */
        INVOKE_ALL_SET_ASYNC(c -> c.invokeAllAsync(Collections.singleton(KEY), (entry, o) -> {
            entry.setValue(VAL);

            return null;
        }).get(), true),

        /** Invoke all with map. */
        INVOKE_ALL_MAP(c -> c.invokeAll(
            Collections.singletonMap(KEY, new EntryProcessor<String, String, Object>() {
                @Override public Object process(MutableEntry<String, String> entry,
                    Object... arguments) throws EntryProcessorException {
                    entry.setValue(VAL);

                    return null;
                }
            })), true),

        /** Invoke all with map. */
        INVOKE_ALL_MAP_ASYNC(c -> c.invokeAllAsync(
            Collections.singletonMap(KEY, new EntryProcessor<String, String, Object>() {
                @Override public Object process(MutableEntry<String, String> entry,
                    Object... arguments) throws EntryProcessorException {
                    entry.setValue(VAL);

                    return null;
                }
            })).get(), true),

        /** Lock. */
        LOCK(c -> {
            Lock lock = c.lock(KEY);

            lock.lock();
            lock.unlock();
        }, false),

        /** Lock all. */
        LOCK_ALL(c -> {
            Lock lock = c.lockAll(singleton(KEY));

            lock.lock();
            lock.unlock();
        }, false),

        /** Scan query. */
        SCAN_QUERY(
            c -> {
                try (QueryCursor<Cache.Entry<String, String>> cursor = c.query(new ScanQuery<>())) {
                    cursor.getAll();
                }
            },
            c -> {
                try (QueryCursor<Cache.Entry<String, String>> cursor = c.query(new ScanQuery<>())) {
                    cursor.getAll();
                }
            },
            null,
            GridRestCommand.EXECUTE_SCAN_QUERY);

        /** IgniteCache operation. */
        public final GridTestUtils.ConsumerX<IgniteCache<String, String>> ignCacheOp;

        /** ClientCache operation. */
        final GridTestUtils.ConsumerX<ClientCache<String, String>> clntCacheOp;

        /** GridClient operation. */
        final GridTestUtils.ConsumerX<GridClientData> gridClntDataOp;

        /** Rest client command. */
        final GridRestCommand restCmd;

        /**
         * True if a test operation requires existence of value in a cache.
         */
        final boolean valIsRequired;

        /**
         * @param ignCacheOp IgniteCache operation.
         */
        TestOperation(GridTestUtils.ConsumerX<IgniteCache<String, String>> ignCacheOp) {
            this(ignCacheOp, null, null, null, true);
        }

        /**
         * @param ignCacheOp IgniteCache operation.
         */
        TestOperation(GridTestUtils.ConsumerX<IgniteCache<String, String>> ignCacheOp, boolean valIsRequired) {
            this(ignCacheOp, null, null, null, valIsRequired);
        }

        /**
         * @param ignCacheOp IgniteCache operation.
         */
        TestOperation(GridTestUtils.ConsumerX<IgniteCache<String, String>> ignCacheOp,
            GridTestUtils.ConsumerX<GridClientData> gridClntDataOp,
            boolean valIsRequired) {
            this(ignCacheOp, null, gridClntDataOp, null, valIsRequired);
        }

        /**
         * @param ignCacheOp IgniteCache operation.
         * @param clntCacheOp ClientCache operation.
         * @param restCmd Rest client command.
         */
        TestOperation(GridTestUtils.ConsumerX<IgniteCache<String, String>> ignCacheOp,
            GridTestUtils.ConsumerX<ClientCache<String, String>> clntCacheOp,
            GridTestUtils.ConsumerX<GridClientData> gridClntDataOp,
            GridRestCommand restCmd) {
            this(ignCacheOp, clntCacheOp, gridClntDataOp, restCmd, true);
        }

        /**
         * @param ignCacheOp IgniteCache operation.
         * @param clntCacheOp ClientCache operation.
         * @param restCmd Rest client command.
         * @param valIsRequired Test operation requires existence of value in a cache.
         */
        TestOperation(GridTestUtils.ConsumerX<IgniteCache<String, String>> ignCacheOp,
            GridTestUtils.ConsumerX<ClientCache<String, String>> clntCacheOp,
            GridTestUtils.ConsumerX<GridClientData> gridClntDataOp,
            GridRestCommand restCmd,
            boolean valIsRequired) {
            this.ignCacheOp = ignCacheOp;
            this.clntCacheOp = clntCacheOp;
            this.gridClntDataOp = gridClntDataOp;
            this.restCmd = restCmd;
            this.valIsRequired = valIsRequired;
        }
    }

    /**
     * Test event's adapter.
     */
    static class TestEventAdapter {
        /** CacheEvent. */
        private final CacheEvent cacheEvt;

        /** CacheQueryExecutedEvent. */
        private final CacheQueryExecutedEvent<String, String> qryExecEvt;

        /** CacheQueryReadEvent. */
        private final CacheQueryReadEvent<String, String> qryReadEvt;

        /**
         * @param evt Event.
         */
        TestEventAdapter(Event evt) {
            if (evt instanceof CacheEvent) {
                cacheEvt = (CacheEvent)evt;
                qryReadEvt = null;
                qryExecEvt = null;
            }
            else if (evt instanceof CacheQueryExecutedEvent) {
                qryExecEvt = (CacheQueryExecutedEvent<String, String>)evt;
                cacheEvt = null;
                qryReadEvt = null;
            }
            else if (evt instanceof CacheQueryReadEvent) {
                qryReadEvt = (CacheQueryReadEvent<String, String>)evt;
                cacheEvt = null;
                qryExecEvt = null;
            }
            else
                throw new IllegalArgumentException("Unexpected event=[" + evt + "]");
        }

        /**
         * @return Event's type.
         */
        int type() {
            if (cacheEvt != null)
                return cacheEvt.type();

            if (qryExecEvt != null)
                return qryExecEvt.type();

            return qryReadEvt.type();
        }

        /**
         * @return Event's cache name.
         */
        String cacheName() {
            if (cacheEvt != null)
                return cacheEvt.cacheName();

            if (qryExecEvt != null)
                return qryExecEvt.cacheName();

            return qryReadEvt.cacheName();
        }

        /**
         * @return Event's subject id.
         */
        UUID subjectId() {
            if (cacheEvt != null)
                return cacheEvt.subjectId();

            if (qryExecEvt != null)
                return qryExecEvt.subjectId();

            return qryReadEvt.subjectId();
        }

        /**
         * @return Event's new value.
         */
        Object newValue() {
            return cacheEvt != null ? cacheEvt.newValue() : null;
        }
    }
}

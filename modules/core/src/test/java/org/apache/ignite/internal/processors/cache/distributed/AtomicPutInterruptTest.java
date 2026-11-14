/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.distributed;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.cache.Cache;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.processor.EntryProcessorResult;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheEntry;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.FailureHandler;
import org.apache.ignite.failure.FailureHandlerWithCallback;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.processors.cache.CacheConflictResolutionManager;
import org.apache.ignite.internal.processors.cache.CacheObjectValueContext;
import org.apache.ignite.internal.processors.cache.GridCacheManagerAdapter;
import org.apache.ignite.internal.processors.cache.binary.CacheObjectBinaryProcessorImpl;
import org.apache.ignite.internal.processors.cache.version.CacheVersionConflictResolver;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionConflictContext;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionedEntryEx;
import org.apache.ignite.internal.processors.marshaller.MappingAcceptedMessage;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.AbstractTestPluginProvider;
import org.apache.ignite.plugin.PluginContext;
import org.apache.ignite.spi.discovery.tcp.TestTcpDiscoverySpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.GridTestUtils.DiscoveryHook;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Checks that an interrupt during an ATOMIC update that registers the class of the value fails only the update.
 */
public class AtomicPutInterruptTest extends GridCommonAbstractTest {
    /** Name of the cache whose updates the conflict resolver merges. */
    private static final String CONFLICT_CACHE = "conflict";

    /** Value that the conflict resolver writes instead of the new value. */
    private static final Integer MERGED_VAL = 42;

    /** Error passed to the failure handler. */
    private final AtomicReference<Throwable> failure = new AtomicReference<>();

    /** While set, holds back the messages that complete the registration of a class name. */
    private volatile CountDownLatch mappingAcceptedLatch;

    /** Logger of the nodes, so a test can check what they log. */
    private final ListeningTestLogger listeningLog = new ListeningTestLogger(log);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName)
            .setGridLogger(listeningLog)
            .setPluginProviders(new MergingResolverPluginProvider());

        ((TestTcpDiscoverySpi)cfg.getDiscoverySpi()).discoveryHook(new DiscoveryHook() {
            @Override public void beforeDiscovery(DiscoveryCustomMessage customMsg) {
                CountDownLatch latch = mappingAcceptedLatch;

                if (latch != null && customMsg instanceof MappingAcceptedMessage) {
                    // Waits with a time limit. If the update is not interrupted, it waits for this message, and the
                    // test must then fail with its diagnostic instead of hanging.
                    try {
                        U.await(latch, 10, TimeUnit.SECONDS);
                    }
                    catch (IgniteInterruptedCheckedException ignored) {
                        // The discovery thread is stopping. U.await() keeps its interrupt flag set.
                    }
                }
            }
        });

        return cfg.setCacheConfiguration(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setAtomicityMode(ATOMIC)
            .setBackups(1)
            .setWriteSynchronizationMode(FULL_SYNC));
    }

    /** {@inheritDoc} */
    @Override protected FailureHandler getFailureHandler(String igniteInstanceName) {
        return new FailureHandlerWithCallback(ctx -> failure.compareAndSet(null, ctx.error()));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        // Removes the class names registered by earlier runs.
        cleanPersistenceDir();

        startGrid(0);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /** */
    @Test
    public void testInterruptedPutOfUnregisteredExternalizableClass() throws Exception {
        IgniteCache<Integer, Object> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        checkInterruptedUpdateFails(() -> cache.put(1, new ExternalizableValue(1)));

        checkCacheWorks(cache);
    }

    /** */
    @Test
    public void testInterruptedInvokeWithUnregisteredExternalizableResult() throws Exception {
        IgniteCache<Integer, Object> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        checkInterruptedUpdateFails(() -> cache.invoke(1, new SetExternalizableValue()));

        checkCacheWorks(cache);
    }

    /** The interrupted invoke must not write anything, so the key keeps both its value and its version. */
    @Test
    public void testInterruptedInvokeWithUnregisteredExternalizableResultDoesNotWrite() throws Exception {
        IgniteCache<Integer, Object> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        cache.put(1, 1);

        CacheEntry<Integer, Object> before = cache.getEntry(1);

        checkInterruptedUpdateFails(() -> cache.invoke(1, new SetExternalizableValue()));

        CacheEntry<Integer, Object> after = cache.getEntry(1);

        assertEquals(1, after.getValue());
        assertEquals("The failed invoke wrote the key", before.version(), after.version());

        checkCacheWorks(cache);
    }

    /**
     * The binary type ID of the class is registered first, so the put has only the optimized marshaller's own
     * registration left, which happens when the value is written.
     */
    @Test
    public void testInterruptedPutOfExternalizableClassWithRegisteredBinaryType() throws Exception {
        IgniteCache<Integer, Object> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        ((CacheObjectBinaryProcessorImpl)grid(0).context().cacheObjects()).binaryContext()
            .registerType(ExternalizableValue.class, true, false);

        checkInterruptedUpdateFails(() -> cache.put(1, new ExternalizableValue(1)));

        checkCacheWorks(cache);
    }

    /**
     * The second value of the request fails to register its class. The whole request must fail before the first
     * value is written, so neither the primary nor the backup has it.
     */
    @Test
    public void testInterruptedPutAllWithUnregisteredExternalizableClass() throws Exception {
        startGrid(1);

        awaitPartitionMapExchange();

        IgniteCache<Integer, Object> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        List<Integer> keys = primaryKeys(cache, 2);

        // Sorted by key, so the value with a registered class is written first.
        Map<Integer, Object> vals = new TreeMap<>();

        vals.put(keys.get(0), 1);
        vals.put(keys.get(1), new ExternalizableValue(1));

        checkInterruptedUpdateFails(() -> cache.putAll(vals));

        Integer key = keys.get(0);

        // The request failed before anything was written, so neither node has the first key.
        assertNull("The first key was written", cache.localPeek(key, CachePeekMode.PRIMARY));
        assertNull("The first key was written", grid(1).cache(DEFAULT_CACHE_NAME).localPeek(key, CachePeekMode.BACKUP));

        checkCacheWorks(cache);
    }

    /**
     * The primary of the key is on another node. The interrupted thread also waits for the response of that node, so
     * the interrupt check alone does not show where the update failed. This test checks that no failure handler is
     * called and that the cache still works.
     */
    @Test
    public void testInterruptedPutToRemotePrimaryOfUnregisteredExternalizableClass() throws Exception {
        startGrid(1);

        awaitPartitionMapExchange();

        // The primary of the key is node 0, and the put is sent from node 1.
        Integer key = primaryKey(grid(0).cache(DEFAULT_CACHE_NAME));

        IgniteCache<Integer, Object> cache = grid(1).cache(DEFAULT_CACHE_NAME);

        checkInterruptedUpdateFails(() -> cache.put(key, new ExternalizableValue(1)));

        checkCacheWorks(cache);
    }

    /**
     * The holder class is registered first, so the interrupted put has only the nested class left to register, and
     * that class is seen only when the value is written.
     */
    @Test
    public void testInterruptedPutOfNestedUnregisteredExternalizableClass() throws Exception {
        IgniteCache<Integer, Object> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        // Registers the holder class. The nested Integer is a system type, so nothing else is registered.
        cache.put(2, new ExternalizableHolder(1));

        checkInterruptedUpdateFails(() -> cache.put(1, new ExternalizableHolder(new ExternalizableValue(1))));

        checkCacheWorks(cache);
    }

    /** The value is a normal class whose field holds an Externalizable value of an unregistered class. */
    @Test
    public void testInterruptedPutOfValueWithUnregisteredExternalizableField() throws Exception {
        IgniteCache<Integer, Object> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        // Registers the outer class and its field type, so only the class of the field value is left to register.
        cache.put(2, new ValueWithExternalizableField(new ExternalizableHolder(1)));

        checkInterruptedUpdateFails(() -> cache.put(1, new ValueWithExternalizableField(new ExternalizableValue(1))));

        checkCacheWorks(cache);
    }

    /**
     * The entry processor creates a normal class whose field holds an Externalizable value of an unregistered class,
     * so the nested class is first seen inside the cache update.
     */
    @Test
    public void testInterruptedInvokeWithUnregisteredExternalizableField() throws Exception {
        IgniteCache<Integer, Object> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        // Registers the outer class and its field type, so only the class of the field value is left to register.
        cache.put(2, new ValueWithExternalizableField(new ExternalizableHolder(1)));

        checkInterruptedUpdateFails(() -> cache.invoke(1, new SetValueWithExternalizableField()));

        checkCacheWorks(cache);
    }

    /**
     * The binary type ID of the class is registered first, so the entry processor's value has only the optimized
     * marshaller's own registration left, which happens inside the cache update.
     */
    @Test
    public void testInterruptedInvokeOfExternalizableClassWithRegisteredBinaryType() throws Exception {
        IgniteCache<Integer, Object> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        ((CacheObjectBinaryProcessorImpl)grid(0).context().cacheObjects()).binaryContext()
            .registerType(ExternalizableValue.class, true, false);

        checkInterruptedUpdateFails(() -> cache.invoke(1, new SetExternalizableValue()));

        checkCacheWorks(cache);
    }

    /**
     * The holder class is registered first, so the entry processor's value has only the nested class left to register,
     * and that class is seen only when the value is written inside the cache update.
     */
    @Test
    public void testInterruptedInvokeOfNestedUnregisteredExternalizableClass() throws Exception {
        IgniteCache<Integer, Object> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        cache.put(2, new ExternalizableHolder(1));

        checkInterruptedUpdateFails(() -> cache.invoke(1, new SetExternalizableHolder()));

        checkCacheWorks(cache);
    }

    /**
     * An invoke of several keys with a write-through store runs the entry processors before the B+Tree, in
     * {@code GridDhtAtomicCache.updateWithBatch()}. With {@code copyOnRead=false}, the store write doesn't make the
     * bytes of the new values, so they are first made inside the B+Tree.
     */
    @Test
    public void testInterruptedInvokeAllWithStoreAndUnregisteredExternalizableResult() throws Exception {
        IgniteCache<Integer, Object> cache = grid(0).createCache(new CacheConfiguration<Integer, Object>("store")
            .setAtomicityMode(ATOMIC)
            .setCopyOnRead(false)
            .setCacheStoreFactory(FactoryBuilder.factoryOf(NoopStore.class))
            .setWriteThrough(true));

        checkInterruptedUpdateFails(() ->
            cache.invokeAll(F.asSet(1, 2), new SetExternalizableValue()).values().forEach(EntryProcessorResult::get));

        checkCacheWorks(cache);
    }

    /**
     * The store returns an Externalizable value of an unregistered class for the second key, so the registration is
     * interrupted while the old value of that key is read, after the first key is already updated. Only the second
     * key must fail, so the backup still gets the first key.
     */
    @Test
    public void testInterruptedInvokeAllWithUnregisteredExternalizableValueFromStore() throws Exception {
        startGrid(1);

        IgniteCache<Integer, Object> cache = grid(0).createCache(new CacheConfiguration<Integer, Object>("store")
            .setAtomicityMode(ATOMIC)
            .setBackups(1)
            .setWriteSynchronizationMode(FULL_SYNC)
            .setCacheStoreFactory(FactoryBuilder.factoryOf(ExternalizableValueStore.class))
            .setReadThrough(true));

        awaitPartitionMapExchange();

        List<Integer> keys = primaryKeys(cache, 2);

        Integer key = keys.get(0);

        // The first key is in the cache, so only the second key is loaded from the store.
        cache.put(key, 0);

        // Sorted by key, so the first key is updated first.
        checkInterruptedUpdateFails(() ->
            cache.invokeAll(new TreeSet<>(keys), new SetIntegerValue()).values().forEach(EntryProcessorResult::get));

        assertEquals(1, cache.localPeek(key, CachePeekMode.PRIMARY));

        IgniteCache<Integer, Object> backupCache = grid(1).cache("store");

        // The backup is updated after the invoke returns, so this waits for it.
        assertTrue("The backup did not get the first key", GridTestUtils.waitForCondition(
            () -> Objects.equals(1, backupCache.localPeek(key, CachePeekMode.BACKUP)), 5_000));

        checkCacheWorks(cache);
    }

    /**
     * The conflict resolver merges the update to another value, so the interrupted registration of the entry
     * processor's value must not cancel the update, and the merged value must be written.
     */
    @Test
    public void testInterruptedInvokeMergedByConflictResolver() throws Exception {
        IgniteCache<Integer, Object> cache = grid(0).createCache(new CacheConfiguration<Integer, Object>(CONFLICT_CACHE)
            .setAtomicityMode(ATOMIC));

        // Holds back the registration of new class names, so the registration of the entry processor's value is
        // interrupted.
        mappingAcceptedLatch = new CountDownLatch(1);

        Thread.currentThread().interrupt();

        try {
            cache.invoke(1, new SetExternalizableValue());
        }
        finally {
            // Clears the interrupt flag, so it does not affect the rest of the test.
            Thread.interrupted();

            // Lets the held registration finish.
            mappingAcceptedLatch.countDown();

            mappingAcceptedLatch = null;
        }

        assertEquals(MERGED_VAL, cache.get(1));

        assertNull("Failure handler was called: " + failure.get(), failure.get());

        // The conflict resolver changes every value of its cache, so the default cache is checked instead.
        checkCacheWorks(grid(0).cache(DEFAULT_CACHE_NAME));
    }

    /**
     * Runs the update on an interrupted thread and checks that only the update fails.
     *
     * @param update Cache update.
     */
    private void checkInterruptedUpdateFails(Runnable update) {
        // The failed update is an expected outcome, so it must not be logged as an unexpected error.
        LogListener unexpectedErrLsnr = LogListener.matches("Unexpected exception during cache update")
            .times(0)
            .build();

        listeningLog.registerListener(unexpectedErrLsnr);

        // Holds back the registration of new class names, so the update always has to wait for it and is interrupted
        // there, however fast the discovery is.
        mappingAcceptedLatch = new CountDownLatch(1);

        Thread.currentThread().interrupt();

        try {
            GridTestUtils.assertThrowsWithCause(update, IgniteInterruptedCheckedException.class);
        }
        catch (AssertionError e) {
            // Tells whether the update cleared the interrupt flag or never had to wait for a class registration.
            throw new AssertionError(e.getMessage() + " [interruptFlagStillSet=" +
                Thread.currentThread().isInterrupted() + ']', e);
        }
        finally {
            // Clears the interrupt flag, so it does not affect the rest of the test.
            Thread.interrupted();

            // Lets the held registration finish.
            mappingAcceptedLatch.countDown();

            mappingAcceptedLatch = null;
        }

        assertNull("Failure handler was called: " + failure.get(), failure.get());
        assertTrue("The failed update was logged as an unexpected error", unexpectedErrLsnr.check());
    }

    /** Checks that the cache still works after the interrupted update. */
    private void checkCacheWorks(IgniteCache<Integer, Object> cache) {
        cache.put(1, new ExternalizableValue(2));

        ExternalizableValue val = (ExternalizableValue)cache.get(1);

        assertEquals(2, val.val);
    }

    /** Externalizable value, so it is written by the optimized marshaller inside the cache update. */
    private static class ExternalizableValue implements Externalizable {
        /** */
        private int val;

        /** Required by {@link Externalizable}. */
        public ExternalizableValue() {
            // No-op.
        }

        /** */
        ExternalizableValue(int val) {
            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeInt(val);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException {
            val = in.readInt();
        }
    }

    /** Externalizable value that writes another object, so the class of that object is seen only when it is written. */
    private static class ExternalizableHolder implements Externalizable {
        /** */
        private Object obj;

        /** Required by {@link Externalizable}. */
        public ExternalizableHolder() {
            // No-op.
        }

        /** */
        ExternalizableHolder(Object obj) {
            this.obj = obj;
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeObject(obj);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            obj = in.readObject();
        }
    }

    /** Normal class, written as a binary object, whose field holds an Externalizable value. */
    private static class ValueWithExternalizableField {
        /** */
        private final Object val;

        /** */
        ValueWithExternalizableField(Object val) {
            this.val = val;
        }
    }

    /** Creates a holder of an Externalizable value inside the cache update, so the nested class is written there. */
    private static class SetExternalizableHolder implements CacheEntryProcessor<Integer, Object, Void> {
        /** {@inheritDoc} */
        @Override public Void process(MutableEntry<Integer, Object> entry, Object... args) {
            entry.setValue(new ExternalizableHolder(new ExternalizableValue(entry.getKey())));

            return null;
        }
    }

    /** Creates the Externalizable value inside the cache update, so it is written there. */
    private static class SetExternalizableValue implements CacheEntryProcessor<Integer, Object, Void> {
        /** {@inheritDoc} */
        @Override public Void process(MutableEntry<Integer, Object> entry, Object... args) {
            entry.setValue(new ExternalizableValue(entry.getKey()));

            return null;
        }
    }

    /** Creates a value with an Externalizable field inside the cache update, so it is written there. */
    private static class SetValueWithExternalizableField implements CacheEntryProcessor<Integer, Object, Void> {
        /** {@inheritDoc} */
        @Override public Void process(MutableEntry<Integer, Object> entry, Object... args) {
            entry.setValue(new ValueWithExternalizableField(new ExternalizableValue(entry.getKey())));

            return null;
        }
    }

    /** Sets a value of a system type, so the entry processor itself registers no class. */
    private static class SetIntegerValue implements CacheEntryProcessor<Integer, Object, Void> {
        /** {@inheritDoc} */
        @Override public Void process(MutableEntry<Integer, Object> entry, Object... args) {
            entry.setValue(1);

            return null;
        }
    }

    /** Store that keeps nothing, so a cache can use write-through. */
    public static class NoopStore extends CacheStoreAdapter<Integer, Object> {
        /** {@inheritDoc} */
        @Override public Object load(Integer key) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void write(Cache.Entry<? extends Integer, ?> entry) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void delete(Object key) {
            // No-op.
        }
    }

    /** Store that loads an Externalizable value for every key, so a cache can read through. */
    public static class ExternalizableValueStore extends CacheStoreAdapter<Integer, Object> {
        /** {@inheritDoc} */
        @Override public Object load(Integer key) {
            return new ExternalizableValue(key);
        }

        /** {@inheritDoc} */
        @Override public void write(Cache.Entry<? extends Integer, ?> entry) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void delete(Object key) {
            // No-op.
        }
    }

    /** Gives the nodes {@link MergingResolutionManager}. */
    private static class MergingResolverPluginProvider extends AbstractTestPluginProvider {
        /** {@inheritDoc} */
        @Override public String name() {
            return "MergingResolverPluginProvider";
        }

        /** {@inheritDoc} */
        @Override public <T> T createComponent(PluginContext ctx, Class<T> cls) {
            if (cls != CacheConflictResolutionManager.class)
                return null;

            return (T)new MergingResolutionManager<>();
        }
    }

    /** Gives a conflict resolver only to the cache {@link #CONFLICT_CACHE}, so the other caches work as usual. */
    private static class MergingResolutionManager<K, V> extends GridCacheManagerAdapter<K, V>
        implements CacheConflictResolutionManager<K, V> {
        /** {@inheritDoc} */
        @Override public CacheVersionConflictResolver conflictResolver() {
            return CONFLICT_CACHE.equals(cctx.name()) ? new MergingResolver() : null;
        }
    }

    /** Merges every update to {@link #MERGED_VAL}, without reading the new value. */
    private static class MergingResolver implements CacheVersionConflictResolver {
        /** {@inheritDoc} */
        @Override public <K, V> GridCacheVersionConflictContext<K, V> resolve(
            CacheObjectValueContext ctx,
            GridCacheVersionedEntryEx<K, V> oldEntry,
            GridCacheVersionedEntryEx<K, V> newEntry,
            boolean atomicVerComparator
        ) {
            GridCacheVersionConflictContext<K, V> res = new GridCacheVersionConflictContext<>(ctx, oldEntry, newEntry);

            res.merge((V)MERGED_VAL, CU.TTL_ETERNAL, CU.EXPIRE_TIME_ETERNAL);

            return res;
        }
    }
}

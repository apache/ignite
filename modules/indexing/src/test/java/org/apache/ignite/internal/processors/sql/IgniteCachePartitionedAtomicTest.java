package org.apache.ignite.internal.processors.sql;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.cache.processor.EntryProcessorResult;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;

public class IgniteCachePartitionedAtomicTest extends GridCommonAbstractTest {
    protected static final long FUT_TIMEOUT = 10_000L;

    protected Consumer<Runnable> shouldFail = (op) -> assertThrowsWithCause(op, IgniteException.class);

    /** */
    protected Consumer<Runnable> shouldSucceed = Runnable::run;

    /** */
    protected <K, V> void checkReplaceOps(Consumer<Runnable> checker, IgniteCache<K, V> cache, T2<K, V> val, V okVal) {
        K k = val.get1();
        V v = val.get2();

        cache.put(k, okVal);

        CacheEntryProcessor<K, V, ?> entryProcessor = (e, arguments) -> {
            e.setValue((V)arguments[0]);

            return null;
        };

        Stream<Runnable> ops = Stream.of(
            () -> cache.replace(k, v),
            () -> cache.getAndReplace(k, v),
            () -> cache.replace(k, okVal, v),
            () -> cache.invoke(k, entryProcessor, v),
            () -> cache.replaceAsync(k, v).get(FUT_TIMEOUT),
            () -> cache.getAndReplaceAsync(k, v).get(FUT_TIMEOUT),
            () -> cache.replaceAsync(k, okVal, v).get(FUT_TIMEOUT),
            () -> cache.invokeAsync(k, entryProcessor, v).get(FUT_TIMEOUT)
        );

        ops.forEach(checker);
    }

    /** */
    protected <K, V> void checkPutOps(Consumer<Runnable> checker, IgniteCache<K, V> cache, T2<K, V> val) {
        K k = val.get1();
        V v = val.get2();

        Stream<Runnable> ops = Stream.of(
            () -> cache.put(k, v),
            () -> cache.putIfAbsent(k, v),
            () -> cache.getAndPut(k, v),
            () -> cache.getAndPutIfAbsent(k, v),
            () -> cache.putAsync(k, v).get(FUT_TIMEOUT),
            () -> cache.putIfAbsentAsync(k, v).get(FUT_TIMEOUT),
            () -> cache.getAndPutAsync(k, v).get(FUT_TIMEOUT),
            () -> cache.getAndPutIfAbsentAsync(k, v).get(FUT_TIMEOUT)
        );

        ops.forEach(checker);
    }

    /** */
    protected <K, V> void checkPutAll(Consumer<Runnable> checker, IgniteCache<K, V> cache, T2<K, V>... entries) {
        CacheEntryProcessor<K, V, ?> entryProcessor = (e, arguments) -> {
            e.setValue(((Iterator<V>)arguments[0]).next());

            return null;
        };

        Map<K, V> vals = Arrays.stream(entries).collect(Collectors.toMap(T2::get1, T2::get2));

        Stream<Runnable> ops = Stream.of(
            () -> cache.putAll(vals),
            () -> cache.putAllAsync(vals).get(FUT_TIMEOUT),
            () -> {
                Map<K, ? extends EntryProcessorResult<?>> map =
                    cache.invokeAll(vals.keySet(), entryProcessor, vals.values().iterator());

                for (EntryProcessorResult<?> result : map.values())
                    log.info(">>> " + result.get());
            },
            () -> {
                Map<K, ? extends EntryProcessorResult<?>> map =
                    cache.invokeAllAsync(vals.keySet(), entryProcessor, vals.values().iterator()).get(FUT_TIMEOUT);

                for (EntryProcessorResult<?> result : map.values())
                    log.info(">>> " + result.get());
            }
        );

        ops.forEach(checker);
    }

    /**
     * @param qryEntity Query entity.
     * @return Cache configuration.
     */
    protected CacheConfiguration cacheConfiguration(QueryEntity qryEntity) {
        CacheConfiguration<?, ?> cache = defaultCacheConfiguration();

        cache.setCacheMode(cacheMode());
        cache.setAtomicityMode(atomicityMode());
        cache.setBackups(1);
        cache.setWriteSynchronizationMode(FULL_SYNC);

        cache.setQueryEntities(Collections.singletonList(qryEntity));

        return cache;
    }

    /** */
    @NotNull protected CacheAtomicityMode atomicityMode() {
        return ATOMIC;
    }

    /** */
    @NotNull protected CacheMode cacheMode() {
        return PARTITIONED;
    }
}

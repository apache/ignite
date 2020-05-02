package org.apache.ignite.internal;

import java.util.Collections;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

public class LoadHelperV2 extends GridCommonAbstractTest {
    @Test
    public void startServer() throws Exception {
        IgniteEx grid = startGrid(0);

        IgniteCache<Object, Object> cache = grid.getOrCreateCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_ASYNC)
            .setAtomicityMode(CacheAtomicityMode.ATOMIC)
            .setBackups(0));

        Random random = new Random();

        int keyRange = 100;

        for (int i = 0; i < keyRange; i++) {
            cache.put(i, random.nextInt());
        }

        int batch = 1000;
        AtomicBoolean stop = new AtomicBoolean();
        AtomicLong cnt = new AtomicLong();

        IgniteInternalFuture<?> fut = GridTestUtils.runAsync(() -> {
            while (!stop.get()) {
                for (int i = 0; i < batch; i++) {
                    cache.get(random.nextInt(keyRange));

                    cnt.incrementAndGet();
                }
            }
        });

        long duration = 1 * 60;

        for (int i = 0; i < duration; i++) {
            long old = cnt.get();

            U.sleep(1000);

            long speed = cnt.get() - old;

            System.out.println("MY speed = " + speed + " ops/sec");
        }

        stop.set(true);
        fut.get();
    }

    @Test
    public void startServerSQL() throws Exception {
        IgniteEx grid = startGrid(0);

        IgniteCache<Integer, Integer> cache = grid.getOrCreateCache(
            new CacheConfiguration<Integer, Integer>(DEFAULT_CACHE_NAME)
                .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_ASYNC)
                .setAtomicityMode(CacheAtomicityMode.ATOMIC)
                .setBackups(0)
                .setQueryEntities(Collections.singletonList(new QueryEntity(Integer.class, Integer.class)
                    .setTableName(DEFAULT_CACHE_NAME))));

        Random random = new Random();

        int keyRange = 100;

        for (int i = 0; i < keyRange; i++) {
            cache.put(i, random.nextInt());
        }

        int batch = 1000;
        AtomicBoolean stop = new AtomicBoolean();
        AtomicLong cnt = new AtomicLong();

        IgniteInternalFuture<?> fut = GridTestUtils.runAsync(() -> {
            while (!stop.get()) {
                for (int i = 0; i < batch; i++) {
                    SqlFieldsQuery qry1 = new SqlFieldsQuery(
                        "SELECT _key FROM " + cache.getName() + " WHERE _key <= " +
                            new Random().nextInt(100) + " LIMIT 1")
                        .setSchema(cache.getName());

                    cache.query(qry1).getAll();

                    cnt.incrementAndGet();
                }
            }
        });

        long duration = 1 * 60;

        for (int i = 0; i < duration; i++) {
            long old = cnt.get();

            U.sleep(1000);

            long speed = cnt.get() - old;

            System.out.println("MY speed = " + speed + " ops/sec");
        }

        stop.set(true);
        fut.get();
    }
}

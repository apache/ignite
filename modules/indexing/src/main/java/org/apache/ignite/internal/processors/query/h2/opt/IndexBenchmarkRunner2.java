package org.apache.ignite.internal.processors.query.h2.opt;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager;
import org.apache.ignite.internal.util.typedef.internal.U;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

public class IndexBenchmarkRunner2 {
    private static final String CACHE_NAME = "cache";

    private static final int IDX_CNT = 1;

    private static final long KEY_CNT = 1_000_000;

    private static final int THREAD_CNT = 4;

    private static final AtomicInteger READY_CNT = new AtomicInteger();

    private static final LongAdder OPS = new LongAdder();

    public static void main(String[] args) throws Exception {
        U.delete(new File("C:\\Personal\\code\\incubator-ignite\\work"));

        IgniteConfiguration cfg = new IgniteConfiguration().setLocalHost("127.0.0.1");

        DataStorageConfiguration dsCfg = new DataStorageConfiguration().setWalMode(WALMode.LOG_ONLY);

        dsCfg.getDefaultDataRegionConfiguration().setPersistenceEnabled(true);
        dsCfg.getDefaultDataRegionConfiguration().setMaxSize(4 * 1024 * 1024 * 1024L);

        dsCfg.setCheckpointFrequency(Long.MAX_VALUE);

        cfg.setDataStorageConfiguration(dsCfg);

        try (Ignite node = Ignition.start(cfg)) {
            node.active(true);

            IgniteCache<Integer, ValueObject> cache = node.createCache(cacheConfig(IDX_CNT));

            String extra = genExtra(ThreadLocalRandom.current());

            cache.put(1, new ValueObject(1, extra));
            cache.remove(1);

            FileWriteAheadLogManager.print = true;

            System.out.println("PUT");
            cache.put(1, new ValueObject(1, extra));
            System.out.println();

            System.out.println("UPDATE I");
            cache.put(1, new ValueObject(2, extra));
            System.out.println();

            System.out.println("UPDATE S");
            cache.put(1, new ValueObject(2, genExtra(ThreadLocalRandom.current())));
            System.out.println();

            System.out.println("DONE");
        }
    }

    private static final int EXTRA_SIZE = 128;

    private static String genExtra(ThreadLocalRandom rand) {
        StringBuilder builder = new StringBuilder(EXTRA_SIZE);
        for (int i = 0; i < EXTRA_SIZE; ++i)
            builder.append((char) ('a' + rand.nextInt('z' - 'a' + 1)));
        return builder.toString();
    }

    private static CacheConfiguration<Integer, ValueObject> cacheConfig(int idxCnt) {
        CacheConfiguration<Integer, ValueObject> ccfg = new CacheConfiguration<>();

        ccfg.setName(CACHE_NAME);
        ccfg.setQueryEntities(Collections.singleton(queryEntity(idxCnt)));

        return ccfg;
    }

    private static QueryEntity queryEntity(int idxCnt) {
        QueryEntity entity = new QueryEntity(Integer.class, ValueObject.class);

//        Collection<QueryIndex> idxs = new ArrayList<>();
//
//        for (int i = 0; i < idxCnt; i++) {
//            String fieldName = "f" + i;
//
//            QueryIndex idx = new QueryIndex().setFieldNames(Collections.singleton(fieldName), true);
//
//            idxs.add(idx);
//        }
//
//        entity.setIndexes(idxs);

        return entity;
    }

    private static class ValueObject {
        @QuerySqlField(index = true)
        private int i;

        @QuerySqlField
        private String s;

        public ValueObject(int i, String s) {
            this.i = i;
            this.s = s;
        }
    }
}

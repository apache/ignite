package org.apache.ignite.cache;

import com.sun.javaws.CacheUtil;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;

/**
 */
public class EntryProcessorInOptimisticTxTest extends GridCommonAbstractTest{
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setCacheConfiguration(new CacheConfiguration("cache")
                .setCacheMode(CacheMode.PARTITIONED)
                .setGroupName("1")
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                .setRebalanceMode(CacheRebalanceMode.SYNC)
                .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
                .setMaxConcurrentAsyncOperations(0)
                .setBackups(0));
    }

    public void testOneNode() throws Exception {
        Ignite ignite = startGrid(0);
        try {
            IgniteCache cache = ignite.cache("cache");
            Key k = new Key(1L);
            Value value = new Value();
            value.field = "1";

            cache.put(k, value);

            CustomCacheEntryProcessor1 cacheEntryProcessor1 = new CustomCacheEntryProcessor1();
            CustomCacheEntryProcessor2 cacheEntryProcessor2 = new CustomCacheEntryProcessor2();

            try (Transaction transaction = ignite.transactions().txStart(TransactionConcurrency.OPTIMISTIC, TransactionIsolation.SERIALIZABLE, 300_000L, 1)) {
                cache.put(1,1);
                cache.invoke(k, cacheEntryProcessor1);
                assertNotNull(cache.get(k));
                cache.invoke(k, cacheEntryProcessor2);

                transaction.commit();
            }
        }finally {
            stopAllGrids();
        }
    }

    /**
     *
     */
    public static class CustomCacheEntryProcessor1 implements CacheEntryProcessor {

        /** {@inheritDoc} */
        public Object process(MutableEntry entry, Object... arguments) throws EntryProcessorException {
//            assertNotNull(entry.getValue());
            Value val = new Value();
            val.field = "Invoke 1";
            System.err.println("!!! " + Thread.currentThread().getName() + " " + entry.getValue());
            Thread.currentThread().dumpStack();
            entry.setValue(val);
            return null;
        }
    }

    /**
     *
     */
    public static class CustomCacheEntryProcessor2 implements CacheEntryProcessor {

        /** {@inheritDoc} */
        public Object process(MutableEntry entry, Object... arguments) throws EntryProcessorException {
//            assertNotNull(entry.getValue());
            Value val = new Value();
            val.field = "Invoke 2";
            System.err.println("!!! " + Thread.currentThread().getName() + " " + entry.getValue());
            Thread.currentThread().dumpStack();
            entry.setValue(val);
            return null;
        }
    }

    public static class Key {
        private final Long id;

        public Key(Long id) {
            this.id = id;
        }

        @Override
        public int hashCode() {
            return super.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            return super.equals(obj);
        }
    }

    public static class Value {
        private String field;

        @Override public String toString() {
            return "Value{" +
                "field='" + field + '\'' +
                '}';
        }
    }
}

package org.apache.ignite.snippets;

import javax.cache.CacheException;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionDeadlockException;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionOptimisticException;
import org.apache.ignite.transactions.TransactionTimeoutException;

public class PerformingTransactions {

    public static void main(String[] args) {
        deadlockDetectionExample();
    }

    public static void runAll() {
        enablingTransactions();
        executingTransactionsExample();
        optimisticTransactionExample();
        deadlockDetectionExample();

    }

    public static void enablingTransactions() {
        // tag::enabling[]
        CacheConfiguration cacheCfg = new CacheConfiguration();

        cacheCfg.setName("cacheName");

        cacheCfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);

        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setCacheConfiguration(cacheCfg);

        // Optional transaction configuration. Configure TM lookup here.
        TransactionConfiguration txCfg = new TransactionConfiguration();

        cfg.setTransactionConfiguration(txCfg);

        // Start a node
        Ignition.start(cfg);
        // end::enabling[]
        Ignition.ignite().close();
    }

    public static void executingTransactionsExample() {
        try (Ignite i = Ignition.start()) {
            CacheConfiguration<String, Integer> cfg = new CacheConfiguration<>();
            cfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
            cfg.setName("myCache");
            IgniteCache<String, Integer> cache = i.getOrCreateCache("myCache");
            cache.put("Hello", 1);
            // tag::executing[]
            Ignite ignite = Ignition.ignite();

            IgniteTransactions transactions = ignite.transactions();

            try (Transaction tx = transactions.txStart()) {
                Integer hello = cache.get("Hello");

                if (hello == 1)
                    cache.put("Hello", 11);

                cache.put("World", 22);

                tx.commit();
            }
            // end::executing[]
            System.out.println(cache.get("Hello"));
            System.out.println(cache.get("World"));
        }
    }

    public static void optimisticTransactionExample() {
        try (Ignite ignite = Ignition.start()) {
            // tag::optimistic[]
            CacheConfiguration<Integer, String> cfg = new CacheConfiguration<>();
            cfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
            cfg.setName("myCache");
            IgniteCache<Integer, String> cache = ignite.getOrCreateCache(cfg);

            // Re-try the transaction a limited number of times.
            int retryCount = 10;
            int retries = 0;

            // Start a transaction in the optimistic mode with the serializable isolation
            // level.
            while (retries < retryCount) {
                retries++;
                try (Transaction tx = ignite.transactions().txStart(TransactionConcurrency.OPTIMISTIC,
                        TransactionIsolation.SERIALIZABLE)) {
                    // modify cache entries as part of this transaction.
                    cache.put(1, "foo");
                    cache.put(2, "bar");
                    // commit the transaction
                    tx.commit();

                    // the transaction succeeded. Leave the while loop.
                    break;
                } catch (TransactionOptimisticException e) {
                    // Transaction has failed. Retry.
                }
            }
            // end::optimistic[]
            System.out.println(cache.get(1));
        }
    }

    void timeout() {
        // tag::timeout[]
        // Create a configuration
        IgniteConfiguration cfg = new IgniteConfiguration();

        // Create a Transaction configuration
        TransactionConfiguration txCfg = new TransactionConfiguration();

        // Set the timeout to 20 seconds
        txCfg.setTxTimeoutOnPartitionMapExchange(20000);

        cfg.setTransactionConfiguration(txCfg);

        // Start the node
        Ignition.start(cfg);
        // end::timeout[]
    }

    public static void deadlockDetectionExample() {
        try (Ignite ignite = Ignition.start()) {

            // tag::deadlock[]
            CacheConfiguration<Integer, String> cfg = new CacheConfiguration<>();
            cfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
            cfg.setName("myCache");
            IgniteCache<Integer, String> cache = ignite.getOrCreateCache(cfg);

            try (Transaction tx = ignite.transactions().txStart(TransactionConcurrency.PESSIMISTIC,
                    TransactionIsolation.READ_COMMITTED, 300, 0)) {
                cache.put(1, "1");
                cache.put(2, "1");

                tx.commit();
            } catch (CacheException e) {
                if (e.getCause() instanceof TransactionTimeoutException
                        && e.getCause().getCause() instanceof TransactionDeadlockException)

                    System.out.println(e.getCause().getCause().getMessage());
            }
            // end::deadlock[]
        }
    }
}

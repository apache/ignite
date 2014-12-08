/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.datagrid;

import org.apache.ignite.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;

import java.io.*;

import static org.gridgain.grid.cache.GridCacheFlag.*;
import static org.gridgain.grid.cache.GridCacheTxConcurrency.*;
import static org.gridgain.grid.cache.GridCacheTxIsolation.*;

/**
 * Demonstrates how to use cache transactions.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ggstart.{sh|bat} examples/config/example-cache.xml'}.
 * <p>
 * Alternatively you can run {@link CacheNodeStartup} in another JVM which will
 * start GridGain node with {@code examples/config/example-cache.xml} configuration.
 */
public class CacheTransactionExample {
    /** Cache name. */
    private static final String CACHE_NAME = "partitioned_tx";

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws GridException If example execution failed.
     */
    public static void main(String[] args) throws GridException {
        try (Ignite g = Ignition.start("examples/config/example-cache.xml")) {
            System.out.println();
            System.out.println(">>> Cache transaction example started.");

            // Clean up caches on all nodes before run.
            g.cache(CACHE_NAME).globalClearAll(0);

            GridCache<Integer, Account> cache = g.cache(CACHE_NAME);

            // Initialize.
            cache.putx(1, new Account(1, 100));
            cache.putx(2, new Account(1, 200));

            System.out.println();
            System.out.println(">>> Accounts before deposit: ");
            System.out.println(">>> " + cache.get(1));
            System.out.println(">>> " + cache.get(2));

            // Make transactional deposits.
            deposit(1, 100);
            deposit(2, 200);

            System.out.println();
            System.out.println(">>> Accounts after transfer: ");
            System.out.println(">>> " + cache.get(1));
            System.out.println(">>> " + cache.get(2));

            System.out.println(">>> Cache transaction example finished.");
        }
    }

    /**
     * Make deposit into specified account.
     *
     * @param acctId Account ID.
     * @param amount Amount to deposit.
     * @throws GridException If failed.
     */
    private static void deposit(int acctId, double amount) throws GridException {
        // Clone every object we get from cache, so we can freely update it.
        GridCacheProjection<Integer, Account> cache = Ignition.ignite().<Integer, Account>cache(CACHE_NAME).flagsOn(CLONE);

        try (GridCacheTx tx = cache.txStart(PESSIMISTIC, REPEATABLE_READ)) {
            Account acct = cache.get(acctId);

            assert acct != null;

            // Deposit into account.
            acct.update(amount);

            // Store updated account in cache.
            cache.putx(acctId, acct);

            tx.commit();
        }

        System.out.println();
        System.out.println(">>> Transferred amount: $" + amount);
    }

    /**
     * Account.
     */
    private static class Account implements Serializable, Cloneable {
        /** Account ID. */
        private int id;

        /** Account balance. */
        private double balance;

        /**
         * @param id Account ID.
         * @param balance Balance.
         */
        Account(int id, double balance) {
            this.id = id;
            this.balance = balance;
        }

        /**
         * Change balance by specified amount.
         *
         * @param amount Amount to add to balance (may be negative).
         */
        void update(double amount) {
            balance += amount;
        }

        /** {@inheritDoc} */
        @Override protected Object clone() throws CloneNotSupportedException {
            return super.clone();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "Account [id=" + id + ", balance=$" + balance + ']';
        }
    }
}

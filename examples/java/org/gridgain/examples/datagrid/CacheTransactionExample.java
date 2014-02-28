// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.datagrid;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;

import java.io.*;
import java.util.*;

import static org.gridgain.grid.cache.GridCacheFlag.*;
import static org.gridgain.grid.cache.GridCacheTxConcurrency.*;
import static org.gridgain.grid.cache.GridCacheTxIsolation.*;

/**
 * Demonstrates how to use cache transactions.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ggstart.{sh|bat} examples/config/example-cache.xml'}.
 * <p>
 * Alternatively you can run {@link org.gridgain.examples.datagrid.CacheNodeStartup} in another JVM which will
 * start GridGain node with {@code examples/config/example-cache.xml} configuration.
 *
 * @author @java.author
 * @version @java.version
 */
public class CacheTransactionExample {
    /** Cache name. */
    private static final String CACHE_NAME = "partitioned_tx";
    //private static final String CACHE_NAME = "replicated";
    //private static final String CACHE_NAME = "local";

    /** Cache. */
    private static GridCache<UUID, Object> cache;

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws GridException If example execution failed.
     */
    public static void main(String[] args) throws GridException {
        try (Grid g = GridGain.start("examples/config/example-cache.xml")) {
            System.out.println();
            System.out.println(">>> Cache transaction example started.");

            GridCache<Long, Account> cache = g.cache(CACHE_NAME);

            // Initialize.
            cache.putx(1L, new Account(1, 100));
            cache.putx(2L, new Account(2, 500));

            System.out.println();
            System.out.println(">>> Accounts before transfer: ");
            System.out.println(">>> " + cache.get(1L));
            System.out.println(">>> " + cache.get(2L));

            // Transfer $200 from account2 to account1 within a transaction.
            transfer(2, 1, 200);

            System.out.println();
            System.out.println(">>> Accounts after transfer: ");
            System.out.println(">>> " + cache.get(1L));
            System.out.println(">>> " + cache.get(2L));

            System.out.println(">>> Cache transaction example finished.");
        }
    }

    /**
     * Transfer money from one account to another.
     *
     * @param fromId 'From' account ID.
     * @param toId 'To' account ID.
     * @param amount Amount to transfer.
     * @throws GridException If failed.
     */
    private static void transfer(long fromId, long toId, double amount) throws GridException {
        if (fromId == toId)
            return;

        // Clone every object we get from cache, so we can freely update it.
        GridCacheProjection<Long, Account> cache = GridGain.grid().<Long, Account>cache(CACHE_NAME).flagsOn(CLONE);

        try (GridCacheTx tx = cache.txStart(PESSIMISTIC, REPEATABLE_READ)) {
            Account fromAcct;
            Account toAcct;

            // In PESSIMISTIC mode cache objects are locked
            // automatically upon access within a transaction.
            // To avoid deadlocks, we always lock account with smaller ID first.
            if (fromId < toId) {
                fromAcct = cache.get(fromId); // Lock 'from' account first.
                toAcct = cache.get(toId);
            }
            else {
                toAcct = cache.get(toId); // Lock 'to' account first.
                fromAcct = cache.get(fromId);
            }

            // Debit 'from' account and credit 'to' account.
            fromAcct.update(-amount);
            toAcct.update(amount);

            // Store updated accounts in cache.
            cache.putx(fromId, fromAcct);
            cache.putx(toId, toAcct);

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
        private long id;

        /** Account balance. */
        private double balance;

        /**
         * @param id Account ID.
         * @param balance Balance.
         */
        Account(long id, double balance) {
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

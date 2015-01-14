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

package org.gridgain.examples.datagrid;

import org.apache.ignite.*;
import org.apache.ignite.transactions.*;
import org.gridgain.grid.cache.*;

import java.io.*;

import static org.gridgain.grid.cache.GridCacheFlag.*;
import static org.apache.ignite.transactions.IgniteTxConcurrency.*;
import static org.apache.ignite.transactions.IgniteTxIsolation.*;

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
     * @throws IgniteCheckedException If example execution failed.
     */
    public static void main(String[] args) throws IgniteCheckedException {
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
     * @throws IgniteCheckedException If failed.
     */
    private static void deposit(int acctId, double amount) throws IgniteCheckedException {
        // Clone every object we get from cache, so we can freely update it.
        GridCacheProjection<Integer, Account> cache = Ignition.ignite().<Integer, Account>cache(CACHE_NAME).flagsOn(CLONE);

        try (IgniteTx tx = cache.txStart(PESSIMISTIC, REPEATABLE_READ)) {
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

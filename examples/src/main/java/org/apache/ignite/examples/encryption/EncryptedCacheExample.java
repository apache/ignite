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

package org.apache.ignite.examples.encryption;

import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.CacheConfiguration;

/**
 * This example demonstrates the usage of Apache Ignite Persistent Store.
 * Data stored in persistence will be encrypted.
 */
public class EncryptedCacheExample {
    /** */
    public static void main(String[] args) {
        System.out.println(">>> Starting cluster.");

        // Starting Ignite with EncryptionSpi configured.
        // Please, note, you should have the same keystore on every server node in cluster with enabled encryption.
        // You can use encryption feature only for deployment with Ignite persistence enabled.
        try (Ignite ignite = Ignition.start("examples/config/encryption/example-encrypted-store.xml")) {
            // Activate the cluster. Required to do if the persistent store is enabled because you might need
            // to wait while all the nodes, that store a subset of data on disk, join the cluster.
            ignite.cluster().active(true);

            CacheConfiguration<Long, BankAccount> ccfg = new CacheConfiguration<>("encrypted-cache");

            // Enabling encryption for newly created cache.
            ccfg.setEncryptionEnabled(true);

            System.out.println(">>> Creating encrypted cache.");

            IgniteCache<Long, BankAccount> cache = ignite.createCache(ccfg);

            System.out.println(">>> Populating cache with data.");

            // Data in this cache will be encrypted on the disk.
            cache.put(1L, new BankAccount("Rich account", 1_000_000L));
            cache.put(2L, new BankAccount("Middle account", 1_000L));
            cache.put(3L, new BankAccount("One dollar account", 1L));
        }

        // After cluster shutdown data persisted on the disk in encrypted form.

        System.out.println(">>> Starting cluster again.");
        // Starting cluster again.
        try (Ignite ignite = Ignition.start("examples/config/encryption/example-encrypted-store.xml")) {
            ignite.cluster().active(true);

            // We can obtain existing cache and load data from disk.
            IgniteCache<Long, BankAccount> cache = ignite.getOrCreateCache("encrypted-cache");

            QueryCursor<Cache.Entry<Long, BankAccount>> cursor = cache.query(new ScanQuery<>());

            System.out.println(">>> Saved data:");

            // Iterating through existing data.
            for (Cache.Entry<Long, BankAccount> entry : cursor) {
                System.out.println(">>> ID = " + entry.getKey() +
                    ", AccountName = " + entry.getValue().accountName +
                    ", Balance = " + entry.getValue().balance);
            }

        }
    }

    /**
     * Test class with very secret data.
     */
    private static class BankAccount {
        /**
         * Name.
         */
        private String accountName;

        /**
         * Balance.
         */
        private long balance;

        /** */
        BankAccount(String accountName, long balance) {
            this.accountName = accountName;
            this.balance = balance;
        }
    }
}

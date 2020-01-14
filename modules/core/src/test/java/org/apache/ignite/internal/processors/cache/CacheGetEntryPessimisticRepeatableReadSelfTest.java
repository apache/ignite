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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Ignore;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT;
import static org.apache.ignite.cache.CacheMode.LOCAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Test getEntry and getEntries methods.
 */
public class CacheGetEntryPessimisticRepeatableReadSelfTest extends CacheGetEntryAbstractTest {
    /** {@inheritDoc} */
    @Override protected TransactionConcurrency concurrency() {
        return TransactionConcurrency.PESSIMISTIC;
    }

    /** {@inheritDoc} */
    @Override protected TransactionIsolation isolation() {
        return TransactionIsolation.REPEATABLE_READ;
    }

    /**
     * @throws Exception If failed.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-7187")
    @Test
    public void testNearTransactionalMvcc() throws Exception {
        CacheConfiguration cfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        cfg.setWriteSynchronizationMode(FULL_SYNC);
        cfg.setCacheMode(PARTITIONED);
        cfg.setAtomicityMode(TRANSACTIONAL_SNAPSHOT);
        cfg.setName("nearT");
        cfg.setNearConfiguration(new NearCacheConfiguration());

        test(cfg);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPartitionedTransactionalMvcc() throws Exception {
        CacheConfiguration cfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        cfg.setWriteSynchronizationMode(FULL_SYNC);
        cfg.setCacheMode(PARTITIONED);
        cfg.setAtomicityMode(TRANSACTIONAL_SNAPSHOT);
        cfg.setName("partitionedT");

        test(cfg);
    }

    /**
     * @throws Exception If failed.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-9530")
    @Test
    public void testLocalTransactionalMvcc() throws Exception {
        CacheConfiguration cfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        cfg.setWriteSynchronizationMode(FULL_SYNC);
        cfg.setCacheMode(LOCAL);
        cfg.setAtomicityMode(TRANSACTIONAL_SNAPSHOT);
        cfg.setName("localT");

        test(cfg);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testReplicatedTransactionalMvcc() throws Exception {
        CacheConfiguration cfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        cfg.setWriteSynchronizationMode(FULL_SYNC);
        cfg.setCacheMode(REPLICATED);
        cfg.setAtomicityMode(TRANSACTIONAL_SNAPSHOT);
        cfg.setName("replicatedT");

        test(cfg);
    }
}

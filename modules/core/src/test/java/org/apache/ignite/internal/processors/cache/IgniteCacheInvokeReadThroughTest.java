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

import org.apache.ignite.testframework.MvccFeatureChecker;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;

/**
 *
 */
public class IgniteCacheInvokeReadThroughTest extends IgniteCacheInvokeReadThroughAbstractTest {
    /** {@inheritDoc} */
    @Override public void setUp() throws Exception {
        MvccFeatureChecker.failIfNotSupported(MvccFeatureChecker.Feature.CACHE_STORE);

        super.setUp();
    }

    /** {@inheritDoc} */
    @Override protected void startNodes() throws Exception {
        startGridsMultiThreaded(4);

        client = true;

        startGrid(4);
    }

    /**
     * @throws Exception If failed.
     */
    public void testInvokeReadThroughAtomic0() throws Exception {
        invokeReadThrough(cacheConfiguration(PARTITIONED, ATOMIC, 0, false));
    }

    /**
     * @throws Exception If failed.
     */
    public void testInvokeReadThroughAtomic1() throws Exception {
        invokeReadThrough(cacheConfiguration(PARTITIONED, ATOMIC, 1, false));
    }

    /**
     * @throws Exception If failed.
     */
    public void testInvokeReadThroughAtomic2() throws Exception {
        invokeReadThrough(cacheConfiguration(PARTITIONED, ATOMIC, 2, false));
    }

    /**
     * @throws Exception If failed.
     */
    public void testInvokeReadThroughAtomicNearCache() throws Exception {
        invokeReadThrough(cacheConfiguration(PARTITIONED, ATOMIC, 1, true));
    }

    /**
     * @throws Exception If failed.
     */
    public void testInvokeReadThroughAtomicReplicated() throws Exception {
        invokeReadThrough(cacheConfiguration(REPLICATED, ATOMIC, 0, false));
    }

    /**
     * @throws Exception If failed.
     */
    public void testInvokeReadThroughTx0() throws Exception {
        invokeReadThrough(cacheConfiguration(PARTITIONED, TRANSACTIONAL, 0, false));
    }

    /**
     * @throws Exception If failed.
     */
    public void testInvokeReadThroughTx1() throws Exception {
        invokeReadThrough(cacheConfiguration(PARTITIONED, TRANSACTIONAL, 1, false));
    }

    /**
     * @throws Exception If failed.
     */
    public void testInvokeReadThroughTx2() throws Exception {
        invokeReadThrough(cacheConfiguration(PARTITIONED, TRANSACTIONAL, 2, false));
    }

    /**
     * @throws Exception If failed.
     */
    public void testInvokeReadThroughTxNearCache() throws Exception {
        invokeReadThrough(cacheConfiguration(PARTITIONED, TRANSACTIONAL, 1, true));
    }

    /**
     * @throws Exception If failed.
     */
    public void testInvokeReadThroughTxReplicated() throws Exception {
        invokeReadThrough(cacheConfiguration(REPLICATED, TRANSACTIONAL, 0, false));
    }

    /**
     * @throws Exception If failed.
     */
    public void testInvokeReadThroughMvccTx0() throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-8582");

        invokeReadThrough(cacheConfiguration(PARTITIONED, TRANSACTIONAL_SNAPSHOT, 0, false));
    }

    /**
     * @throws Exception If failed.
     */
    public void testInvokeReadThroughMvccTx1() throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-8582");

        invokeReadThrough(cacheConfiguration(PARTITIONED, TRANSACTIONAL_SNAPSHOT, 1, false));
    }

    /**
     * @throws Exception If failed.
     */
    public void testInvokeReadThroughMvccTx2() throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-8582");

        invokeReadThrough(cacheConfiguration(PARTITIONED, TRANSACTIONAL_SNAPSHOT, 2, false));
    }

    /**
     * @throws Exception If failed.
     */
    public void testInvokeReadThroughMvccTxNearCache() throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-8582");

        invokeReadThrough(cacheConfiguration(PARTITIONED, TRANSACTIONAL_SNAPSHOT, 1, true));
    }

    /**
     * @throws Exception If failed.
     */
    public void testInvokeReadThroughMvccTxReplicated() throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-8582");

        invokeReadThrough(cacheConfiguration(REPLICATED, TRANSACTIONAL_SNAPSHOT, 0, false));
    }
}

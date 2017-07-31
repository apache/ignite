/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.standbycluster.extended;

import junit.framework.TestSuite;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCachePartitionedNearDisabledFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCachePartitionedNearDisabledMultiNodeFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCachePartitionedNearDisabledMultiNodeP2PDisabledFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.CachePartitionedMultiNodeLongTxTimeoutFullApiTest;
import org.apache.ignite.internal.processors.cache.distributed.near.CachePartitionedNearEnabledMultiNodeLongTxTimeoutFullApiTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheNearOnlyMultiNodeFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheNearOnlyMultiNodeP2PDisabledFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedClientOnlyNoPrimaryFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedCopyOnReadDisabledMultiNodeFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedMultiNodeFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedMultiNodeP2PDisabledFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedNearOnlyNoPrimaryFullApiSelfTest;

/**
 *
 */
public class GridActivationPartitionedCacheSuit extends GridActivationCacheAbstractTestSuit {
    static {
        addTest(CachePartitionedMultiNodeLongTxTimeoutFullApiTest.class);
        addTest(CachePartitionedNearEnabledMultiNodeLongTxTimeoutFullApiTest.class);
        addTest(GridCacheNearOnlyMultiNodeFullApiSelfTest.class);
        addTest(GridCacheNearOnlyMultiNodeP2PDisabledFullApiSelfTest.class);
        addTest(GridCachePartitionedClientOnlyNoPrimaryFullApiSelfTest.class);
        addTest(GridCachePartitionedCopyOnReadDisabledMultiNodeFullApiSelfTest.class);
        addTest(GridCachePartitionedFullApiSelfTest.class);
        addTest(GridCachePartitionedMultiNodeFullApiSelfTest.class);
        addTest(GridCachePartitionedMultiNodeP2PDisabledFullApiSelfTest.class);
        addTest(GridCachePartitionedNearDisabledFullApiSelfTest.class);
        addTest(GridCachePartitionedNearDisabledMultiNodeFullApiSelfTest.class);
        addTest(GridCachePartitionedNearDisabledMultiNodeP2PDisabledFullApiSelfTest.class);
        addTest(GridCachePartitionedNearOnlyNoPrimaryFullApiSelfTest.class);

//        addTest(GridCachePartitionedMultiJvmFullApiSelfTest.class);
//        addTest(GridCachePartitionedCopyOnReadDisabledMultiJvmFullApiSelfTest.class);
//        addTest(GridCachePartitionedFairAffinityMultiJvmFullApiSelfTest.class);
//        addTest(GridCachePartitionedMultiJvmP2PDisabledFullApiSelfTest.class);
//        addTest(GridCachePartitionedNearDisabledAtomicOffHeapTieredMultiJvmFullApiSelfTest.class);
//        addTest(GridCachePartitionedNearDisabledFairAffinityMultiJvmFullApiSelfTest.class);
//        addTest(GridCachePartitionedNearDisabledMultiJvmFullApiSelfTest.class);
//        addTest(GridCachePartitionedNearDisabledMultiJvmP2PDisabledFullApiSelfTest.class);
//        addTest(GridCachePartitionedNearDisabledOffHeapMultiJvmFullApiSelfTest.class);
//        addTest(GridCachePartitionedNearDisabledOffHeapTieredMultiJvmFullApiSelfTest.class);
//        addTest(GridCachePartitionedOffHeapMultiJvmFullApiSelfTest.class);
//        addTest(GridCachePartitionedOffHeapTieredMultiJvmFullApiSelfTest.class);
    }

    /**
     *
     */
    public static TestSuite suite() {
        TestSuite suite = buildSuite();

        suite.setName("Activation Stand-by Cluster After Primary Cluster Stopped Check Partitioned Cache");

        return suite;
    }
}

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
import org.apache.ignite.internal.processors.cache.distributed.replicated.CacheReplicatedRendezvousAffinityExcludeNeighborsMultiNodeFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.CacheReplicatedRendezvousAffinityMultiNodeFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.GridCacheReplicatedAtomicFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.GridCacheReplicatedAtomicMultiNodeFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.GridCacheReplicatedFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.GridCacheReplicatedMultiNodeFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.GridCacheReplicatedMultiNodeP2PDisabledFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.GridCacheReplicatedNearOnlyMultiNodeFullApiSelfTest;

/**
 *
 */
public class GridActivationReplicatedCacheSuit extends GridActivationCacheAbstractTestSuit {
    static {
        addTest(CacheReplicatedRendezvousAffinityExcludeNeighborsMultiNodeFullApiSelfTest.class);
        addTest(CacheReplicatedRendezvousAffinityMultiNodeFullApiSelfTest.class);
        addTest(GridCacheReplicatedAtomicFullApiSelfTest.class);
        addTest(GridCacheReplicatedFullApiSelfTest.class);
        addTest(GridCacheReplicatedMultiNodeFullApiSelfTest.class);
        addTest(GridCacheReplicatedAtomicMultiNodeFullApiSelfTest.class);
        addTest(GridCacheReplicatedMultiNodeP2PDisabledFullApiSelfTest.class);
        addTest(GridCacheReplicatedNearOnlyMultiNodeFullApiSelfTest.class);

//        tests.add(transform(GridCacheReplicatedAtomicMultiJvmFullApiSelfTest.class));
//        tests.add(transform(GridCacheReplicatedAtomicPrimaryWriteOrderMultiJvmFullApiSelfTest.class));
//        tests.add(transform(GridCacheReplicatedMultiJvmFullApiSelfTest.class));
//        tests.add(transform(GridCacheReplicatedMultiJvmP2PDisabledFullApiSelfTest.class));
//        tests.add(transform(GridCacheReplicatedNearOnlyMultiJvmFullApiSelfTest.class));
//        tests.add(transform(GridCacheReplicatedOffHeapMultiJvmFullApiSelfTest.class));
//        tests.add(transform(GridCacheReplicatedOffHeapTieredMultiJvmFullApiSelfTest.class));
    }

    /**
     *
     */
    public static TestSuite suite() {
        TestSuite suite = buildSuite();

        suite.setName("Activation Stand-by Cluster After Primary Cluster Stopped  Check Replicated Cache");

        return suite;
    }
}

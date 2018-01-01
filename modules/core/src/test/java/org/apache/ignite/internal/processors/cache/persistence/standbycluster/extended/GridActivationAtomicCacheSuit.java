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
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheAtomicFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheAtomicNearEnabledFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheAtomicClientOnlyMultiNodeFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheAtomicClientOnlyMultiNodeP2PDisabledFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheAtomicCopyOnReadDisabledMultiNodeFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheAtomicMultiNodeFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheAtomicMultiNodeP2PDisabledFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheAtomicNearEnabledMultiNodeFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheAtomicNearOnlyMultiNodeFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheAtomicNearOnlyMultiNodeP2PDisabledFullApiSelfTest;

/**
 *
 */
public class GridActivationAtomicCacheSuit extends GridActivationCacheAbstractTestSuit {
    static {
        addTest(GridCacheAtomicClientOnlyMultiNodeFullApiSelfTest.class);
        addTest(GridCacheAtomicClientOnlyMultiNodeP2PDisabledFullApiSelfTest.class);
        addTest(GridCacheAtomicCopyOnReadDisabledMultiNodeFullApiSelfTest.class);
        addTest(GridCacheAtomicFullApiSelfTest.class);
        addTest(GridCacheAtomicMultiNodeFullApiSelfTest.class);
        addTest(GridCacheAtomicMultiNodeP2PDisabledFullApiSelfTest.class);
        addTest(GridCacheAtomicNearEnabledFullApiSelfTest.class);
        addTest(GridCacheAtomicNearEnabledMultiNodeFullApiSelfTest.class);
        addTest(GridCacheAtomicNearOnlyMultiNodeFullApiSelfTest.class);
        addTest(GridCacheAtomicNearOnlyMultiNodeP2PDisabledFullApiSelfTest.class);

//        addTest(GridCacheAtomicClientOnlyFairAffinityMultiJvmFullApiSelfTest.class));
//        addTest(GridCacheAtomicClientOnlyMultiJvmFullApiSelfTest.class));
//        addTest(GridCacheAtomicClientOnlyMultiJvmP2PDisabledFullApiSelfTest.class));
//        addTest(GridCacheAtomicCopyOnReadDisabledMultiJvmFullApiSelfTest.class));
//        addTest(GridCacheAtomicFairAffinityMultiJvmFullApiSelfTest.class));
//        addTest(GridCacheAtomicMultiJvmFullApiSelfTest.class));
//        addTest(GridCacheAtomicMultiJvmP2PDisabledFullApiSelfTest.class));
//        addTest(GridCacheAtomicOffHeapMultiJvmFullApiSelfTest.class));
//        addTest(GridCacheAtomicOffHeapTieredMultiJvmFullApiSelfTest.class));
//        addTest(GridCacheAtomicPrimaryWriteOrderFairAffinityMultiJvmFullApiSelfTest.class));
//        addTest(GridCacheAtomicPrimaryWriteOrderMultiJvmFullApiSelfTest.class));
//        addTest(GridCacheAtomicPrimaryWriteOrderMultiJvmP2PDisabledFullApiSelfTest.class));
//        addTest(GridCacheAtomicPrimaryWrityOrderOffHeapMultiJvmFullApiSelfTest.class));
//        addTest(GridCacheAtomicPrimaryWrityOrderOffHeapTieredMultiJvmFullApiSelfTest.class));
//        addTest(GridCacheAtomicNearEnabledFairAffinityMultiJvmFullApiSelfTest.class));
//        addTest(GridCacheAtomicNearEnabledMultiJvmFullApiSelfTest.class));
//        addTest(GridCacheAtomicNearEnabledPrimaryWriteOrderMultiJvmFullApiSelfTest.class));
//        addTest(GridCacheAtomicNearOnlyMultiJvmFullApiSelfTest.class));
//        addTest(GridCacheAtomicNearOnlyMultiJvmP2PDisabledFullApiSelfTest.class));
    }

    /**
     *
     */
    public static TestSuite suite() {
        TestSuite suite = buildSuite();

        suite.setName("Activation Stand-by Cluster After Primary Cluster Stopped Check Atomic Cache");

        return suite;
    }
}

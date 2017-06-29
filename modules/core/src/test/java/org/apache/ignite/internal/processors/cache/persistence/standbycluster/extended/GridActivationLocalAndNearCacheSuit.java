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
import org.apache.ignite.internal.processors.cache.local.GridCacheLocalAtomicFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.local.GridCacheLocalFullApiSelfTest;

/**
 *
 */
public class GridActivationLocalAndNearCacheSuit extends GridActivationCacheAbstractTestSuit {
    static {
        addTest(GridCacheLocalAtomicFullApiSelfTest.class);
        addTest(GridCacheLocalFullApiSelfTest.class);

//        addTest(GridCacheNearOnlyFairAffinityMultiJvmFullApiSelfTest.class);
//        addTest(GridCacheNearOnlyMultiJvmFullApiSelfTest.class);
//        addTest(GridCacheNearOnlyMultiJvmP2PDisabledFullApiSelfTest.class);
    }

    /**
     *
     */
    public static TestSuite suite() {
        TestSuite suite = buildSuite();

        suite.setName("Activation Stand-by Cluster After Primary Cluster Stopped Check Local and Near Cache");

        return suite;
    }
}

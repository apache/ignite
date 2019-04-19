/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.hadoop.impl;

import org.apache.ignite.Ignite;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.IgniteTxConfigCacheSelfTest;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Test checks whether hadoop system cache doesn't use user defined TX config.
 */
@RunWith(JUnit4.class)
public class HadoopTxConfigCacheTest extends IgniteTxConfigCacheSelfTest {
    /**
     * Success if system caches weren't timed out.
     *
     * @throws Exception If failed.
     */
    @Test
    @Override public void testSystemCacheTx() throws Exception {
        final Ignite ignite = grid(0);

        final IgniteInternalCache<Object, Object> hadoopCache = getSystemCache(ignite, CU.SYS_CACHE_HADOOP_MR);

        checkImplicitTxSuccess(hadoopCache);
        checkStartTxSuccess(hadoopCache);
    }
}

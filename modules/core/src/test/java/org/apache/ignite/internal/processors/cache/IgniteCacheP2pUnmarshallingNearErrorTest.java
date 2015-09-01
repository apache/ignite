/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.cache.eviction.fifo.FifoEvictionPolicy;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;

/**
 * Checks behavior on exception while unmarshalling key.
 */
public class IgniteCacheP2pUnmarshallingNearErrorTest extends IgniteCacheP2pUnmarshallingErrorTest {
    /** {@inheritDoc} */
    @Override protected NearCacheConfiguration nearConfiguration() {
        return new NearCacheConfiguration();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        if (cfg.isClientMode() == null || !cfg.isClientMode()) {
            cfg.getCacheConfiguration()[0].setEvictMaxOverflowRatio(0);
            cfg.getCacheConfiguration()[0].setEvictSynchronized(true);
            cfg.getCacheConfiguration()[0].setEvictSynchronizedKeyBufferSize(1);
            cfg.getCacheConfiguration()[0].setEvictionPolicy(new FifoEvictionPolicy(1));
        }

        return cfg;
    }

    /** {@inheritDoc} */
    @Override public void testResponseMessageOnUnmarshallingFailed() throws InterruptedException {
        //GridCacheEvictionRequest unmarshalling failed test
        readCnt.set(5); //2 for each put

        jcache(0).put(new TestKey(String.valueOf(++key)), "");
        jcache(0).put(new TestKey(String.valueOf(++key)), "");

        //Eviction request unmarshalling failed but ioManager does not hangs up.

        // Wait for eviction complete.
        Thread.sleep(1000);
    }
}
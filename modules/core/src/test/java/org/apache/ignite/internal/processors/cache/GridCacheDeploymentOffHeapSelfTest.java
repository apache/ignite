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

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMemoryMode.OFFHEAP_VALUES;

/**
 * Tests deployment with off-heap storage.
 */
public class GridCacheDeploymentOffHeapSelfTest extends GridCacheDeploymentSelfTest {
    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration() throws Exception {
        CacheConfiguration cacheCfg = super.cacheConfiguration();

        cacheCfg.setMemoryMode(OFFHEAP_VALUES);
        cacheCfg.setOffHeapMaxMemory(0);
        cacheCfg.setAtomicityMode(TRANSACTIONAL);
        cacheCfg.setNearConfiguration(new NearCacheConfiguration());

        return cacheCfg;
    }
}
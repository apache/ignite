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

package org.apache.ignite.internal.processors.cache.persistence;

import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;

/**
 * Adding shared group and indexes to testing. It would impact how we evict partitions.
 */
public class IgnitePdsContinuousRestartTestWithSharedGroupAndIndexes extends IgnitePdsContinuousRestartTest {
    /** Cache 2 singleton group name. */
    public static final String CACHE_GROUP_NAME = "Group2";

    /**
     * Default constructor.
     */
    public IgnitePdsContinuousRestartTestWithSharedGroupAndIndexes() {
        super(false);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration ccfg2 = new CacheConfiguration();

        ccfg2.setName(CACHE_NAME);
        ccfg2.setGroupName(CACHE_GROUP_NAME);
        ccfg2.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        ccfg2.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        ccfg2.setAffinity(new RendezvousAffinityFunction(false, 32));
        ccfg2.setIndexedTypes(Integer.class, Integer.class);
        ccfg2.setBackups(2);

        cfg.setCacheConfiguration(ccfg2);

        return cfg;
    }
}

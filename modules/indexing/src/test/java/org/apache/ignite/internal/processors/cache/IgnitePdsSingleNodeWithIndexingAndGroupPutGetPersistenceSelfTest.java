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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;

/**
 *
 */
public class IgnitePdsSingleNodeWithIndexingAndGroupPutGetPersistenceSelfTest
    extends IgnitePdsSingleNodeWithIndexingPutGetPersistenceTest {
    /** {@inheritDoc} */
    @Override protected void configure(IgniteConfiguration cfg) {
        super.configure(cfg);

        for (CacheConfiguration ccfg : cfg.getCacheConfiguration()) {
            AffinityFunction aff = ccfg.getAffinity();

            int parts = aff != null ? aff.partitions() : RendezvousAffinityFunction.DFLT_PARTITION_COUNT;

            ccfg.setGroupName("testGroup-parts" + parts);
        }
    }
}

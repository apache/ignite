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

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

package org.apache.ignite.internal.processors.cache.index;

import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.index.IndexingTestUtils.SlowdownBuildIndexConsumer;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_INDEX_REBUILD_BATCH_SIZE;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;

public class ResumeCreateIndexTest extends AbstractRebuildIndexTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setCacheConfiguration(
                cacheCfg(DEFAULT_CACHE_NAME, null).setAffinity(new RendezvousAffinityFunction(false, 1))
            );
    }

    @Test
    @WithSystemProperty(key = IGNITE_INDEX_REBUILD_BATCH_SIZE, value = "1")
    public void test0() throws Exception {
        IgniteH2IndexingEx.prepareBeforeNodeStart();

        IgniteEx n = startGrid(0);
        n.cluster().state(ACTIVE);

        populate(n.cache(DEFAULT_CACHE_NAME), 100_000);

        String idxName = "IDX0";
        SlowdownBuildIndexConsumer slowdownIdxCreateConsumer = addSlowdownIdxCreateConsumer(n, idxName, 10);

        // TODO: 07.06.2021 continue
    }
}

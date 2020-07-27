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

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.PartitionLossPolicy;
import org.apache.ignite.cluster.ClusterTopologyException;
import org.apache.ignite.internal.util.typedef.G;

/**
 *
 */
public class IgniteCachePartitionMapUpdateSafeLossPolicyTest extends IgniteCachePartitionMapUpdateTest {
    /** {@inheritDoc} */
    @Override protected PartitionLossPolicy policy() {
        return PartitionLossPolicy.READ_WRITE_SAFE;
    }

    /** Lost partitions counter. */
    private AtomicInteger lostCnt = new AtomicInteger();

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        lostCnt.set(0);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        assertTrue(lostCnt.get() > 0);
    }

    /** {@inheritDoc} */
    @Override protected void stopGrid(int idx) {
        List<Ignite> grids0 = G.allGrids();

        super.stopGrid(idx);

        if (grids0.size() == 1)
            return;

        Stream.of(CACHE1, CACHE2).forEach(new Consumer<String>() {
            @Override public void accept(String cache) {
                Ignite testGrid = G.allGrids().get(0);

                lostCnt.addAndGet(testGrid.cache(cache).lostPartitions().size());

                try {
                    testGrid.resetLostPartitions(Collections.singleton(cache));
                } catch (ClusterTopologyException ignored) {
                    // No-op.
                }
            }
        });
    }
}

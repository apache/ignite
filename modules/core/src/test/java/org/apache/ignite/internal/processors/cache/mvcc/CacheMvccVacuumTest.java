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

package org.apache.ignite.internal.processors.cache.mvcc;

import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.testframework.GridTestUtils;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 * Vacuum test.
 */
public class CacheMvccVacuumTest extends CacheMvccAbstractTest {
    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return PARTITIONED;
    }

    /**
     * @throws Exception If failed.
     */
    public void testStartStopVacuumInMemory() throws Exception {
        Ignite node0 = startGrid(0);
        Ignite node1 = startGrid(1);

        node1.createCache(new CacheConfiguration<>("test1")
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL));

        ensureNoVacuum(node0);
        ensureNoVacuum(node1);

        node1.createCache(new CacheConfiguration<>("test2")
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT));

        ensureVacuum(node0);
        ensureVacuum(node1);

        stopGrid(0);

        ensureNoVacuum(node0);
        ensureVacuum(node1);

        stopGrid(1);

        ensureNoVacuum(node0);
        ensureNoVacuum(node1);
    }

    /**
     * @throws Exception If failed.
     */
    public void testStartStopVacuumPersistence() throws Exception {
        persistence = true;

        Ignite node0 = startGrid(0);
        Ignite node1 = startGrid(1);

        ensureNoVacuum(node0);
        ensureNoVacuum(node1);

        node1.cluster().active(true);

        ensureNoVacuum(node0);
        ensureNoVacuum(node1);

        node1.createCache(new CacheConfiguration<>("test2")
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT));

        ensureVacuum(node0);
        ensureVacuum(node1);

        node1.cluster().active(false);

        ensureNoVacuum(node0);
        ensureNoVacuum(node1);

        node1.cluster().active(true);

        ensureVacuum(node0);
        ensureVacuum(node1);

        stopGrid(0);

        ensureNoVacuum(node0);
        ensureVacuum(node1);

        stopGrid(1);

        ensureNoVacuum(node0);
        ensureNoVacuum(node1);

        node0 = startGrid(0);
        node1 = startGrid(1);

        ensureNoVacuum(node0);
        ensureNoVacuum(node1);

        node1.cluster().active(true);

        ensureVacuum(node0);
        ensureVacuum(node1);
    }

    /**
     * @throws Exception If failed.
     */
    public void testVacuumNotStartedWithoutMvcc() throws Exception {
        IgniteConfiguration cfg = getConfiguration("grid1");

        Ignite node = startGrid(cfg);

        ensureNoVacuum(node);
    }

    /**
     * @throws Exception If failed.
     */
    public void testVacuumNotStartedWithoutMvccPersistence() throws Exception {
        persistence = true;

        IgniteConfiguration cfg = getConfiguration("grid1");

        Ignite node = startGrid(cfg);

        ensureNoVacuum(node);

        node.cluster().active(true);

        ensureNoVacuum(node);
    }

    /**
     * Ensures vacuum is running on the given node.
     *
     * @param node Node.
     */
    private void ensureVacuum(Ignite node) {
        MvccProcessorImpl crd = mvccProcessor(node);

        assertNotNull(crd);

        List<GridWorker> vacuumWorkers = GridTestUtils.getFieldValue(crd, "vacuumWorkers");

        assertNotNull(vacuumWorkers);
        assertFalse(vacuumWorkers.isEmpty());

        for (GridWorker w : vacuumWorkers) {
            assertFalse(w.isCancelled());
            assertFalse(w.isDone());
        }
    }

    /**
     * Ensures vacuum is stopped on the given node.
     *
     * @param node Node.
     */
    private void ensureNoVacuum(Ignite node) {
        MvccProcessorImpl crd = mvccProcessor(node);

        assertNull(GridTestUtils.<List<GridWorker>>getFieldValue(crd, "vacuumWorkers"));
    }
}

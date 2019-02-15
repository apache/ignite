/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.loadtests;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskSplitAdapter;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.resources.IgniteInstanceResource;

/**
 *
 */
public class GridCacheLoadPopulationTask extends ComputeTaskSplitAdapter<Void, Void> {
    /** Serial version UID. */
    private static final long serialVersionUID = 1L;

    /** {@inheritDoc} */
    @Override public Void reduce(List<ComputeJobResult> results) {
        return null;
    }

    /** {@inheritDoc} */
    @Override protected Collection<? extends ComputeJob> split(int gridSize, Void arg) {
        Collection<ChunkPopulationJob> jobs = new ArrayList<>();

        int maxElements = 10000;
        int currStartElement = 0;

        while (currStartElement < GridCacheMultiNodeLoadTest.ELEMENTS_COUNT) {
            jobs.add(new ChunkPopulationJob(currStartElement, maxElements));

            currStartElement += maxElements;
        }

        return jobs;
    }

    /**
     * Chunk population job.
     */
    private static class ChunkPopulationJob implements ComputeJob {
        /** Serial version UID. */
        private static final long serialVersionUID = 1L;

        /** Start element index. */
        private int startElementIdx;

        /** Mex elements. */
        private int maxElements;

        /** Injected grid. */
        @IgniteInstanceResource
        private Ignite g;

        /**
         * Creates chunk population job.
         *
         * @param startElementIdx Start element index.
         * @param maxElements Max elements.
         */
        ChunkPopulationJob(int startElementIdx, int maxElements) {
            this.startElementIdx = startElementIdx;
            this.maxElements = maxElements;
        }

        /** {@inheritDoc} */
        @SuppressWarnings({"unchecked", "ConstantConditions"})
        @Override public Object execute() {
            Map<Object, TestValue> map = new TreeMap<>();

            for (int i = startElementIdx; i < startElementIdx + maxElements; i++) {
                if (i >= GridCacheMultiNodeLoadTest.ELEMENTS_COUNT)
                    break;

                Object key = UUID.randomUUID();

                map.put(key, new TestValue(key, i));
            }

            g.log().info("Putting values to partitioned cache [nodeId=" + g.cluster().localNode().id() + ", mapSize=" +
                map.size() + ']');

            g.cache(GridCacheMultiNodeLoadTest.CACHE_NAME).putAll(map);

            return null;
        }

        /** {@inheritDoc} */
        @Override public void cancel() {
            // No-op.
        }
    }
}

/**
 * Test value.
 */
@SuppressWarnings("ClassNameDiffersFromFileName")
class TestValue {
    /** Value key. */
    private Object key;

    /** Value data. */
    private String someData;

    /**
     * Constructs test value.
     *
     * @param key Key.
     * @param id Data.
     */
    TestValue(Object key, Object id) {
        this.key = key;
        someData = key + "_" + id + "_" + System.currentTimeMillis();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TestValue.class, this);
    }

    /**
     * @return Key.
     */
    public Object key() {
        return key;
    }

    /**
     * @return Value data.
     */
    public String someData() {
        return someData;
    }
}
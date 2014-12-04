/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.loadtests;

import org.apache.ignite.*;
import org.apache.ignite.compute.*;
import org.gridgain.grid.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.util.*;

/**
 *
 */
public class GridCacheLoadPopulationTask extends GridComputeTaskSplitAdapter<Void, Void> {
    /** Serial version UID. */
    private static final long serialVersionUID = 1L;

    /** {@inheritDoc} */
    @Override public Void reduce(List<GridComputeJobResult> results) throws GridException {
        return null;
    }

    /** {@inheritDoc} */
    @Override protected Collection<? extends GridComputeJob> split(int gridSize, Void arg) throws GridException {
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
    private static class ChunkPopulationJob implements GridComputeJob {
        /** Serial version UID. */
        private static final long serialVersionUID = 1L;

        /** Start element index. */
        private int startElementIdx;

        /** Mex elements. */
        private int maxElements;

        /** Injected grid. */
        @GridInstanceResource
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
        @Override public Object execute() throws GridException {
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
    public String toString() {
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

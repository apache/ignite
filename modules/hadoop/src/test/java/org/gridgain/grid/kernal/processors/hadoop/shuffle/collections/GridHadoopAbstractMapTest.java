/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.shuffle.collections;

import org.apache.commons.collections.comparators.*;
import org.apache.hadoop.io.*;
import org.gridgain.grid.*;
import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.kernal.processors.hadoop.v2.*;
import org.gridgain.grid.logger.*;
import org.gridgain.testframework.junits.common.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Abstract class for maps test.
 */
public abstract class GridHadoopAbstractMapTest extends GridCommonAbstractTest {
    /**
     * Test task context.
     */
    protected static class TaskContext extends GridHadoopTaskContext {
        /**
         */
        protected TaskContext() {
            super(null, null);
        }

        /** {@inheritDoc} */
        @Override public GridHadoopPartitioner partitioner() throws GridException {
            assert false;

            return null;
        }

        /** {@inheritDoc} */
        @Override public GridHadoopSerialization keySerialization() throws GridException {
            return new GridHadoopWritableSerialization(IntWritable.class);
        }

        /** {@inheritDoc} */
        @Override public GridHadoopSerialization valueSerialization() throws GridException {
            return new GridHadoopWritableSerialization(IntWritable.class);
        }

        /** {@inheritDoc} */
        @Override public Comparator<Object> sortComparator() {
            return ComparableComparator.getInstance();
        }

        /** {@inheritDoc} */
        @Override public Comparator<Object> groupComparator() {
            return ComparableComparator.getInstance();
        }

        /** {@inheritDoc} */
        @Override public void run() throws GridException {
            assert false;
        }

        /** {@inheritDoc} */
        @Override public void cancel() {
            assert false;
        }

        /** {@inheritDoc} */
        @Override public void prepareTaskEnvironment() throws GridException {
            assert false;
        }

        @Override public void cleanupTaskEnvironment() throws GridException {
            assert false;
        }
    }

    /**
     * Test job info.
     */
    protected static class JobInfo implements GridHadoopJobInfo {
        /** {@inheritDoc} */
        @Nullable @Override public String property(String name) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public boolean hasCombiner() {
            assert false;

            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean hasReducer() {
            assert false;

            return false;
        }

        /** {@inheritDoc} */
        @Override public GridHadoopJob createJob(GridHadoopJobId jobId, GridLogger log) throws GridException {
            assert false;

            return null;
        }

        /** {@inheritDoc} */
        @Override public int reducers() {
            assert false;

            return 0;
        }

        /** {@inheritDoc} */
        @Override public String jobName() {
            assert false;

            return null;
        }

        /** {@inheritDoc} */
        @Override public String user() {
            assert false;

            return null;
        }
    }
}

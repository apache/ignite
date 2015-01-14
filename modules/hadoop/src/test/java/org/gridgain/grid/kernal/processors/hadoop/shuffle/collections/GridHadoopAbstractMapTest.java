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

package org.gridgain.grid.kernal.processors.hadoop.shuffle.collections;

import org.apache.commons.collections.comparators.*;
import org.apache.hadoop.io.*;
import org.apache.ignite.*;
import org.gridgain.grid.*;
import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.kernal.processors.hadoop.v2.*;
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
        @Override public <T extends GridHadoopCounter> T counter(String grp, String name, Class<T> cls) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public GridHadoopCounters counters() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public GridHadoopPartitioner partitioner() throws IgniteCheckedException {
            assert false;

            return null;
        }

        /** {@inheritDoc} */
        @Override public GridHadoopSerialization keySerialization() throws IgniteCheckedException {
            return new GridHadoopWritableSerialization(IntWritable.class);
        }

        /** {@inheritDoc} */
        @Override public GridHadoopSerialization valueSerialization() throws IgniteCheckedException {
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
        @Override public void run() throws IgniteCheckedException {
            assert false;
        }

        /** {@inheritDoc} */
        @Override public void cancel() {
            assert false;
        }

        /** {@inheritDoc} */
        @Override public void prepareTaskEnvironment() throws IgniteCheckedException {
            assert false;
        }

        @Override public void cleanupTaskEnvironment() throws IgniteCheckedException {
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
        @Override public GridHadoopJob createJob(GridHadoopJobId jobId, IgniteLogger log) throws IgniteCheckedException {
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

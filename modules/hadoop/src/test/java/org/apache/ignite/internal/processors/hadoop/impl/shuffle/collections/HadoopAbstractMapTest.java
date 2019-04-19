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

package org.apache.ignite.internal.processors.hadoop.impl.shuffle.collections;

import java.util.Comparator;
import java.util.concurrent.Callable;
import org.apache.commons.collections.comparators.ComparableComparator;
import org.apache.hadoop.io.IntWritable;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.hadoop.HadoopHelper;
import org.apache.ignite.internal.processors.hadoop.HadoopJobEx;
import org.apache.ignite.internal.processors.hadoop.HadoopJobId;
import org.apache.ignite.internal.processors.hadoop.HadoopJobInfo;
import org.apache.ignite.internal.processors.hadoop.HadoopPartitioner;
import org.apache.ignite.internal.processors.hadoop.HadoopSerialization;
import org.apache.ignite.internal.processors.hadoop.HadoopTaskContext;
import org.apache.ignite.internal.processors.hadoop.counter.HadoopCounter;
import org.apache.ignite.internal.processors.hadoop.counter.HadoopCounters;
import org.apache.ignite.internal.processors.hadoop.impl.v2.HadoopWritableSerialization;
import org.apache.ignite.internal.processors.hadoop.io.PartiallyOffheapRawComparatorEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;

/**
 * Abstract class for maps test.
 */
public abstract class HadoopAbstractMapTest extends GridCommonAbstractTest {
    /**
     * Test task context.
     */
    protected static class TaskContext extends HadoopTaskContext {
        /**
         */
        protected TaskContext() {
            super(null, null);
        }

        /** {@inheritDoc} */
        @Override public <T extends HadoopCounter> T counter(String grp, String name, Class<T> cls) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public HadoopCounters counters() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public HadoopPartitioner partitioner() throws IgniteCheckedException {
            assert false;

            return null;
        }

        /** {@inheritDoc} */
        @Override public HadoopSerialization keySerialization() throws IgniteCheckedException {
            return new HadoopWritableSerialization(IntWritable.class);
        }

        /** {@inheritDoc} */
        @Override public HadoopSerialization valueSerialization() throws IgniteCheckedException {
            return new HadoopWritableSerialization(IntWritable.class);
        }

        /** {@inheritDoc} */
        @SuppressWarnings("unchecked")
        @Override public Comparator<Object> sortComparator() {
            return ComparableComparator.getInstance();
        }

        /** {@inheritDoc} */
        @Override public PartiallyOffheapRawComparatorEx<Object> partialRawSortComparator() {
            return null;
        }

        /** {@inheritDoc} */
        @SuppressWarnings("unchecked")
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

        /** {@inheritDoc} */
        @Override public void cleanupTaskEnvironment() throws IgniteCheckedException {
            assert false;
        }

        /** {@inheritDoc} */
        @Override public <T> T runAsJobOwner(Callable<T> c) throws IgniteCheckedException {
            try {
                return c.call();
            }
            catch (Exception e) {
                throw new IgniteCheckedException(e);
            }
        }
    }

    /**
     * Test job info.
     */
    protected static class JobInfo implements HadoopJobInfo {
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
        @Override public HadoopJobEx createJob(Class<? extends HadoopJobEx> jobCls, HadoopJobId jobId, IgniteLogger log,
            @Nullable String[] libNames, HadoopHelper helper) throws IgniteCheckedException {
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

        @Override public byte[] credentials() {
            assert false;

            return null;
        }
    }
}
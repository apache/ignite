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

package org.gridgain.grid.kernal;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.resources.*;
import org.apache.ignite.testframework.junits.common.*;

import java.io.*;
import java.util.*;

/**
 * {@link org.apache.ignite.compute.ComputeTaskContinuousMapper} test.
 */
@GridCommonTest(group = "Kernal Self")
public class GridTaskContinuousMapperSelfTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If test failed.
     */
    public void testContinuousMapperMethods() throws Exception {
        try {
            Ignite ignite = startGrid(0);
            startGrid(1);

            ignite.compute().execute(TestAllMethodsTask.class, null);
        }
        finally {
            stopGrid(0);
            stopGrid(1);
        }
    }

    /**
     * @throws Exception If test failed.
     */
    public void testContinuousMapperLifeCycle() throws Exception {
        try {
            Ignite ignite = startGrid(0);

            ignite.compute().execute(TestLifeCycleTask.class, null);
        }
        finally {
            stopGrid(0);
        }
    }

    /**
     * @throws Exception If test failed.
     */
    public void testContinuousMapperNegative() throws Exception {
        try {
            Ignite ignite = startGrid(0);

            ignite.compute().execute(TestNegativeTask.class, null);
        }
        finally {
            stopGrid(0);
        }
    }

    /** */
    @SuppressWarnings({"PublicInnerClass"})
    public static class TestAllMethodsTask extends ComputeTaskAdapter<Object, Object> {
        /** */
        @SuppressWarnings({"UnusedDeclaration"})
        @IgniteTaskContinuousMapperResource
        private ComputeTaskContinuousMapper mapper;

        /** */
        private int cnt;

        /** {@inheritDoc} */
        @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, Object arg) throws IgniteCheckedException {
            assert subgrid.size() == 2;

            mapper.send(new TestJob(cnt++), subgrid.get(0));

            Map<ComputeJob, ClusterNode> mappedJobs = new HashMap<>(2);

            mappedJobs.put(new TestJob(cnt++), subgrid.get(0));
            mappedJobs.put(new TestJob(cnt++), subgrid.get(1));

            mapper.send(mappedJobs);

            mapper.send(new TestJob(cnt++));

            int size = subgrid.size();

            Collection<ComputeJob> jobs = new ArrayList<>(size);

            for (ClusterNode n : subgrid)
                jobs.add(new TestJob(cnt++));

            mapper.send(jobs);

            return null;
        }

        /** {@inheritDoc} */
        @Override public Object reduce(List<ComputeJobResult> results) throws IgniteCheckedException {
            assert results.size() == cnt : "Unexpected result count: " + results.size();

            return null;
        }
    }

    /** */
    @SuppressWarnings({"PublicInnerClass"})
    public static class TestLifeCycleTask extends ComputeTaskAdapter<Object, Object> {
        /** */
        @IgniteLoggerResource
        private IgniteLogger log;

        /** */
        private ComputeTaskContinuousMapper mapper;

        /**
         * @param mapper Continuous mapper.
         * @throws IgniteCheckedException Thrown if any exception occurs.
         */
        @SuppressWarnings("unused")
        @IgniteTaskContinuousMapperResource
        private void setMapper(ComputeTaskContinuousMapper mapper) throws IgniteCheckedException {
            this.mapper = mapper;

            mapper.send(new TestJob());
        }

        /** {@inheritDoc} */
        @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, Object arg) throws IgniteCheckedException {
            mapper.send(new TestJob());

            return null;
        }

        /** {@inheritDoc} */
        @Override public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> received) throws IgniteCheckedException {
            ComputeJobResultPolicy plc = super.result(res, received);

            if (received != null && received.size() == 2)
                mapper.send(new TestJob());

            return plc;
        }

        /** {@inheritDoc} */
        @Override public Object reduce(List<ComputeJobResult> results) throws IgniteCheckedException {
            assert results.size() == 3 : "Unexpected result count: " + results.size();

            ClusterNode node = results.get(0).getNode();

            try {
                mapper.send(new TestJob(), node);

                assert false;
            }
            catch (IgniteCheckedException e) {
                if (log.isInfoEnabled())
                    log.info("Expected exception: " + e);
            }

            try {
                mapper.send(Collections.singletonMap(new TestJob(), node));

                assert false;
            }
            catch (IgniteCheckedException e) {
                if (log.isInfoEnabled())
                    log.info("Expected exception: " + e);
            }

            try {
                mapper.send(new TestJob());

                assert false;
            }
            catch (IgniteCheckedException e) {
                if (log.isInfoEnabled())
                    log.info("Expected exception: " + e);
            }

            try {
                mapper.send(Collections.singleton(new TestJob()));

                assert false;
            }
            catch (IgniteCheckedException e) {
                if (log.isInfoEnabled())
                    log.info("Expected exception: " + e);
            }

            return null;
        }
    }

    /** */
    @SuppressWarnings({"PublicInnerClass"})
    public static class TestNegativeTask extends ComputeTaskAdapter<Object, Object> {
        /** */
        @SuppressWarnings({"UnusedDeclaration"})
        @IgniteTaskContinuousMapperResource
        private ComputeTaskContinuousMapper mapper;

        /** */
        @IgniteLoggerResource
        private IgniteLogger log;

        /** {@inheritDoc} */
        @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, Object arg) throws IgniteCheckedException {
            try {
                mapper.send(new TestJob(), null);

                assert false;

            }
            catch (NullPointerException e) {
                if (log.isInfoEnabled())
                    log.info("Expected exception: " + e);
            }

            try {
                mapper.send(null, subgrid.get(0));

                assert false;
            }
            catch (NullPointerException e) {
                if (log.isInfoEnabled())
                    log.info("Expected exception: " + e);
            }

            try {
                mapper.send((Map<? extends ComputeJob, ClusterNode>)null);

                assert false;
            }
            catch (NullPointerException e) {
                if (log.isInfoEnabled())
                    log.info("Expected exception: " + e);
            }

            try {
                mapper.send(Collections.singletonMap(new TestJob(), (ClusterNode)null));

                assert false;
            }
            catch (IgniteCheckedException e) {
                if (log.isInfoEnabled())
                    log.info("Expected exception: " + e);
            }

            try {
                mapper.send((ComputeJob)null);

                assert false;
            }
            catch (NullPointerException e) {
                if (log.isInfoEnabled())
                    log.info("Expected exception: " + e);
            }

            try {
                mapper.send((Collection<ComputeJob>)null);

                assert false;
            }
            catch (NullPointerException e) {
                if (log.isInfoEnabled())
                    log.info("Expected exception: " + e);
            }

            try {
                mapper.send(Collections.singleton((ComputeJob)null));

                assert false;
            }
            catch (IgniteCheckedException e) {
                if (log.isInfoEnabled())
                    log.info("Expected exception: " + e);
            }

            mapper.send(new TestJob());

            return null;
        }

        /** {@inheritDoc} */
        @Override public Object reduce(List<ComputeJobResult> results) throws IgniteCheckedException {
            assert results.size() == 1;

            return null;
        }
    }

    /** */
    @SuppressWarnings({"PublicInnerClass"})
    public static class TestJob extends ComputeJobAdapter {
        /** */
        public TestJob() {
            super(-1);
        }

        /**
         * @param idx Index.
         */
        public TestJob(int idx) {
            super(idx);
        }

        /** {@inheritDoc} */
        @Override public Serializable execute() throws IgniteCheckedException {
            return argument(0);
        }
    }
}

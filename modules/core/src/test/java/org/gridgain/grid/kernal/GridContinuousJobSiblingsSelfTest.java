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
import org.apache.ignite.compute.*;
import org.apache.ignite.resources.*;
import org.gridgain.grid.*;
import org.gridgain.testframework.junits.common.*;

import java.io.*;
import java.util.*;

/**
 * Test continuous mapper with siblings.
 */
@GridCommonTest(group = "Kernal Self")
public class GridContinuousJobSiblingsSelfTest extends GridCommonAbstractTest {
    /** */
    private static final int JOB_COUNT = 10;

    /**
     * @throws Exception If test failed.
     */
    public void testContinuousJobSiblings() throws Exception {
        try {
            Ignite ignite = startGrid(0);
            startGrid(1);

            ignite.compute().execute(TestTask.class, null);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If test failed.
     */
    public void testContinuousJobSiblingsLocalNode() throws Exception {
        try {
            Ignite ignite = startGrid(0);

            compute(ignite.cluster().forLocal()).execute(TestTask.class, null);
        }
        finally {
            stopAllGrids();
        }
    }

    /** */
    private static class TestTask extends ComputeTaskSplitAdapter<Object, Object> {
        /** */
        @IgniteTaskContinuousMapperResource
        private ComputeTaskContinuousMapper mapper;

        /** */
        @IgniteTaskSessionResource
        private ComputeTaskSession ses;

        /** */
        private volatile int jobCnt;

        /** {@inheritDoc} */
        @Override protected Collection<? extends ComputeJob> split(int gridSize, Object arg) throws IgniteCheckedException {
            return Collections.singleton(new TestJob(++jobCnt));
        }

        /** {@inheritDoc} */
        @Override public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> received)
            throws IgniteCheckedException {
            if (res.getException() != null)
                throw new IgniteCheckedException("Job resulted in error: " + res, res.getException());

            assert ses.getJobSiblings().size() == jobCnt;

            if (jobCnt < JOB_COUNT) {
                mapper.send(new TestJob(++jobCnt));

                assert ses.getJobSiblings().size() == jobCnt;
            }

            return ComputeJobResultPolicy.WAIT;
        }

        /** {@inheritDoc} */
        @Override public Object reduce(List<ComputeJobResult> results) throws IgniteCheckedException {
            assertEquals(JOB_COUNT, results.size());

            return null;
        }
    }

    /** */
    private static class TestJob extends ComputeJobAdapter {
        /** */
        @IgniteTaskSessionResource
        private ComputeTaskSession ses;

        /** */
        @IgniteLoggerResource
        private IgniteLogger log;

        /**
         * @param sibCnt Siblings count to check.
         */
        TestJob(int sibCnt) {
            super(sibCnt);
        }

        /** {@inheritDoc} */
        @Override public Serializable execute() throws IgniteCheckedException {
            assert ses != null;
            assert argument(0) != null;

            Integer sibCnt = argument(0);

            log.info("Executing job.");

            assert sibCnt != null;

            Collection<ComputeJobSibling> sibs = ses.getJobSiblings();

            assert sibs != null;
            assert sibs.size() == sibCnt : "Unexpected siblings collection [expectedSize=" + sibCnt +
                ", siblingsCnt=" + sibs.size() + ", siblings=" + sibs + ']';

            return null;
        }
    }
}

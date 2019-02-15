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

package org.apache.ignite.internal;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobResultPolicy;
import org.apache.ignite.compute.ComputeJobSibling;
import org.apache.ignite.compute.ComputeTaskContinuousMapper;
import org.apache.ignite.compute.ComputeTaskSession;
import org.apache.ignite.compute.ComputeTaskSplitAdapter;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.resources.TaskContinuousMapperResource;
import org.apache.ignite.resources.TaskSessionResource;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Test continuous mapper with siblings.
 */
@GridCommonTest(group = "Kernal Self")
@RunWith(JUnit4.class)
public class GridContinuousJobSiblingsSelfTest extends GridCommonAbstractTest {
    /** */
    private static final int JOB_COUNT = 10;

    /**
     * @throws Exception If test failed.
     */
    @Test
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
    @Test
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
        @TaskContinuousMapperResource
        private ComputeTaskContinuousMapper mapper;

        /** */
        @TaskSessionResource
        private ComputeTaskSession ses;

        /** */
        private volatile int jobCnt;

        /** {@inheritDoc} */
        @Override protected Collection<? extends ComputeJob> split(int gridSize, Object arg) {
            return Collections.singleton(new TestJob(++jobCnt));
        }

        /** {@inheritDoc} */
        @Override public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> received) {
            if (res.getException() != null)
                throw new IgniteException("Job resulted in error: " + res, res.getException());

            assert ses.getJobSiblings().size() == jobCnt;

            if (jobCnt < JOB_COUNT) {
                mapper.send(new TestJob(++jobCnt));

                assert ses.getJobSiblings().size() == jobCnt;
            }

            return ComputeJobResultPolicy.WAIT;
        }

        /** {@inheritDoc} */
        @Override public Object reduce(List<ComputeJobResult> results) {
            assertEquals(JOB_COUNT, results.size());

            return null;
        }
    }

    /** */
    private static class TestJob extends ComputeJobAdapter {
        /** */
        @TaskSessionResource
        private ComputeTaskSession ses;

        /** */
        @LoggerResource
        private IgniteLogger log;

        /**
         * @param sibCnt Siblings count to check.
         */
        TestJob(int sibCnt) {
            super(sibCnt);
        }

        /** {@inheritDoc} */
        @Override public Serializable execute() {
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

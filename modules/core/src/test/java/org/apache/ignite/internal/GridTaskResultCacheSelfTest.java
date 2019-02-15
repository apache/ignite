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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobResultPolicy;
import org.apache.ignite.compute.ComputeTaskNoResultCache;
import org.apache.ignite.compute.ComputeTaskSplitAdapter;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;
import org.junit.Test;

/**
 *
 */
@GridCommonTest(group = "Kernal Self")
public class GridTaskResultCacheSelfTest extends GridCommonAbstractTest {
    /**
     *
     */
    public GridTaskResultCacheSelfTest() {
        super(true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNoCacheResultAnnotationUsage() throws Exception {
        Ignite ignite = G.ignite(getTestIgniteInstanceName());

        ignite.compute()
            .execute(GridResultNoCacheResultAnnotationTestTask.class, "Grid Result No Cache Annotation Test Argument");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNoCacheResultMethodUsage() throws Exception {
        Ignite ignite = G.ignite(getTestIgniteInstanceName());

        ignite.compute().withNoResultCache()
                .execute(GridResultNoCacheResultMethodTestTask.class, "Grid Result No Cache Method Test Argument");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCacheResults() throws Exception {
        Ignite ignite = G.ignite(getTestIgniteInstanceName());

        ignite.compute().execute(GridResultCacheTestTask.class, "Grid Result Cache Test Argument");
    }

    /**
     *
     */
    private static class GridResultNoCacheResultMethodTestTask extends GridAbstractCacheTestTask {
        /** {@inheritDoc} */
        @Override public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> rcvd) {
            assert res.getData() != null;
            assert rcvd.isEmpty();

            return super.result(res, rcvd);
        }

        /** {@inheritDoc} */
        @Override public Object reduce(List<ComputeJobResult> results) {
            assert results.isEmpty();

            return null;
        }
    }

    /**
     *
     */
    @ComputeTaskNoResultCache
    private static class GridResultNoCacheResultAnnotationTestTask extends GridResultNoCacheResultMethodTestTask {

    }

    /**
     *
     */
    private static class GridResultCacheTestTask extends GridAbstractCacheTestTask {
        /** {@inheritDoc} */
        @Override public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> rcvd) {
            assert res.getData() != null;
            assert rcvd.contains(res);

            for (ComputeJobResult jobRes : rcvd)
                assert jobRes.getData() != null;

            return super.result(res, rcvd);
        }

        /** {@inheritDoc} */
        @Override public Object reduce(List<ComputeJobResult> results) {
            for (ComputeJobResult res : results) {
                if (res.getException() != null)
                    throw res.getException();

                assert res.getData() != null;
            }

            return null;
        }
    }

    /**
     * Test task.
     */
    private abstract static class GridAbstractCacheTestTask extends ComputeTaskSplitAdapter<String, Object> {
        /** {@inheritDoc} */
        @Override protected Collection<? extends ComputeJob> split(int gridSize, String arg) {
            String[] words = arg.split(" ");

            Collection<ComputeJobAdapter> jobs = new ArrayList<>(words.length);

            for (String word : words) {
                jobs.add(new ComputeJobAdapter(word) {
                    @Override public Serializable execute() {
                        return argument(0);
                    }
                });
            }

            return jobs;
        }
    }
}

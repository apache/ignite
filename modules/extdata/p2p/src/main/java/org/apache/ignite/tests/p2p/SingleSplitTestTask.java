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

package org.apache.ignite.tests.p2p;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskSplitAdapter;

/**
 * Test task for P2P deployment tests.
 */
public class SingleSplitTestTask extends ComputeTaskSplitAdapter<Integer, Integer> {
    /**
     * {@inheritDoc}
     */
    @Override protected Collection<? extends ComputeJob> split(int gridSize, Integer arg) {
        assert gridSize > 0 : "Subgrid cannot be empty.";

        Collection<ComputeJobAdapter> jobs = new ArrayList<>(gridSize);

        for (int i = 0; i < arg; i++)
            jobs.add(new SingleSplitTestJob(1));

        return jobs;
    }

    /**
     * {@inheritDoc}
     */
    @Override public Integer reduce(List<ComputeJobResult> results) {
        int retVal = 0;

        for (ComputeJobResult res : results) {
            assert res.getException() == null : "Load test jobs can never fail: " + res;

            retVal += (Integer)res.getData();
        }

        return retVal;
    }

    /**
     * Test job for P2P deployment tests.
     */
    @SuppressWarnings("PublicInnerClass")
    public static final class SingleSplitTestJob extends ComputeJobAdapter {
        /**
         * @param args Job arguments.
         */
        public SingleSplitTestJob(Integer args) {
            super(args);
        }

        /** {@inheritDoc} */
        @Override public Serializable execute() {
            return new GridSingleSplitTestJobTarget().executeLoadTestJob((Integer)argument(0));
        }
    }
}
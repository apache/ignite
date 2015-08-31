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

package org.apache.loadtests.gridify;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.ignite.IgniteException;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskSplitAdapter;
import org.apache.ignite.compute.gridify.GridifyArgument;

/**
 * Gridify load test task.
 */
public class GridifyLoadTestTask extends ComputeTaskSplitAdapter<GridifyArgument, Integer> {
    /** {@inheritDoc} */
    @Override protected Collection<? extends ComputeJob> split(int gridSize, GridifyArgument arg) {
        assert gridSize > 0 : "Subgrid cannot be empty.";

        int jobsNum = (Integer)arg.getMethodParameters()[0];

        assert jobsNum > 0;

        Collection<ComputeJobAdapter> jobs = new ArrayList<>(gridSize);

        for (int i = 0; i < jobsNum; i++)
            jobs.add(new ComputeJobAdapter(1) {
                @Override public Serializable execute() {
                    Integer arg = this.<Integer>argument(0);

                    assert arg != null;

                    return new GridifyLoadTestJobTarget().executeLoadTestJob(arg);
                }
            });

        return jobs;
    }

    /** {@inheritDoc} */
    @Override public Integer reduce(List<ComputeJobResult> results) {
        int retVal = 0;

        for (ComputeJobResult res : results) {
            if (res.getException() != null) {
                throw new IgniteException("Received exception in reduce method (load test jobs can never fail): " + res,
                    res.getException());
            }

            retVal += (Integer)res.getData();
        }

        return retVal;
    }
}
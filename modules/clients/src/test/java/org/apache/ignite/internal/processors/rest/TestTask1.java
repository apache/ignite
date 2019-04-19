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

package org.apache.ignite.internal.processors.rest;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskSplitAdapter;
import org.apache.ignite.internal.util.typedef.X;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
class TestTask1 extends ComputeTaskSplitAdapter<String, String> {
    /** {@inheritDoc} */
    @Override protected Collection<? extends ComputeJob> split(int gridSize, String arg) {
        Collection<ComputeJob> jobs = new ArrayList<>(gridSize);

        for (int i = 0; i < gridSize; i++)
            jobs.add(new ComputeJobAdapter() {
                @Nullable @Override public Object execute() {
                    X.println("Test task1.");

                    return null;
                }
            });

        return jobs;
    }

    /** {@inheritDoc} */
    @Override public String reduce(List<ComputeJobResult> results) {
        return null;
    }
}
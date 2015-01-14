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

package org.gridgain.grid.kernal.processors.rest;

import org.apache.ignite.*;
import org.apache.ignite.compute.*;
import org.gridgain.grid.util.typedef.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 *
 */
class TestTask2 extends ComputeTaskSplitAdapter<String, String> {
    static final String RES = "Task 2 result.";

    /** {@inheritDoc} */
    @Override protected Collection<? extends ComputeJob> split(int gridSize, String arg) throws IgniteCheckedException {
        Collection<ComputeJob> jobs = new ArrayList<>(gridSize);

        for (int i = 0; i < gridSize; i++)
            jobs.add(new ComputeJobAdapter() {
                @Nullable
                @Override public Object execute() {
                    X.println("Test task2.");

                    return null;
                }
            });

        return jobs;
    }

    /** {@inheritDoc} */
    @Override public String reduce(List<ComputeJobResult> results) throws IgniteCheckedException {
        return RES;
    }
}

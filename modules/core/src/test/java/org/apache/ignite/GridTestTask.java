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

package org.apache.ignite;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskSplitAdapter;
import org.apache.ignite.resources.LoggerResource;

/**
 * Test task.
 */
public class GridTestTask extends ComputeTaskSplitAdapter<Object, Object> {
    /** Logger. */
    @LoggerResource
    private IgniteLogger log;

    /** {@inheritDoc} */
    @Override public Collection<? extends ComputeJob> split(int gridSize, Object arg) {
        if (log.isDebugEnabled())
            log.debug("Splitting task [task=" + this + ", gridSize=" + gridSize + ", arg=" + arg + ']');

        Collection<ComputeJob> refs = new ArrayList<>(gridSize);

        for (int i = 0; i < gridSize; i++)
            refs.add(new GridTestJob(arg.toString() + i + 1));

        return refs;
    }

    /** {@inheritDoc} */
    @Override public Object reduce(List<ComputeJobResult> results) {
        if (log.isDebugEnabled())
            log.debug("Reducing task [task=" + this + ", results=" + results + ']');

        return results;
    }
}
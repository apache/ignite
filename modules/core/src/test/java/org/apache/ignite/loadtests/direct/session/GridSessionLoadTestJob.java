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

package org.apache.ignite.loadtests.direct.session;

import java.io.Serializable;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeTaskSession;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.resources.TaskSessionResource;

/**
 * Session load test job.
 */
public class GridSessionLoadTestJob extends ComputeJobAdapter {
    /** */
    @TaskSessionResource
    private ComputeTaskSession taskSes;

    /** */
    @LoggerResource
    private IgniteLogger log;

    /** */
    public GridSessionLoadTestJob() {
        // No-op.
    }

    /**
     * @param arg Argument.
     */
    public GridSessionLoadTestJob(String arg) {
        super(arg);
    }

    /** {@inheritDoc} */
    @Override public Serializable execute() {
        assert taskSes != null;

        Object arg = argument(0);

        assert arg != null;

        Serializable ser = taskSes.getAttribute(arg);

        assert ser != null;

        int val = (Integer)ser + 1;

        // Generate garbage.
        for (int i = 0; i < 10; i++)
            taskSes.setAttribute(arg, i);

        // Set final value.
        taskSes.setAttribute(arg, val);

        if (log.isDebugEnabled())
            log.debug("Set session attribute [name=" + arg + ", value=" + val + ']');

        return val;
    }
}
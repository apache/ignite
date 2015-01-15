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

package org.gridgain.loadtests.direct.stealing;

import org.apache.ignite.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.resources.*;
import org.gridgain.grid.*;

import java.io.*;
import java.util.*;

/**
 * Stealing load test.
 */
public class GridStealingLoadTestJob extends ComputeJobAdapter {
    /** */
    @IgniteLoggerResource
    private IgniteLogger log;

    /** */
    @IgniteLocalNodeIdResource
    private UUID nodeId;

    /** */
    @IgniteJobContextResource
    private ComputeJobContext ctx;

    /** {@inheritDoc} */
    @Override public Serializable execute() throws IgniteCheckedException {
        if (log.isDebugEnabled())
            log.debug("Executing job on node [nodeId=" + nodeId + ", jobId=" + ctx.getJobId() + ']');

        try {
            Thread.sleep(500);
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }

        // Here we gonna return node id which executed this job.
        // Hopefully it would be stealing node.
        return nodeId;
    }
}

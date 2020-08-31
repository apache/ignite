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

import java.util.concurrent.CountDownLatch;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.resources.LoggerResource;

/**
 * Test job.
 */
public class GridTestJob extends ComputeJobAdapter {
    /** Logger. */
    @LoggerResource
    private IgniteLogger log;

    CountDownLatch latch;

    /** */
    public GridTestJob() {
        // No-op.
    }

    /**
     * @param arg Job argument.
     */
    public GridTestJob(String arg) {
        super(arg);
    }

    /**
     * @param arg Job argument.
     */
    public GridTestJob(String arg, CountDownLatch latch) {
        super(arg);
        this.latch = latch;
    }

    /** {@inheritDoc} */
    @Override public String execute() {
        if (log.isDebugEnabled())
            log.debug("Executing job [job=" + this + ", arg=" + argument(0) + ']');
        if (latch != null) {
            try {
                latch.await();
            } catch (InterruptedException e) {
                // Nothing to do
            }
        }

        return argument(0);
    }
}

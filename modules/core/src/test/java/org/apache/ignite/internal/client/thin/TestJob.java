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

package org.apache.ignite.internal.client.thin;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.testframework.junits.GridAbstractTest;

/**
 * Compute job which returns node id where it was executed.
 */
public class TestJob implements ComputeJob {
    /** Ignite. */
    @IgniteInstanceResource
    Ignite ignite;

    /** Sleep time. */
    private final Long sleepTime;

    /**
     * @param sleepTime Sleep time.
     */
    TestJob(Long sleepTime) {
        this.sleepTime = sleepTime;
    }

    /** {@inheritDoc} */
    @Override public void cancel() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public Object execute() throws IgniteException {
        if (sleepTime != null)
            GridAbstractTest.doSleep(sleepTime);

        return ignite.cluster().localNode().id();
    }
}

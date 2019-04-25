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

package org.apache.ignite.internal.client;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobResultPolicy;
import org.apache.ignite.compute.ComputeTaskSplitAdapter;

import static org.apache.ignite.compute.ComputeJobResultPolicy.FAILOVER;
import static org.apache.ignite.compute.ComputeJobResultPolicy.WAIT;

/**
 * Test task, that sleeps for 10 seconds in split and returns
 * the length of an argument.
 */
public class SleepTestTask extends ComputeTaskSplitAdapter<String, Integer> {
    /** {@inheritDoc} */
    @Override public Collection<? extends ComputeJob> split(int gridSize, String arg) {
        return Collections.singleton(new ComputeJobAdapter(arg) {
            @Override public Object execute() {
                try {
                    Thread.sleep(10000);

                    String val = argument(0);

                    return val == null ? 0 : val.length();
                }
                catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }

    /** {@inheritDoc} */
    @Override public Integer reduce(List<ComputeJobResult> results) {
        int sum = 0;

        for (ComputeJobResult res : results)
            sum += res.<Integer>getData();

        return sum;
    }

    /** {@inheritDoc} */
    @Override public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> rcvd) {
        if (res.getException() != null)
            return FAILOVER;

        return WAIT;
    }
}
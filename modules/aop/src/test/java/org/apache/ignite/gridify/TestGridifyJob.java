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

package org.apache.ignite.gridify;

import java.io.Serializable;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.resources.LoggerResource;

/**
 * Test gridify job.
 */
public class TestGridifyJob extends ComputeJobAdapter {
    /** */
    @LoggerResource
    private IgniteLogger log;

    /**
     * @param arg Argument.
     */
    public TestGridifyJob(String arg) {
        super(arg);
    }

    /** {@inheritDoc} */
    @Override public Serializable execute() {
        if (log.isInfoEnabled())
            log.info("Execute TestGridifyJob.execute(" + argument(0) + ')');

        TestAopTarget target = new TestAopTarget();

        try {
            if ("1".equals(argument(0)))
                return target.gridifyNonDefaultClass("10");
            else if ("2".equals(argument(0)))
                return target.gridifyNonDefaultName("20");
            else if ("3".equals(argument(0)))
                return target.gridifyNonDefaultClassResource("30");
            else if ("4".equals(argument(0)))
                return target.gridifyNonDefaultNameResource("40");
        }
        catch (TestGridifyException e) {
            throw new RuntimeException("Failed to execute target method.", e);
        }

        assert false : "Argument must be equals to \"0\" [gridifyArg=" + argument(0) + ']';

        // Never reached.
        return null;
    }
}
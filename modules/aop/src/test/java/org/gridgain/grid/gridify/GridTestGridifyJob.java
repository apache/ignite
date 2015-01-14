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

package org.gridgain.grid.gridify;

import org.apache.ignite.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.resources.*;
import org.gridgain.grid.*;

import java.io.*;

/**
 * Test gridify job.
 */
public class GridTestGridifyJob extends ComputeJobAdapter {
    /** */
    @IgniteLoggerResource
    private IgniteLogger log;

    /**
     * @param arg Argument.
     */
    public GridTestGridifyJob(String arg) {
        super(arg);
    }

    /** {@inheritDoc} */
    @Override public Serializable execute() throws IgniteCheckedException {
        if (log.isInfoEnabled())
            log.info("Execute GridTestGridifyJob.execute(" + argument(0) + ')');

        GridTestAopTarget target = new GridTestAopTarget();

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
        catch (GridTestGridifyException e) {
            throw new RuntimeException("Failed to execute target method.", e);
        }

        assert false : "Argument must be equals to \"0\" [gridifyArg=" + argument(0) + ']';

        // Never reached.
        return null;
    }
}

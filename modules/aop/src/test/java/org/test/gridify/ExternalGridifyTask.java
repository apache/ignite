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

package org.test.gridify;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskSplitAdapter;
import org.apache.ignite.compute.gridify.GridifyArgument;
import org.apache.ignite.resources.LoggerResource;

/**
 * External gridify task.
 */
public class ExternalGridifyTask extends ComputeTaskSplitAdapter<GridifyArgument, Object> {
    /** */
    public static final String TASK_NAME = "org.test.gridify.ExternalGridifyTask";

    /** {@inheritDoc} */
    @Override public Collection<? extends ComputeJob> split(int gridSize, GridifyArgument arg) {
        assert arg.getMethodParameters().length == 1;

        return Collections.singletonList(new ComputeJobAdapter((String)arg.getMethodParameters()[0]) {
            /** */
            @LoggerResource
            private IgniteLogger log;

            /** {@inheritDoc} */
            @Override public Serializable execute() {
                if (log.isInfoEnabled())
                    log.info("Execute TestGridifyJob.execute(" + argument(0) + ')');

                ExternalAopTarget target = new ExternalAopTarget();

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
                catch (ExternalGridifyException e) {
                    throw new RuntimeException("Failed to execute target method.", e);
                }

                assert false : "Argument must be equals to \"0\" [gridifyArg=" + argument(0) + ']';

                // Never reached.
                return null;
            }
        });
    }

    /** {@inheritDoc} */
    @Override public Object reduce(List<ComputeJobResult> results) {
        assert results.size() == 1;

        return results.get(0).getData();
    }
}
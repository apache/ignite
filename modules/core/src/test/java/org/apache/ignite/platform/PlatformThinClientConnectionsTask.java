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

package org.apache.ignite.platform;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.internal.processors.odbc.ClientListenerProcessor;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.mxbean.ClientProcessorMXBean;
import org.apache.ignite.testframework.junits.GridAbstractTest;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Task to get thin client connections.
 */
public class PlatformThinClientConnectionsTask extends ComputeTaskAdapter<Object, String[]> {
    /** {@inheritDoc} */
    @NotNull @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
        @Nullable Object arg) {
        return Collections.singletonMap(new PlatformThinClientConnectionsJob((String)arg), F.first(subgrid));
    }

    /** {@inheritDoc} */
    @Nullable @Override public String[] reduce(List<ComputeJobResult> results) {
        return results.get(0).getData();
    }

    /**
     * Job.
     */
    private static class PlatformThinClientConnectionsJob extends ComputeJobAdapter {
        /** */
        private final String igniteInstanceName;

        /** */
        public PlatformThinClientConnectionsJob(String igniteInstanceName) {
            this.igniteInstanceName = igniteInstanceName;
        }

        /** {@inheritDoc} */
        @Nullable @Override public String[] execute() {
            ClientProcessorMXBean bean = GridAbstractTest.getMxBean(igniteInstanceName, "Clients",
                    ClientListenerProcessor.class, ClientProcessorMXBean.class);

            List<String> connections = bean.getConnections();

            //noinspection ZeroLengthArrayAllocation
            return connections.toArray(new String[0]);
        }
    }
}

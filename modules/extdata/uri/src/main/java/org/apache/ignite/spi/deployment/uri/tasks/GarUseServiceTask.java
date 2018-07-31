/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.ignite.spi.deployment.uri.tasks;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.compute.ComputeTaskName;
import org.apache.ignite.resources.ServiceResource;

/**
 * This class defines grid task for this example. Grid task is responsible for splitting the task into jobs. This
 * particular implementation splits given string into individual words and creates grid jobs for each word. Task class
 * in that example should be placed in GAR file.
 *
 * Calls service bean during operation.
 */
@ComputeTaskName("GarUseServiceTask")
public class GarUseServiceTask extends ComputeTaskAdapter<String, String> {
    /**
     * Cluster singleton dependency.
     */
    @ServiceResource(serviceName = "runner", proxyInterface = Runnable.class)
    private Runnable runnable;

    /**
     * Creates task for every node.
     */
    @Override public Map<ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, String arg) {
        Map<ComputeJob, ClusterNode> result = new HashMap<>();
        for (ClusterNode node : subgrid) {
            result.put(new ComputeJobAdapter() {
                /*
                 * Simply calls the service.
                 */
                @Override public Serializable execute() {
                    runnable.run();

                    // This job does not return any result.
                    return null;
                }
            }, node);
        }
        return result;
    }

    /** */
    @Override public String reduce(List<ComputeJobResult> results) {
        return "OK";
    }
}

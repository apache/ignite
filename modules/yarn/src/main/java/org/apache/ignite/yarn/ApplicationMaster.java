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

package org.apache.ignite.yarn;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.yarn.api.*;
import org.apache.hadoop.yarn.api.protocolrecords.*;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.*;
import org.apache.hadoop.yarn.client.api.async.*;
import org.apache.hadoop.yarn.conf.*;
import org.apache.hadoop.yarn.util.*;

import java.util.*;

/**
 * TODO
 */
public class ApplicationMaster implements AMRMClientAsync.CallbackHandler {
    /** {@inheritDoc} */
    @Override public void onContainersCompleted(List<ContainerStatus> statuses) {

    }

    /** {@inheritDoc} */
    @Override public void onContainersAllocated(List<Container> containers) {

    }

    /** {@inheritDoc} */
    @Override public void onShutdownRequest() {

    }

    /** {@inheritDoc} */
    @Override public void onNodesUpdated(List<NodeReport> updatedNodes) {

    }

    /** {@inheritDoc} */
    @Override public float getProgress() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public void onError(Throwable e) {

    }

    /**
     * @param args Arguments.
     */
    public static void main(String[] args) throws Exception {
        final String command = args[0];
        final int n = Integer.valueOf(args[1]);

        // Initialize clients to ResourceManager and NodeManagers
        Configuration conf = new YarnConfiguration();

        AMRMClient<AMRMClient.ContainerRequest> rmClient = AMRMClient.createAMRMClient();
        rmClient.init(conf);
        rmClient.start();

        NMClient nmClient = NMClient.createNMClient();
        nmClient.init(conf);
        nmClient.start();

        // Register with ResourceManager
        rmClient.registerApplicationMaster("", 0, "");

        // Priority for worker containers - priorities are intra-application
        Priority priority = Records.newRecord(Priority.class);
        priority.setPriority(0);

        // Resource requirements for worker containers
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(128);
        capability.setVirtualCores(1);

        // Make container requests to ResourceManager
        for (int i = 0; i < n; ++i) {
            AMRMClient.ContainerRequest containerAsk =
                new AMRMClient.ContainerRequest(capability, null, null, priority);

            rmClient.addContainerRequest(containerAsk);
        }

        // Obtain allocated containers, launch and check for responses
        int responseId = 0;
        int completedContainers = 0;
        while (completedContainers < n) {
            AllocateResponse response = rmClient.allocate(responseId++);
            for (Container container : response.getAllocatedContainers()) {
                // Launch container by create ContainerLaunchContext
                ContainerLaunchContext ctx =
                    Records.newRecord(ContainerLaunchContext.class);

                ctx.setCommands(
                    Collections.singletonList(
                        command +
                            " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" +
                            " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"
                    ));

                nmClient.startContainer(container, ctx);
            }
            for (ContainerStatus status : response.getCompletedContainersStatuses()) {
                ++completedContainers;
                System.out.println("Completed container " + status.getContainerId());
            }
            Thread.sleep(100);
        }

        // Un-register with ResourceManager
        rmClient.unregisterApplicationMaster(
            FinalApplicationStatus.SUCCEEDED, "", "");
    }
}

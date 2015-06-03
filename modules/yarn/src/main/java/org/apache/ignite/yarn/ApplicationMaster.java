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

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
    YarnConfiguration configuration;
    NMClient nmClient;
    int numContainersToWaitFor = 1;

    public ApplicationMaster() {
        configuration = new YarnConfiguration();

        nmClient = NMClient.createNMClient();
        nmClient.init(configuration);
        nmClient.start();
    }

    /** {@inheritDoc} */
    public void onContainersAllocated(List<Container> containers) {
        for (Container container : containers) {
            try {
                // Launch container by create ContainerLaunchContext
                ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);

                final LocalResource igniteZip = Records.newRecord(LocalResource.class);
                setupAppMasterJar(new Path("/user/ntikhonov/gridgain-community-fabric-1.0.6.zip"), igniteZip,
                    configuration);

                ctx.setLocalResources(Collections.singletonMap("ignite", igniteZip));
                ctx.setCommands(
                        Lists.newArrayList(
                                "$LOCAL_DIRS/ignite/*/bin/ignite.sh" +
                                " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" +
                                " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"
                        ));
                System.out.println("[AM] Launching container " + container.getId());
                nmClient.startContainer(container, ctx);
            } catch (Exception ex) {
                System.err.println("[AM] Error launching container " + container.getId() + " " + ex);
            }
        }
    }

    /** {@inheritDoc} */
    private static void setupAppMasterJar(Path jarPath, LocalResource appMasterJar, YarnConfiguration conf)
        throws Exception {
        FileSystem fileSystem = FileSystem.get(conf);
        jarPath = fileSystem.makeQualified(jarPath);

        FileStatus jarStat = fileSystem.getFileStatus(jarPath);

        appMasterJar.setResource(ConverterUtils.getYarnUrlFromPath(jarPath));
        appMasterJar.setSize(jarStat.getLen());
        appMasterJar.setTimestamp(jarStat.getModificationTime());
        appMasterJar.setType(LocalResourceType.ARCHIVE);
        appMasterJar.setVisibility(LocalResourceVisibility.APPLICATION);

        System.out.println("Path :" + jarPath);
    }

    /** {@inheritDoc} */
    public void onContainersCompleted(List<ContainerStatus> statuses) {
        for (ContainerStatus status : statuses) {
            System.out.println("[AM] Completed container " + status.getContainerId());
            synchronized (this) {
                numContainersToWaitFor--;
            }
        }
    }

    /** {@inheritDoc} */
    public void onNodesUpdated(List<NodeReport> updated) {
    }

    /** {@inheritDoc} */
    public void onShutdownRequest() {
    }

    /** {@inheritDoc} */
    public void onError(Throwable t) {
    }

    /** {@inheritDoc} */
    public float getProgress() {
        return 50;
    }

    public boolean doneWithContainers() {
        return numContainersToWaitFor == 0;
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public static void main(String[] args) throws Exception {
        ApplicationMaster master = new ApplicationMaster();
        master.runMainLoop();
    }

    public void runMainLoop() throws Exception {

        AMRMClientAsync<AMRMClient.ContainerRequest> rmClient = AMRMClientAsync.createAMRMClientAsync(100, this);
        rmClient.init(getConfiguration());
        rmClient.start();

        // Register with ResourceManager
        System.out.println("[AM] registerApplicationMaster 0");
        rmClient.registerApplicationMaster("", 0, "");
        System.out.println("[AM] registerApplicationMaster 1");

        // Priority for worker containers - priorities are intra-application
        Priority priority = Records.newRecord(Priority.class);
        priority.setPriority(0);

        // Resource requirements for worker containers
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(128);
        capability.setVirtualCores(1);

        // Make container requests to ResourceManager
        for (int i = 0; i < numContainersToWaitFor; ++i) {
            AMRMClient.ContainerRequest containerAsk = new AMRMClient.ContainerRequest(capability, null, null, priority);
            System.out.println("[AM] Making res-req " + i);
            rmClient.addContainerRequest(containerAsk);
        }

        System.out.println("[AM] waiting for containers to finish");
        while (!doneWithContainers()) {
            Thread.sleep(100);
        }



        System.out.println("[AM] unregisterApplicationMaster 0");
        // Un-register with ResourceManager
        rmClient.unregisterApplicationMaster(
                FinalApplicationStatus.SUCCEEDED, "", "");
        System.out.println("[AM] unregisterApplicationMaster 1");
    }
}

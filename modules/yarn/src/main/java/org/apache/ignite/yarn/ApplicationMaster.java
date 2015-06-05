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

import org.apache.commons.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.*;
import org.apache.hadoop.yarn.client.api.async.*;
import org.apache.hadoop.yarn.conf.*;
import org.apache.hadoop.yarn.util.*;
import org.apache.ignite.yarn.utils.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * TODO
 */
public class ApplicationMaster implements AMRMClientAsync.CallbackHandler {
    /** Default port range. */
    public static final String DEFAULT_PORT = ":47500..47510";

    /** Delimiter char. */
    public static final String DELIM = ",";

    /** */
    private YarnConfiguration conf;

    /** */
    private ClusterProperties props;

    /** */
    private NMClient nmClient;

    /** */
    private Path ignitePath;

    /** */
    private Path cfgPath;

    /** */
    private FileSystem fs;

    /** */
    private Map<String, IgniteContainer> containers = new HashMap<>();

    /**
     * Constructor.
     */
    public ApplicationMaster(String ignitePath, ClusterProperties props) throws Exception {
        this.conf = new YarnConfiguration();
        this.props = props;
        this.fs = FileSystem.get(conf);
        this.ignitePath = new Path(ignitePath);

        nmClient = NMClient.createNMClient();

        nmClient.init(conf);
        nmClient.start();
    }

    /** {@inheritDoc} */
    public synchronized void onContainersAllocated(List<Container> conts) {
        for (Container container : conts) {
            try {
                ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);

                Map<String, String> env = new HashMap<>(System.getenv());

                env.put("IGNITE_TCP_DISCOVERY_ADDRESSES", getAddress(container.getNodeId().getHost()));

                ctx.setEnvironment(env);

                Map<String, LocalResource> resources = new HashMap<>();

                resources.put("ignite", IgniteYarnUtils.setupFile(ignitePath, fs, LocalResourceType.ARCHIVE));
                resources.put("ignite-config.xml", IgniteYarnUtils.setupFile(cfgPath, fs, LocalResourceType.FILE));

                ctx.setLocalResources(resources);

                ctx.setCommands(
                    Collections.singletonList(
                        "./ignite/*/bin/ignite.sh "
                        + "./ignite-config.xml"
                        + " -J-Xmx" + container.getResource().getMemory() + "m"
                        + " -J-Xms" + container.getResource().getMemory() + "m"
                        + IgniteYarnUtils.YARN_LOG_OUT
                    ));

                System.out.println("[AM] Launching container " + container.getId());

                nmClient.startContainer(container, ctx);

                containers.put(container.getNodeId().getHost(),
                    new IgniteContainer(container.getNodeId().getHost(), container.getResource().getVirtualCores(),
                        container.getResource().getMemory()));
            }
            catch (Exception ex) {
                System.err.println("[AM] Error launching container " + container.getId() + " " + ex);
            }
        }
    }

    /**
     * @return Address running nodes.
     */
    private String getAddress(String address) {
        if (containers.isEmpty()) {
            if (address != null && !address.isEmpty())
                return address + DEFAULT_PORT;

            return "";
        }

        StringBuilder sb = new StringBuilder();

        for (IgniteContainer cont : containers.values())
            sb.append(cont.host()).append(DEFAULT_PORT).append(DELIM);

        return sb.substring(0, sb.length() - 1);
    }

    /** {@inheritDoc} */
    public synchronized void onContainersCompleted(List<ContainerStatus> statuses) {
        for (ContainerStatus status : statuses) {
            synchronized (this) {
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
        nmClient.stop();
    }

    /** {@inheritDoc} */
    public float getProgress() {
        return 50;
    }

    /**
     * @param args Args.
     * @throws Exception If failed.
     */
    public static void main(String[] args) throws Exception {
        ClusterProperties props = ClusterProperties.from(null);

        ApplicationMaster master = new ApplicationMaster(args[0], props);

        master.init();

        master.run();
    }

    /**
     * Runs application master.
     *
     * @throws Exception If failed.
     */
    public void run() throws Exception {
        // Create asyn application master.
        AMRMClientAsync<AMRMClient.ContainerRequest> rmClient = AMRMClientAsync.createAMRMClientAsync(300, this);

        rmClient.init(conf);
        rmClient.start();

        // Register with ResourceManager
        rmClient.registerApplicationMaster("", 0, "");

        System.out.println("[AM] registerApplicationMaster 1");

        // Priority for worker containers - priorities are intra-application
        Priority priority = Records.newRecord(Priority.class);
        priority.setPriority(0);

        // Resource requirements for worker containers
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(1024);
        capability.setVirtualCores(2);

        // Make container requests to ResourceManager
        for (int i = 0; i < 1; ++i) {
            AMRMClient.ContainerRequest containerAsk =
                new AMRMClient.ContainerRequest(capability, null, null, priority);

            System.out.println("[AM] Making res-req " + i);

            rmClient.addContainerRequest(containerAsk);
        }

        System.out.println("[AM] waiting for containers to finish");

        TimeUnit.MINUTES.sleep(10);

        System.out.println("[AM] unregisterApplicationMaster 0");

        // Un-register with ResourceManager
        rmClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "", "");

        System.out.println("[AM] unregisterApplicationMaster 1");
    }

    /**
     * @throws IOException
     */
    public void init() throws IOException {
        if (props.igniteConfigUrl() == null || props.igniteConfigUrl().isEmpty()) {
            InputStream input = Thread.currentThread().getContextClassLoader()
                .getResourceAsStream(IgniteYarnUtils.DEFAULT_IGNITE_CONFIG);

            cfgPath = new Path(props.igniteWorkDir() + File.separator + IgniteYarnUtils.DEFAULT_IGNITE_CONFIG);

            // Create file. Override by default.
            FSDataOutputStream outputStream = fs.create(cfgPath, true);

            IOUtils.copy(input, outputStream);

            IOUtils.closeQuietly(input);

            IOUtils.closeQuietly(outputStream);
        }
        else
            cfgPath = new Path(props.igniteConfigUrl());
    }
}

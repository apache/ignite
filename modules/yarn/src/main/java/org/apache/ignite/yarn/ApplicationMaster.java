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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Records;
import org.apache.ignite.yarn.utils.IgniteYarnUtils;

/**
 * Application master request containers from Yarn and decides how many resources will be occupied.
 */
public class ApplicationMaster implements AMRMClientAsync.CallbackHandler {
    /** */
    public static final Logger log = Logger.getLogger(ApplicationMaster.class.getSimpleName());

    /** Default port range. */
    public static final String DEFAULT_PORT = ":47500..47510";

    /** Delimiter char. */
    public static final String DELIM = ",";

    /** */
    private long schedulerTimeout = TimeUnit.SECONDS.toMillis(1);

    /** Yarn configuration. */
    private final YarnConfiguration conf;

    /** Cluster properties. */
    private final ClusterProperties props;

    /** Network manager. */
    private NMClient nmClient;

    /** Resource manager. */
    private AMRMClientAsync<AMRMClient.ContainerRequest> rmClient;

    /** Ignite path. */
    private final Path ignitePath;

    /** Config path. */
    private Path cfgPath;

    /** Hadoop file system. */
    private FileSystem fs;

    /** Buffered tokens to be injected into newly allocated containers. */
    private ByteBuffer allTokens;

    /** Running containers. */
    private final Map<ContainerId, IgniteContainer> containers = new ConcurrentHashMap<>();

    /**
     * @param ignitePath Hdfs path to ignite.
     * @param props Cluster properties.
     */
    public ApplicationMaster(String ignitePath, ClusterProperties props) throws Exception {
        this.conf = new YarnConfiguration();
        this.props = props;
        this.ignitePath = new Path(ignitePath);
    }

    /** {@inheritDoc} */
    public synchronized void onContainersAllocated(List<Container> conts) {
        for (Container c : conts) {
            if (checkContainer(c)) {
                try {
                    ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);

                    if (UserGroupInformation.isSecurityEnabled())
                        // Set the tokens to the newly allocated container:
                        ctx.setTokens(allTokens.duplicate());

                    Map<String, String> env = new HashMap<>(System.getenv());

                    env.put("IGNITE_TCP_DISCOVERY_ADDRESSES", getAddress(c.getNodeId().getHost()));

                    if (props.jvmOpts() != null && !props.jvmOpts().isEmpty())
                        env.put("JVM_OPTS", props.jvmOpts());

                    ctx.setEnvironment(env);

                    Map<String, LocalResource> resources = new HashMap<>();

                    resources.put("ignite", IgniteYarnUtils.setupFile(ignitePath, fs, LocalResourceType.ARCHIVE));
                    resources.put("ignite-config.xml", IgniteYarnUtils.setupFile(cfgPath, fs, LocalResourceType.FILE));

                    if (props.licencePath() != null)
                        resources.put("gridgain-license.xml",
                            IgniteYarnUtils.setupFile(new Path(props.licencePath()), fs, LocalResourceType.FILE));

                    if (props.userLibs() != null)
                        resources.put("libs", IgniteYarnUtils.setupFile(new Path(props.userLibs()), fs,
                            LocalResourceType.FILE));

                    ctx.setLocalResources(resources);

                    ctx.setCommands(
                        Collections.singletonList(
                            (props.licencePath() != null ? "cp gridgain-license.xml ./ignite/*/ || true && " : "")
                            + "cp -r ./libs/* ./ignite/*/libs/ || true && "
                            + "./ignite/*/bin/ignite.sh "
                            + "./ignite-config.xml"
                            + " -J-Xmx" + ((int)props.memoryPerNode()) + "m"
                            + " -J-Xms" + ((int)props.memoryPerNode()) + "m"
                            + IgniteYarnUtils.YARN_LOG_OUT
                        ));

                    log.log(Level.INFO, "Launching container: {0}.", c.getId());

                    nmClient.startContainer(c, ctx);

                    containers.put(c.getId(),
                        new IgniteContainer(
                            c.getId(),
                            c.getNodeId(),
                            c.getResource().getVirtualCores(),
                            c.getResource().getMemory()));
                }
                catch (Exception ex) {
                    log.log(Level.WARNING, "Error launching container " + c.getId(), ex);
                }
            }
            else
                rmClient.releaseAssignedContainer(c.getId());
        }
    }

    /**
     * @param cont Container.
     * @return {@code True} if container satisfies requirements.
     */
    private boolean checkContainer(Container cont) {
        // Check limit on running nodes.
        if (props.instances() <= containers.size())
            return false;

        // Check host name
        if (props.hostnameConstraint() != null
                && props.hostnameConstraint().matcher(cont.getNodeId().getHost()).matches())
            return false;

        // Check that slave satisfies min requirements.
        if (cont.getResource().getVirtualCores() < props.cpusPerNode()
            || cont.getResource().getMemory() < props.totalMemoryPerNode()) {
            log.log(Level.FINE, "Container resources not sufficient requirements. Host: {0}, cpu: {1}, mem: {2}",
                new Object[]{cont.getNodeId().getHost(), cont.getResource().getVirtualCores(),
                   cont.getResource().getMemory()});

            return false;
        }

        return true;
    }

    /**
     * @return Address running nodes.
     */
    private String getAddress(String addr) {
        if (containers.isEmpty()) {
            if (addr != null && !addr.isEmpty())
                return addr + DEFAULT_PORT;

            return "";
        }

        StringBuilder sb = new StringBuilder();

        for (IgniteContainer cont : containers.values())
            sb.append(cont.nodeId.getHost()).append(DEFAULT_PORT).append(DELIM);

        return sb.substring(0, sb.length() - 1);
    }

    /** {@inheritDoc} */
    public synchronized void onContainersCompleted(List<ContainerStatus> statuses) {
        for (ContainerStatus status : statuses) {
            containers.remove(status.getContainerId());

            log.log(Level.INFO, "Container completed. Container id: {0}. State: {1}.",
                new Object[]{status.getContainerId(), status.getState()});
        }
    }

    /** {@inheritDoc} */
    public synchronized void onNodesUpdated(List<NodeReport> updated) {
        for (NodeReport node : updated) {
            // If node unusable.
            if (node.getNodeState().isUnusable()) {
                for (IgniteContainer cont : containers.values()) {
                    if (cont.nodeId().equals(node.getNodeId())) {
                        containers.remove(cont.id());

                        log.log(Level.WARNING, "Node is unusable. Node: {0}, state: {1}.",
                            new Object[]{node.getNodeId().getHost(), node.getNodeState()});
                    }
                }

                log.log(Level.WARNING, "Node is unusable. Node: {0}, state: {1}.",
                    new Object[]{node.getNodeId().getHost(), node.getNodeState()});
            }
        }
    }

    /** {@inheritDoc} */
    public void onShutdownRequest() {
        // No-op.
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
        ClusterProperties props = ClusterProperties.from();

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
        // Register with ResourceManager
        rmClient.registerApplicationMaster("", 0, "");

        log.log(Level.INFO, "Application master registered.");

        // Priority for worker containers - priorities are intra-application
        Priority priority = Records.newRecord(Priority.class);
        priority.setPriority(0);

        try {
            // Check ignite cluster.
            while (!nmClient.isInState(Service.STATE.STOPPED)) {
                int runningCnt = containers.size();

                if (runningCnt < props.instances() && checkAvailableResource()) {
                    // Resource requirements for worker containers.
                    Resource capability = Records.newRecord(Resource.class);

                    capability.setMemory((int)props.totalMemoryPerNode());
                    capability.setVirtualCores((int)props.cpusPerNode());

                    for (int i = 0; i < props.instances() - runningCnt; ++i) {
                        // Make container requests to ResourceManager
                        AMRMClient.ContainerRequest containerAsk =
                            new AMRMClient.ContainerRequest(capability, null, null, priority);

                        rmClient.addContainerRequest(containerAsk);

                        log.log(Level.INFO, "Making request. Memory: {0}, cpu {1}.",
                            new Object[]{props.totalMemoryPerNode(), props.cpusPerNode()});
                    }
                }

                TimeUnit.MILLISECONDS.sleep(schedulerTimeout);
            }
        }
        catch (InterruptedException ignored) {
            // Un-register with ResourceManager
            rmClient.unregisterApplicationMaster(FinalApplicationStatus.KILLED, "", "");

            log.log(Level.WARNING, "Application master killed.");
        }
        catch (Exception e) {
            // Un-register with ResourceManager
            rmClient.unregisterApplicationMaster(FinalApplicationStatus.FAILED, "", "");

            log.log(Level.SEVERE, "Application master failed.", e);
        }
    }

    /**
     * @return {@code True} if cluster contains available resources.
     */
    private boolean checkAvailableResource() {
        Resource availableRes = rmClient.getAvailableResources();

        return availableRes == null || availableRes.getMemory() >= props.totalMemoryPerNode()
            && availableRes.getVirtualCores() >= props.cpusPerNode();
    }

    /**
     * @throws IOException
     */
    public void init() throws IOException {
        if (UserGroupInformation.isSecurityEnabled()) {
            Credentials cred = UserGroupInformation.getCurrentUser().getCredentials();

            allTokens = IgniteYarnUtils.createTokenBuffer(cred);
        }

        fs = FileSystem.get(conf);

        nmClient = NMClient.createNMClient();

        nmClient.init(conf);
        nmClient.start();

        // Create async application master.
        rmClient = AMRMClientAsync.createAMRMClientAsync(300, this);

        rmClient.init(conf);

        rmClient.start();

        if (props.igniteCfg() == null || props.igniteCfg().isEmpty()) {
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
            cfgPath = new Path(props.igniteCfg());
    }

    /**
     * Sets NMClient.
     *
     * @param nmClient NMClient.
     */
    public void setNmClient(NMClient nmClient) {
        this.nmClient = nmClient;
    }

    /**
     * Sets RMClient
     *
     * @param rmClient AMRMClientAsync.
     */
    public void setRmClient(AMRMClientAsync<AMRMClient.ContainerRequest> rmClient) {
        this.rmClient = rmClient;
    }

    /**
     * Sets scheduler timeout.
     *
     * @param schedulerTimeout Scheduler timeout.
     */
    public void setSchedulerTimeout(long schedulerTimeout) {
        this.schedulerTimeout = schedulerTimeout;
    }

    /**
     * Sets file system.
     * @param fs File system.
     */
    public void setFs(FileSystem fs) {
        this.fs = fs;
    }

    /**
     * JUST FOR TESTING!!!
     *
     * @return Running containers.
     */
    @Deprecated
    public Map<ContainerId, IgniteContainer> getContainers() {
        return containers;
    }
}

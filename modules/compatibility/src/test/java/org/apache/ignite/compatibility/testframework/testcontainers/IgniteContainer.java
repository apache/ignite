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

package org.apache.ignite.compatibility.testframework.testcontainers;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import com.github.dockerjava.api.model.ContainerNetwork;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.lifecycle.Startable;
import org.testcontainers.utility.DockerImageName;

import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;
import static org.testcontainers.utility.MountableFile.forClasspathResource;

/** Ignite container. */
public class IgniteContainer extends GenericContainer<IgniteContainer> implements Startable {
    /** Property for local work directory. */
    static final String LOCAL_WORK_DIR_PROP = "local.work.dir";

    /** Local work directory. */
    public static final String LOCAL_WORK_DIR_PATH = System.getProperty(LOCAL_WORK_DIR_PROP,
        System.getProperty("user.home") + "/test-ignite-work");

    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger(IgniteContainer.class);

    /** Ignite root directory in container. */
    private static final String ROOT_DIR_PATH = "/opt/ignite/apache-ignite/";

    /** Ignite work directory in container. */
    private static final String WORK_DIR_PATH = ROOT_DIR_PATH + "work";

    /** Config path in container. */
    private static final String CFG_PATH = ROOT_DIR_PATH + "config/test-config-%d.xml";

    /** */
    private static final String ENABLE_EXPERIMENTAL_FLAG = "--enable-experimental";

    /** */
    private static final Pattern CLUSTER_STATE_PATTERN = Pattern.compile("Cluster state: (ACTIVE|INACTIVE)");

    /** Hostname. */
    private final String hostname;

    /** Constructor. */
    public IgniteContainer(String commitHash, Network net, String hostname, String staticIp, int idx) {
        super(DockerImageName.parse("apacheignite/ignite:" + commitHash));

        String finalPath = String.format(CFG_PATH, idx);

        this.hostname = hostname;

        withEnv("CONFIG_URI", "file://" + finalPath);
        withEnv("IGNITE_QUIET", "false");
        withEnv("IGNITE_WORK_DIR", WORK_DIR_PATH + "/" + hostname);
        withEnv("TZ", java.time.ZoneId.systemDefault().toString());

        this.withLogConsumer(new Slf4jLogConsumer(LOGGER));

        File nodeDir = new File(LOCAL_WORK_DIR_PATH + '/' + hostname);

        if (!nodeDir.exists())
            nodeDir.mkdirs();

        nodeDir.setWritable(true, false);
        nodeDir.setReadable(true, false);
        nodeDir.setExecutable(true, false); // Required for directories so users can enter them

        withFileSystemBind(nodeDir.getAbsolutePath(), WORK_DIR_PATH + '/' + hostname, BindMode.READ_WRITE);
        withCopyFileToContainer(forClasspathResource("docker/test-config.xml"), finalPath);

        withNetwork(net);
        withNetworkAliases(hostname);
        withExposedPorts(ClientConnectorConfiguration.DFLT_PORT, TcpCommunicationSpi.DFLT_PORT, TcpDiscoverySpi.DFLT_PORT);

        this.withCreateContainerCmdModifier(cmd -> {
            cmd.getHostConfig().withNetworkMode(net.getId());
            cmd.withIpv4Address(staticIp);
        });

        waitingFor(Wait.forLogMessage(".*Node started.*", 1).withStartupTimeout(Duration.ofSeconds(90)));
    }

    /** */
    public String localWorkDirectory() {
        return LOCAL_WORK_DIR_PATH + "/" + hostname;
    }

    /** @return Client address. */
    public String serverAddress() {
        return address(TcpDiscoverySpi.DFLT_PORT);
    }

    /** @return Client address. */
    public String clientAddress() {
        return address(ClientConnectorConfiguration.DFLT_PORT);
    }

    /** */
    private String address(int port) {
        return getHost() + ":" + getMappedPort(port);
    }

    /** Activate cluster. */
    public void activateCluster(int nodeCnt) {
        execControl("--set-state", "ACTIVE", "--yes");

        try {
            boolean success = waitForCondition(() -> {
                String out = execControl("--state");

                Matcher matcher = CLUSTER_STATE_PATTERN.matcher(out);

                if (matcher.find())
                    return ClusterState.valueOf(matcher.group(1)) == ClusterState.ACTIVE;

                return false;
            }, 30_000);

            if (!success)
                throw new IllegalStateException("Failed to set state ACTIVE");

            success = waitForCondition(() -> {
                String out = execControl("--baseline");

                System.out.println(">>> Out=" + out);

                return out.contains("Number of baseline nodes: " + nodeCnt);
            }, 30_000, 5_000);

            if (!success)
                throw new IllegalStateException("Check cluster count failed");
        }
        catch (IgniteInterruptedCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /** */
    private String execControl(String... cmd) {
        String[] fullCmd = new String[cmd.length + 2];

        fullCmd[0] = ROOT_DIR_PATH + "bin/control.sh";
        fullCmd[1] = ENABLE_EXPERIMENTAL_FLAG;

        System.arraycopy(cmd, 0, fullCmd, 2, cmd.length);

        ExecResult result;

        try {
            LOGGER.info("Running command: {}", Arrays.toString(fullCmd).replace(", ", " "));

            result = execInContainer(fullCmd);
        }
        catch (IOException | InterruptedException e) {
            throw new IgniteException(e);
        }

        if (result.getExitCode() != 0)
            throw new IllegalStateException(result.toString());

        return result.getStdout();
    }

    /** {@inheritDoc} */
    @Override public void stop() {
        if (isRunning()) {
            try {
                LOGGER.info("Sending SIGTERM to Ignite node {} for graceful shutdown...", hostname);

                getDockerClient().killContainerCmd(getContainerId())
                    .withSignal("TERM")
                    .exec();

                long stopTimeoutSeconds = 60;

                await()
                    .atMost(Duration.ofSeconds(stopTimeoutSeconds))
                    .pollInterval(Duration.ofMillis(500))
                    .until(() -> !isRunning());
            } catch (Exception e) {
                LOGGER.warn("Graceful shutdown failed for node {}. Proceeding with forceful stop.", hostname, e);
            }
        }

        LOGGER.info("Ignite node {} shut down gracefully.", hostname);

        // ==============================================================
        // POST-STOP CLEANUP: FIX OWNERSHIP USING AN ALPINE CONTAINER
        // ==============================================================
        try {
            LOGGER.info("Repairing file ownership metrics for host JVM usage on {}...", hostname);

            // This runs a tiny container to chown everything inside node1 back to your user
            // 174208964 is your exact host UID from the log inspection!
            String hostWorkspace = LOCAL_WORK_DIR_PATH + "/" + hostname;

            new GenericContainer<>(DockerImageName.parse("alpine:latest"))
                .withFileSystemBind(hostWorkspace, "/target", BindMode.READ_WRITE)
                .withCommand("sh", "-c", "chown -R 174208964:174200513 /target && chmod -R 777 /target")
                .start(); // Blocks temporarily, executes the fix, and shuts down instantly

        } catch (Exception e) {
            LOGGER.error("Failed to repair volume directory permissions via background container hook.", e);
        }

        super.stop();
    }

    /**
     * Dynamically resolves the Docker Network Gateway IP address for this container.
     */
    public String getGatewayIp() {
        if (!isRunning())
            throw new IllegalStateException("Container must be running to extract runtime network parameters.");

        Map<String, ContainerNetwork> networks = getContainerInfo().getNetworkSettings().getNetworks();

        if (networks.isEmpty())
            return "127.0.0.1";

        ContainerNetwork activeNet = networks.values().iterator().next();
        String gateway = activeNet.getGateway();

        LOGGER.info("Successfully discovered dynamic Docker gateway IP: {}", gateway);

        return gateway;
    }

}

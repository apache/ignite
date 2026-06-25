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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.compatibility.testframework.plugins.TestCompatibilityPluginProvider;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import static org.apache.ignite.compatibility.testframework.testcontainers.ContainerAddressResolver.EXT_ADDR_PROP_PREFIX;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;
import static org.testcontainers.utility.MountableFile.forClasspathResource;
import static org.testcontainers.utility.MountableFile.forHostPath;

/** Ignite container. */
public class IgniteContainer extends GenericContainer<IgniteContainer> {
    /** Property for local work directory. */
    private static final String LOCAL_WORK_DIR_PROP = "local.work.dir";

    /** Local work directory. */
    public static final String LOCAL_WORK_DIR_PATH = System.getProperty(LOCAL_WORK_DIR_PROP,
        System.getProperty("user.dir") + "/target/test-ignite-work");

    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger(IgniteContainer.class);

    /** Ignite root directory in container. */
    private static final String ROOT_DIR_PATH = "/opt/ignite/apache-ignite/";

    /** Ignite work directory in container. */
    private static final String WORK_DIR_PATH = ROOT_DIR_PATH + "work";

    /** Config path in container. */
    private static final String CFG_PATH = ROOT_DIR_PATH + "config/test-config.xml";

    /** */
    private static final String ENABLE_EXPERIMENTAL_FLAG = "--enable-experimental";

    /** */
    private static final Pattern CLUSTER_STATE_PATTERN = Pattern.compile("Cluster state: (ACTIVE|INACTIVE)");

    /** Base host port for the published discovery port (node index added). Kept clear of the host-node ports. */
    private static final int DISCO_HOST_PORT_BASE = 50500;

    /** Base host port for the published communication port (node index added). */
    private static final int COMM_HOST_PORT_BASE = 50100;

    /** Base host port for the published thin-client port (node index added). */
    private static final int CLIENT_HOST_PORT_BASE = 50800;

    /** Custom classes used by node in containers. */
    private static final List<String> TEST_CLASSES = List.of(
        ContainerAddressResolver.class.getName(),
        TestCompatibilityPluginProvider.class.getName()
    );

    /** Jar holding {@link #TEST_CLASSES}, injected so the old image can load it. */
    private static volatile File testClassesJar;

    /** Hostname. */
    private final String hostname;

    /** Consistent ID. */
    private final String consistentId;

    /** Path to work directory. */
    private final String workDirPath;

    /** Constructor. */
    public IgniteContainer(String commitHash, Network net, String hostname, String consistentId, int idx) throws IOException {
        super(DockerImageName.parse("apacheignite/ignite:" + commitHash));

        this.hostname = hostname;
        this.consistentId = consistentId;
        workDirPath = WORK_DIR_PATH + "/" + hostname;

        int discoHostPort = DISCO_HOST_PORT_BASE + idx;
        int commHostPort = COMM_HOST_PORT_BASE + idx;

        withEnv("CONFIG_URI", "file://" + CFG_PATH);
        withEnv("IGNITE_QUIET", "false");
        withEnv("IGNITE_WORK_DIR", workDirPath);
        withEnv("IGNITE_LOCAL_HOST", "0.0.0.0");
        withEnv("TZ", ZoneId.systemDefault().toString());

        // On macOS the host JVM cannot reach container-internal addresses, so each node advertises its
        // host-published discovery/communication ports (127.0.0.1:hostPort) via ContainerAddressResolver.
        // node.consistent.id pins the node's consistent id (and thus its persistence folder) to consistentId so the
        // upgraded host node, started with the same consistent id, inherits this node's persisted data.
        withEnv("JVM_OPTS", "-Xms512m -Xmx1g" + " -Dnode.consistent.id=" + consistentId
            + " -D" + EXT_ADDR_PROP_PREFIX + TcpDiscoverySpi.DFLT_PORT + "=127.0.0.1:" + discoHostPort
            + " -D" + EXT_ADDR_PROP_PREFIX + TcpCommunicationSpi.DFLT_PORT + "=127.0.0.1:" + commHostPort);

        withFileSystemBind(LOCAL_WORK_DIR_PATH, WORK_DIR_PATH, BindMode.READ_WRITE);
        withCopyFileToContainer(forClasspathResource("docker/test-config.xml"), CFG_PATH);
        withCopyFileToContainer(forHostPath(testClassesJar().getAbsolutePath()), ROOT_DIR_PATH + "libs/test-classes.jar");

        withNetwork(net);
        withNetworkAliases(hostname);

        // Stream container logs to stdout so they appear in the IDE test runner.
        withLogConsumer(frame -> System.out.println("[" + consistentId + "] " + frame.getUtf8String().trim()));

        // Fixed host ports so the host JVM node can target each container deterministically.
        addFixedExposedPort(CLIENT_HOST_PORT_BASE + idx, ClientConnectorConfiguration.DFLT_PORT);
        addFixedExposedPort(commHostPort, TcpCommunicationSpi.DFLT_PORT);
        addFixedExposedPort(discoHostPort, TcpDiscoverySpi.DFLT_PORT);

        waitingFor(Wait.forLogMessage(".*Node started.*", 1)
            .withStartupTimeout(Duration.ofSeconds(600)));
    }

    /** {@inheritDoc} */
    @Override public void stop() {
        if (isRunning()) {
            try {
                LOGGER.info("Sending SIGTERM to Ignite node {} for graceful shutdown...", hostname);

                getDockerClient().killContainerCmd(getContainerId())
                    .withSignal("TERM")
                    .exec();

                await()
                    .atMost(Duration.ofSeconds(60))
                    .pollInterval(Duration.ofMillis(500))
                    .until(() -> !isRunning());
            }
            catch (Exception e) {
                LOGGER.warn("Graceful shutdown failed for node {}. Proceeding with forceful stop.", hostname, e);
            }
        }

        LOGGER.info("Ignite node {} shut down gracefully.", hostname);

        super.stop();
    }

    /** @return Consistent ID. */
    public String consistentId() {
        return consistentId;
    }

    /** */
    public String localWorkDirectory() {
        return LOCAL_WORK_DIR_PATH + "/" + hostname;
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

    /** @return Client address. */
    public String clientAddress() {
        return address(ClientConnectorConfiguration.DFLT_PORT);
    }

    /** */
    public String discoveryAddress() {
        return address(TcpDiscoverySpi.DFLT_PORT);
    }

    /** */
    private String address(int port) {
        return getHost() + ":" + getMappedPort(port);
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

    /** @return Jar with {@link #TEST_CLASSES}, built once and reused for all containers. */
    private static File testClassesJar() throws IOException {
        File jar = testClassesJar;

        if (jar != null)
            return jar;

        synchronized (IgniteContainer.class) {
            if (testClassesJar != null)
                return testClassesJar;

            jar = File.createTempFile("test-classes", ".jar");
            jar.deleteOnExit();

            try (JarOutputStream out = new JarOutputStream(new FileOutputStream(jar))) {
                for (String cls : TEST_CLASSES) {
                    String clsPath = cls.replace('.', '/') + ".class";

                    try (InputStream in = IgniteContainer.class.getClassLoader().getResourceAsStream(clsPath)) {
                        if (in == null)
                            throw new IOException("Class not found on classpath: " + clsPath);

                        out.putNextEntry(new JarEntry(clsPath));

                        in.transferTo(out);

                        out.closeEntry();
                    }
                }
            }

            return testClassesJar = jar;
        }
    }
}

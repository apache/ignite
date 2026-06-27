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

import com.github.dockerjava.api.model.ContainerNetwork;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
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

    /**
     * {@code true} on Linux, where the host shares the Docker bridge and reaches containers directly. Elsewhere
     * (macOS/Windows Docker Desktop) the host talks to containers through a VM proxy, so the address hacks
     * (published ports + ContainerAddressResolver + host.docker.internal) are used instead.
     */
    public static final boolean LINUX = System.getProperty("os.name", "").toLowerCase().contains("linux");

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

    /** Custom classes (with their nested classes) used by node in containers. */
    private static final List<String> TEST_CLASSES = List.of(
        ContainerAddressResolver.class.getName(),
        TestCompatibilityPluginProvider.class.getName(),
        "org.apache.ignite.compatibility.testframework.plugins.DisabledRollingUpgradeProcessor",
        "org.apache.ignite.compatibility.testframework.plugins.DisabledValidationProcessor"
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

        // node.consistent.id pins the node's consistent id (and thus its persistence folder) so the upgraded host
        // node, started with the same consistent id, inherits this node's persisted data.
        String jvmOpts = "-Xms512m -Xmx1g -Dnode.consistent.id=" + consistentId;

        // Proxy-networking hosts (macOS/Windows) can't reach container-internal addresses, so each node advertises
        // its host-published ports (127.0.0.1:hostPort) via ContainerAddressResolver. On Linux containers are
        // directly routable and advertise their real address, so no override is needed.
        if (!LINUX) {
            jvmOpts += " -D" + EXT_ADDR_PROP_PREFIX + TcpDiscoverySpi.DFLT_PORT + "=127.0.0.1:" + discoHostPort
                + " -D" + EXT_ADDR_PROP_PREFIX + TcpCommunicationSpi.DFLT_PORT + "=127.0.0.1:" + commHostPort;
        }

        withEnv("JVM_OPTS", jvmOpts);

        withFileSystemBind(LOCAL_WORK_DIR_PATH, WORK_DIR_PATH, BindMode.READ_WRITE);
        withCopyFileToContainer(forClasspathResource("docker/test-config.xml"), CFG_PATH);
        withCopyFileToContainer(forHostPath(testClassesJar().getAbsolutePath()), ROOT_DIR_PATH + "libs/test-classes.jar");

        withNetwork(net);
        withNetworkAliases(hostname);

        // Stream container logs to stdout so they appear in the IDE test runner.
        withLogConsumer(frame -> System.out.println("[" + consistentId + "] " + frame.getUtf8String().trim()));

        // Proxy-networking hosts only: publish fixed host ports so the host JVM node can target each container at
        // 127.0.0.1:<port>. On Linux the host reaches containers at their bridge IP directly, so nothing is published.
        if (!LINUX) {
            addFixedExposedPort(CLIENT_HOST_PORT_BASE + idx, ClientConnectorConfiguration.DFLT_PORT);
            addFixedExposedPort(commHostPort, TcpCommunicationSpi.DFLT_PORT);
            addFixedExposedPort(discoHostPort, TcpDiscoverySpi.DFLT_PORT);
        }

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
                    .atMost(Duration.ofSeconds(10))
                    .pollInterval(Duration.ofMillis(500))
                    .until(() -> !isRunning());
            }
            catch (Exception e) {
                LOGGER.warn("Graceful shutdown failed for node {}. Proceeding with forceful stop.", hostname, e);
            }
        }

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

    /** @return Address the host JVM uses to reach this container's {@code port}. */
    private String address(int port) {
        return LINUX ? bridgeIp() + ":" + port : getHost() + ":" + getMappedPort(port);
    }

    /** @return Gateway IP of the test Docker network — the address containers use to reach the host JVM on Linux. */
    public String gatewayIp() {
        return network().getGateway();
    }

    /** @return This container's IP on the test Docker network (directly routable from the host on Linux). */
    private String bridgeIp() {
        return network().getIpAddress();
    }

    /** @return This container's attachment to the single test Docker network. */
    private ContainerNetwork network() {
        return getContainerInfo().getNetworkSettings().getNetworks().values().iterator().next();
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

                    URL url = IgniteContainer.class.getClassLoader().getResource(clsPath);

                    if (url == null)
                        throw new IOException("Class not found on classpath: " + clsPath);

                    File dir;

                    try {
                        dir = new File(url.toURI()).getParentFile();
                    }
                    catch (URISyntaxException e) {
                        throw new IOException(e);
                    }

                    String pkg = clsPath.substring(0, clsPath.lastIndexOf('/') + 1);
                    String simple = cls.substring(cls.lastIndexOf('.') + 1);

                    // Include the class and its nested classes (e.g. the provider's anonymous $1).
                    File[] clsFiles = dir.listFiles((d, name) ->
                        name.equals(simple + ".class") || name.startsWith(simple + '$'));

                    if (clsFiles == null)
                        throw new IOException("Cannot list class directory: " + dir);

                    for (File f : clsFiles) {
                        out.putNextEntry(new JarEntry(pkg + f.getName()));

                        Files.copy(f.toPath(), out);

                        out.closeEntry();
                    }
                }
            }

            return testClassesJar = jar;
        }
    }
}

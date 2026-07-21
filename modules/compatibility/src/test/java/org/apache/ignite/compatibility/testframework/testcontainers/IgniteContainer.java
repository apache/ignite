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

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import com.github.dockerjava.api.command.ExecCreateCmdResponse;
import com.github.dockerjava.api.command.InspectExecResponse;
import com.github.dockerjava.api.model.ContainerNetwork;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.compatibility.testframework.plugins.DisabledRollingUpgradeProcessor;
import org.apache.ignite.compatibility.testframework.plugins.DisabledValidationProcessor;
import org.apache.ignite.compatibility.testframework.plugins.TestCompatibilityPluginProvider;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.shaded.com.github.dockerjava.core.command.ExecStartResultCallback;
import org.testcontainers.utility.DockerImageName;

import static org.apache.ignite.compatibility.testframework.testcontainers.ContainerAddressResolver.EXT_ADDR_PROP_PREFIX;
import static org.apache.ignite.testframework.GridTestUtils.DFLT_TEST_TIMEOUT;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;
import static org.junit.Assert.assertTrue;
import static org.testcontainers.utility.MountableFile.forClasspathResource;
import static org.testcontainers.utility.MountableFile.forHostPath;

/** Ignite container. */
public class IgniteContainer extends GenericContainer<IgniteContainer> {
    /** Local work directory. */
    public static final String LOCAL_WORK_DIR_PATH = System.getProperty("ru.local.work.dir",
        U.getIgniteHome() + "/target/test-ignite-work");

    /**
     * {@code true} on Linux, where the host shares the Docker bridge and reaches containers directly. Elsewhere
     * (macOS/Windows Docker Desktop) the host talks to containers through a VM proxy, so the address hacks
     * (published ports + ContainerAddressResolver + host.docker.internal) are used instead.
     */
    public static final boolean LINUX = System.getProperty("os.name", "").toLowerCase().contains("linux");

    /** Host directory with target-version jars for DOCKER upgrade mode, overridable via {@code -Dru.target.libs.dir}. */
    private static final Path TARGET_LIBS_DIR = Path.of(System.getProperty("ru.target.libs.dir",
        U.getIgniteHome() + "/target/ignite-target-libs"));

    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger(IgniteContainer.class);

    /** Ignite root directory in container. */
    private static final String ROOT_DIR_PATH = "/opt/ignite/apache-ignite/";

    /** Ignite libs directory in container. */
    private static final String LIBS_DIR_PATH = ROOT_DIR_PATH + "libs/";

    /** Ignite work directory in container. */
    private static final String WORK_DIR_PATH = ROOT_DIR_PATH + "work";

    /** Config path in container. */
    private static final String CFG_PATH = ROOT_DIR_PATH + "config/test-config.xml";

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
        DisabledRollingUpgradeProcessor.class.getName(),
        DisabledValidationProcessor.class.getName()
    );

    /** Seconds to wait after SIGTERM before SIGKILL. */
    private static final int SHUTDOWN_TIMEOUT_SEC = 30;

    /** Jar holding {@link #TEST_CLASSES}, injected so the old image can load it. */
    private static volatile File testClassesJar;

    /** Hostname. */
    private final String hostname;

    /** Consistent ID. */
    private final String consistentId;

    /** Path to work directory. */
    private final String workDirPath;

    /**
     * Constructor with a commit hash (image tag).
     * Uses {@code apacheignite/ignite:<commitHash>} as the Docker image.
     */
    public IgniteContainer(String commitHash, Network net, String hostname, String consistentId, int idx) throws Exception {
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

        // Containers advertise published ports as external addresses via ContainerAddressResolver.
        // This is needed on all platforms:
        //   - macOS/Windows: container-internal bridge IPs are not routable from the host
        //   - Linux: bridge IPs (172.x.x.x) may be blocked by host firewall (nftables/iptables)
        // Published ports (127.0.0.1:5050x) are always reachable from the host via Docker port forwarding.
        jvmOpts += " -D" + EXT_ADDR_PROP_PREFIX + TcpDiscoverySpi.DFLT_PORT + "=127.0.0.1:" + discoHostPort
            + " -D" + EXT_ADDR_PROP_PREFIX + TcpCommunicationSpi.DFLT_PORT + "=127.0.0.1:" + commHostPort;

        withEnv("JVM_OPTS", jvmOpts);

        File locWorkDir = new File(LOCAL_WORK_DIR_PATH);

        if (!locWorkDir.exists())
            locWorkDir.mkdirs();

        withFileSystemBind(LOCAL_WORK_DIR_PATH, WORK_DIR_PATH, BindMode.READ_WRITE);

        // On Linux, run as the host user so bind-mounted directories (work dir, etc.) are owned by
        // the host user and can be cleaned up without root. Docker supports numeric UID:GID without
        // the user existing in the container's /etc/passwd.
        if (LINUX) {
            String uidGid = hostUserUidGid();

            LOGGER.info("Running container {} as host user uid/gid: {}", hostname, uidGid);

            withCreateContainerCmdModifier(cmd -> cmd.withUser(uidGid));
        }

        withCopyFileToContainer(forClasspathResource("docker/test-config.xml"), CFG_PATH);
        withCopyFileToContainer(forHostPath(testClassesJar().getAbsolutePath()), LIBS_DIR_PATH + "test-classes.jar");

        withNetwork(net);
        withNetworkAliases(hostname);

        withLogConsumer(frame -> System.out.println("[" + consistentId + "] " + frame.getUtf8String().trim()));

        // Always publish fixed host ports so the host JVM node and thin client can reach each container at
        // 127.0.0.1:<port> via Testcontainers port forwarding. On Linux the bridge-internal IP (172.x) may
        // be unreachable due to host firewall rules (firewalld/nftables) or Docker-in-VM setups (WSL2,
        // VirtualBox), so published ports are the only reliable cross-platform approach.
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
                stopGraceful();
            }
            catch (Exception e) {
                LOGGER.warn("Graceful shutdown failed for node {}. Proceeding with forceful stop.", hostname, e);
            }
        }

        super.stop();
    }

    /** In-place upgrade inside Docker: clean libs → graceful stop → swap libs → restart. */
    public void upgradeAndRestart() throws Exception {
        LOGGER.info("Cleaning up old libs in container {}", hostname);

        if (LINUX) {
            // Container runs as host-UID:GID (see constructor), but libs/ files are owned by root
            // inside the Docker image. Non-root cannot unlink root-owned files even with 777 on the
            // parent directory (fs.protected_regular on Linux). Run rm as root via the low-level API.
            ExecCreateCmdResponse execResp = getDockerClient().execCreateCmd(getContainerId())
                .withUser("root")
                .withAttachStdout(true)
                .withAttachStderr(true)
                .withCmd("sh", "-c", "rm -rf " + LIBS_DIR_PATH + "*")
                .exec();

            ByteArrayOutputStream err = new ByteArrayOutputStream();

            getDockerClient().execStartCmd(execResp.getId())
                .exec(new ExecStartResultCallback(new ByteArrayOutputStream(), err))
                .awaitCompletion(30, TimeUnit.SECONDS);

            InspectExecResponse resp = getDockerClient().inspectExecCmd(execResp.getId()).exec();

            if (!Boolean.TRUE.equals(resp.isRunning()) && resp.getExitCodeLong() != null && resp.getExitCodeLong() != 0)
                throw new IllegalStateException("Failed to clean libs: " + err);
        }
        else {
            ExecResult result = execInContainer("sh", "-c", "rm -rf " + LIBS_DIR_PATH + "*");

            if (result.getExitCode() != 0)
                throw new IllegalStateException("Failed to clean libs: " + result.getStderr());
        }

        // Copy new libs BEFORE stopping — docker exec won't work on a stopped container.
        // Files will be overwritten on restart; this ensures they're present in the container FS.
        copyDirectoryToContainer(TARGET_LIBS_DIR, LIBS_DIR_PATH);

        stopGraceful();

        restartWithTargetLibs(TARGET_LIBS_DIR);

        assertTrue("Upgraded Docker node is not running", isRunning());
    }

    /**
     * Stop the container gracefully <b>without removing it</b> (container stays in "Exited" state).
     * Call this before {@link #restartWithTargetLibs(Path)}.
     *
     * <p>Sends SIGTERM via {@code docker stop} and waits up to {@value #SHUTDOWN_TIMEOUT_SEC} seconds
     * before SIGKILL. The entrypoint {@code run.sh} uses {@code exec java}, so JVM receives SIGTERM directly
     * and runs its shutdown hooks. The extended timeout ensures Ignite can leave the cluster properly
     * (flush persistence, notify discovery neighbors, close socket connections) so that remaining nodes
     * don't trigger spurious "Failed to check connection to previous node" warnings during teardown.</p>
     */
    private void stopGraceful() {
        if (!isRunning())
            return;

        LOGGER.info("Graceful stop of node {} (timeout {} s)", hostname, SHUTDOWN_TIMEOUT_SEC);

        getDockerClient().stopContainerCmd(getContainerId())
            .withTimeout(SHUTDOWN_TIMEOUT_SEC)
            .exec();

        LOGGER.info("Node {} stopped", hostname);
    }

    /**
     * Restart the stopped container after swapping its {@code libs/} directory.
     * <p>
     * Copies all jars and subdirectories from the provided host directory into the container's {@code /opt/ignite/apache-ignite/libs/},
     * then re-injects the test-classes jar and starts the container.
     * </p>
     *
     * @param targetLibsHostDir Host directory containing target-version jars.
     */
    private void restartWithTargetLibs(Path targetLibsHostDir) throws Exception {
        LOGGER.info("Replacing libs in container {} with jars from {}", hostname, targetLibsHostDir);

        try (Stream<Path> files = Files.list(targetLibsHostDir)) {
            for (Path file : files.toArray(Path[]::new)) {
                String destPath = LIBS_DIR_PATH + file.getFileName().toString();

                if (Files.isRegularFile(file))
                    copyFileToContainer(forHostPath(file.toAbsolutePath().toString()), destPath);
                else if (Files.isDirectory(file))
                    copyDirectoryToContainerViaArchive(file, destPath);
            }
        }

        // Re-inject the test-classes jar.
        copyFileToContainer(forHostPath(testClassesJar().getAbsolutePath()), LIBS_DIR_PATH + "test-classes.jar");

        LOGGER.info("Starting container {} with target libraries...", hostname);

        getDockerClient().startContainerCmd(getContainerId()).exec();

        // isRunning() may cache stale state — inspect directly.
        assertTrue(waitForCondition(() -> {
            try {
                return "running".equals(getDockerClient().inspectContainerCmd(getContainerId()).exec().getState().getStatus());
            }
            catch (Exception e) {
                return false;
            }
        }, DFLT_TEST_TIMEOUT));

        LOGGER.info("Restarted node {} with target libraries", hostname);
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

            checkNodeCount(nodeCnt);
        }
        catch (IgniteInterruptedCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /** Check node count in cluster.*/
    public void checkNodeCount(int nodeCnt) {
        try {
            boolean success = waitForCondition(() -> {
                String out = execControl("--baseline");

                LOGGER.debug(">>> Baseline output={}", out);

                return out.contains("Number of baseline nodes: " + nodeCnt);
            }, 30_000, 5_000);

            if (!success)
                throw new IllegalStateException("Check cluster count failed");
        }
        catch (IgniteInterruptedCheckedException e) {
            throw new RuntimeException(e);
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
        // Always use published ports via Testcontainers port forwarding. This works on all platforms
        // (Linux, macOS, Windows) because Docker Desktop / Docker-in-VM may not route container-internal
        // bridge IPs (172.x) from the host.
        return getHost() + ":" + getMappedPort(port);
    }

    /** @return Gateway IP of the test Docker network — the address containers use to reach the host JVM on Linux. */
    public String gatewayIp() {
        return network().getGateway();
    }

    /** @return This container's attachment to the single test Docker network. */
    private ContainerNetwork network() {
        return getContainerInfo().getNetworkSettings().getNetworks().values().iterator().next();
    }

    /** */
    private String execControl(String... cmd) {
        String[] fullCmd = new String[cmd.length + 1];

        fullCmd[0] = ROOT_DIR_PATH + "bin/control.sh";

        System.arraycopy(cmd, 0, fullCmd, 1, cmd.length);

        ExecResult result;

        try {
            LOGGER.info("Running command: {}", Arrays.toString(fullCmd).replace(", ", " "));

            result = execInContainer(fullCmd);
        }
        catch (IOException | InterruptedException e) {
            throw new IgniteException(e);
        }

        if (result.getExitCode() != 0)
            throw new IllegalStateException(result.getStderr());

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

    /**
     * Copies a host directory recursively into the container using a gzipped tar archive.
     *
     * @param sourceDir Host directory to copy.
     * @param destPath  Destination path inside the container.
     */
    private void copyDirectoryToContainer(Path sourceDir, String destPath) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        try (TarArchiveOutputStream taos = new TarArchiveOutputStream(new GzipCompressorOutputStream(baos));
             Stream<Path> files = Files.walk(sourceDir)) {
            taos.setLongFileMode(TarArchiveOutputStream.LONGFILE_POSIX);

            files.forEach(srcFile -> {
                String entryName = sourceDir.relativize(srcFile).toString();

                try {
                    TarArchiveEntry entry = new TarArchiveEntry(srcFile.toFile(), entryName);
                    taos.putArchiveEntry(entry);

                    if (entry.isFile())
                        Files.copy(srcFile, taos);

                    taos.closeArchiveEntry();
                }
                catch (IOException e) {
                    throw new RuntimeException("Failed to add tar entry: " + entryName, e);
                }
            });
        }

        try {
            ExecCreateCmdResponse execResp = getDockerClient().execCreateCmd(getContainerId())
                .withUser("root")
                .withAttachStdout(true)
                .withAttachStderr(true)
                .withAttachStdin(true)
                .withCmd("sh", "-c", "mkdir -p " + destPath + " && tar xzf - -C " + destPath)
                .exec();

            getDockerClient().execStartCmd(execResp.getId())
                .withStdIn(new ByteArrayInputStream(baos.toByteArray()))
                .exec(new ExecStartResultCallback(new ByteArrayOutputStream(), System.err))
                .awaitCompletion(60, TimeUnit.SECONDS);

            InspectExecResponse resp = getDockerClient().inspectExecCmd(execResp.getId()).exec();

            if (!Boolean.TRUE.equals(resp.isRunning()) && resp.getExitCodeLong() != null && resp.getExitCodeLong() != 0)
                throw new IOException("Failed to copy directory " + sourceDir + " to " + destPath);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new IOException("Interrupted while copying directory " + sourceDir, e);
        }
    }

    /**
     * Copies a host directory into the container using Docker's {@code copyArchiveToContainer} API.
     * Unlike {@link #copyDirectoryToContainer(Path, String)}, this method works on <b>stopped</b> containers
     * because it uses the low-level Docker API instead of {@code docker exec}.
     *
     * <p>The tar entries include the directory name so extraction into {@code LIBS_DIR_PATH} produces the
     * correct nested path (e.g. {@code ignite-control-utility/file.jar}).</p>
     *
     * @param sourceDir Host directory to copy.
     * @param destPath  Destination path inside the container.
     */
    private void copyDirectoryToContainerViaArchive(Path sourceDir, String destPath) throws IOException {
        // Extract the directory name (e.g. "ignite-control-utility") to include in tar entries.
        String dirName = Path.of(destPath).getFileName().toString();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        try (TarArchiveOutputStream taos = new TarArchiveOutputStream(new GzipCompressorOutputStream(baos));
             Stream<Path> files = Files.walk(sourceDir)) {
            taos.setLongFileMode(TarArchiveOutputStream.LONGFILE_POSIX);

            files.forEach(srcFile -> {
                Path relativePath = sourceDir.relativize(srcFile);
                // Include the directory name so extraction into LIBS_DIR_PATH produces the correct nested path.
                String entryName = dirName + "/" + relativePath;

                try {
                    TarArchiveEntry entry = new TarArchiveEntry(srcFile.toFile(), entryName);
                    taos.putArchiveEntry(entry);

                    if (entry.isFile())
                        Files.copy(srcFile, taos);

                    taos.closeArchiveEntry();
                }
                catch (IOException e) {
                    throw new RuntimeException("Failed to add tar entry: " + entryName, e);
                }
            });
        }

        getDockerClient().copyArchiveToContainerCmd(getContainerId())
            .withTarInputStream(new ByteArrayInputStream(baos.toByteArray()))
            .withRemotePath(LIBS_DIR_PATH)
            .exec();
    }

    /**
     * Returns the "uid:gid" of the current host user.
     * Guaranteed to read the process stream before cleanup.
     */
    private static String hostUserUidGid() throws IOException, InterruptedException {
        // Corrected shell syntax to output "UID:GID" cleanly on a single line
        ProcessBuilder pb = new ProcessBuilder("sh", "-c", "echo $(id -u):$(id -g)");
        pb.redirectErrorStream(true);

        Process p = pb.start();
        String resultLine;

        // 1. Read the stream while the process is active or finishing
        try (BufferedReader r = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
            resultLine = r.readLine();
        }

        // 2. Wait for the process to exit completely
        boolean completed = p.waitFor(5, TimeUnit.SECONDS);

        if (!completed) {
            p.destroyForcibly();

            throw new IOException("Timed out waiting for id command");
        }

        if (p.exitValue() != 0)
            throw new IOException("Command 'id' failed with exit code: " + p.exitValue() + ". Output: " + resultLine);

        if (resultLine == null || resultLine.trim().isEmpty())
            throw new IOException("Empty output from 'id' command");

        return resultLine.trim();
    }
}

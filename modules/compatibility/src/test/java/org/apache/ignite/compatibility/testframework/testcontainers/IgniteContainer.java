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
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.JarURLConnection;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.JarOutputStream;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import com.github.dockerjava.api.command.ExecCreateCmdResponse;
import com.github.dockerjava.api.command.InspectExecResponse;
import com.github.dockerjava.api.command.StopContainerCmd;
import com.github.dockerjava.api.model.ContainerNetwork;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
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
import org.testcontainers.containers.wait.strategy.WaitStrategy;
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

    /** */
    private static final Pattern CLUSTER_STATE_PATTERN = Pattern.compile("Cluster state: (ACTIVE|INACTIVE)");

    /** Base host port for the published discovery port (node index added). Kept clear of the host-node ports. */
    private static final int DISCO_HOST_PORT_BASE = 50500;

    /** Base host port for the published communication port (node index added). */
    private static final int COMM_HOST_PORT_BASE = 50100;

    /** Base host port for the published thin-client port (node index added). */
    private static final int CLIENT_HOST_PORT_BASE = 50800;

    /** Seconds to wait after SIGTERM before SIGKILL. */
    private static final int SHUTDOWN_TIMEOUT_SEC = 30;

    /** Jar holding the {@link #testClasses() test classes}, injected so the old image can load it. */
    private static volatile File testClassesJar;

    /** Cached tar archive of {@link #TARGET_LIBS_DIR} + test-classes.jar, built once and reused for all containers. */
    private static volatile Path targetLibsArchive;

    /** Cached "uid:gid" of the host user. */
    private static volatile String hostUidGid;

    /** Hostname. */
    private final String hostname;

    /** Consistent ID. */
    private final String consistentId;

    /** Ignite root directory in container, computed from {@link #rootDirPath()}. */
    private final String rootDir;

    /** Ignite libs directory in container. */
    private final String libsDirPath;

    /** Config path in container. */
    private final String cfgPath;

    /**
     * @param imageName Image name.
     * @param net Network.
     * @param hostname Hostname.
     * @param consistentId Consistent ID.
     * @param idx Node index.
     */
    public IgniteContainer(String imageName, Network net, String hostname, String consistentId, int idx) throws Exception {
        super(DockerImageName.parse(imageName));

        this.hostname = hostname;
        this.consistentId = consistentId;
        rootDir = rootDirPath();
        libsDirPath = rootDir + "libs/";
        String workDirBase = rootDir + "work";
        cfgPath = rootDir + "config/test-config.xml";

        int discoHostPort = DISCO_HOST_PORT_BASE + idx;
        int commHostPort = COMM_HOST_PORT_BASE + idx;

        withEnv("CONFIG_URI", "file://" + cfgPath);
        // Some entrypoints (e.g. bin/ignite.sh) resolve the config from DEFAULT_CONFIG rather than CONFIG_URI.
        // Point it at the same config so both entrypoint styles load it; run.sh-based images ignore DEFAULT_CONFIG.
        withEnv("DEFAULT_CONFIG", cfgPath);
        withEnv("IGNITE_QUIET", "false");
        withEnv("IGNITE_WORK_DIR", workDirBase + "/" + hostname);
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

        withFileSystemBind(LOCAL_WORK_DIR_PATH, workDirBase, BindMode.READ_WRITE);

        // On Linux, run as the host user so bind-mounted directories (work dir, etc.) are owned by
        // the host user and can be cleaned up without root. Docker supports numeric UID:GID without
        // the user existing in the container's /etc/passwd.
        if (LINUX) {
            String uidGid = hostUserUidGid();

            LOGGER.info("Running container {} as host user uid/gid: {}", hostname, uidGid);

            withCreateContainerCmdModifier(cmd -> cmd.withUser(uidGid));
        }

        withCopyFileToContainer(forClasspathResource(commonConfigResource()), rootDir + "config/common-test-config.xml");
        withCopyFileToContainer(forClasspathResource(sourceConfigResource()), cfgPath);
        withCopyFileToContainer(forHostPath(testClassesJar().getAbsolutePath()), libsDirPath + "test-classes.jar");

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

        waitingFor(waitStrategy());
    }

    /** @return Wait strategy for the node to become ready. */
    protected WaitStrategy waitStrategy() {
        return Wait.forLogMessage(".*Node started.*", 1).withStartupTimeout(Duration.ofSeconds(600));
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

    /** In-place upgrade inside Docker: copy archive → clean+extract → graceful stop → restart. */
    public void upgradeAndRestart() throws Exception {
        LOGGER.info("Upgrading libs in container {}", hostname);

        // Build/retrieve the cached tar archive (shared across all containers).
        Path archivePath = libsArchive();

        String archivePathInContainer = "/tmp/target-libs.tar";

        copyFileToContainer(forHostPath(archivePath.toAbsolutePath().toString()), archivePathInContainer);

        ExecCreateCmdResponse execResp = getDockerClient().execCreateCmd(getContainerId())
            .withUser("root")
            .withAttachStdout(true)
            .withAttachStderr(true)
            .withCmd("sh", "-c",
                "rm -rf " + libsDirPath + "* && tar xf " + archivePathInContainer + " -C " + libsDirPath
                + " && rm -f " + archivePathInContainer)
            .exec();

        ByteArrayOutputStream err = new ByteArrayOutputStream();

        getDockerClient().execStartCmd(execResp.getId())
            .exec(new ExecStartResultCallback(new ByteArrayOutputStream(), err))
            .awaitCompletion(60, TimeUnit.SECONDS);

        InspectExecResponse resp = getDockerClient().inspectExecCmd(execResp.getId()).exec();

        if (!Boolean.TRUE.equals(resp.isRunning()) && resp.getExitCodeLong() != null && resp.getExitCodeLong() != 0)
            throw new IllegalStateException("Failed to clean and extract libs: " + err);

        copyFileToContainer(forClasspathResource(targetConfigResource()), cfgPath);

        stopGraceful();

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

        assertTrue("Upgraded Docker node is not running", isRunning());
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
            }, 30_000, 1_000);

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

    /** @return Gateway IP of the test Docker network — the address containers use to reach the host JVM on Linux. */
    public String gatewayIp() {
        return network().getGateway();
    }

    /** */
    protected String execControl(String... cmd) {
        ExecResult result;

        try {
            String[] fullCmd = command(cmd);

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

    /**
     * Builds the {@code control.sh} command line to be executed inside the container.
     *
     * @param cmd Control utility arguments (e.g. {@code --set-state ACTIVE --yes}).
     * @return Full command whose first element is the absolute path to {@code control.sh}, followed by {@code cmd}.
     */
    protected String[] command(String... cmd) {
        String[] fullCmd = new String[cmd.length + 1];

        fullCmd[0] = rootDir + "bin/control.sh";

        System.arraycopy(cmd, 0, fullCmd, 1, cmd.length);

        return fullCmd;
    }

    /** @return Classpath resource of the common (shared) node config copied into the container. */
    protected String commonConfigResource() {
        return "docker/common-test-config.xml";
    }

    /** @return Classpath resource of the source (pre-upgrade) node config copied into the container. */
    protected String sourceConfigResource() {
        return "docker/test-config.xml";
    }

    /** @return Classpath resource of the node config used on the target (upgraded) side during in-place Docker upgrade. */
    protected String targetConfigResource() {
        return "docker/target-test-config.xml";
    }

    /** @return Ignite root directory inside the container, with a trailing slash. */
    protected String rootDirPath() {
        return "/opt/ignite/apache-ignite/";
    }

    /** @return Custom classes (with their nested classes) used by the node in containers. */
    protected List<String> testClasses() {
        return List.of(
            ContainerAddressResolver.class.getName(),
            TestCompatibilityPluginProvider.class.getName(),
            DisabledRollingUpgradeProcessor.class.getName(),
            DisabledValidationProcessor.class.getName()
        );
    }

    /** @return Jar with the {@link #testClasses() test classes}, built once and reused for all containers. */
    protected File testClassesJar() throws IOException {
        File jar = testClassesJar;

        if (jar != null)
            return jar;

        synchronized (IgniteContainer.class) {
            if (testClassesJar != null)
                return testClassesJar;

            jar = File.createTempFile("test-classes", ".jar");
            jar.deleteOnExit();

            try (JarOutputStream out = new JarOutputStream(new FileOutputStream(jar))) {
                for (String cls : testClasses()) {
                    String clsPath = cls.replace('.', '/') + ".class";
                    String pkg = clsPath.substring(0, clsPath.lastIndexOf('/') + 1);
                    String simple = cls.substring(cls.lastIndexOf('.') + 1);

                    // Include the class and its nested classes (e.g. the provider's anonymous $1).
                    for (String resName : classResources(clsPath, pkg + simple + "$")) {
                        URL url = IgniteContainer.class.getClassLoader().getResource(resName);

                        if (url == null)
                            throw new IOException("Class not found on classpath: " + resName);

                        out.putNextEntry(new JarEntry(resName));

                        try (InputStream in = url.openStream()) {
                            in.transferTo(out);
                        }

                        out.closeEntry();
                    }
                }
            }

            return testClassesJar = jar;
        }
    }

    /**
     * Resolves the fully qualified resources (the top-level class plus its nested classes, e.g. {@code Outer$1})
     * for a class located either on the file system or inside a jar on the classpath.
     *
     * @param clsPath Resource path of the top-level class (package separator replaced with '/', ending in {@code .class}).
     * @param nestedPrefix Package-based prefix of the nested classes, e.g. {@code org/apache/foo/Simple$}.
     * @return Resource names of the class and its nested classes.
     */
    private static Collection<String> classResources(String clsPath, String nestedPrefix) throws IOException {
        URL url = IgniteContainer.class.getClassLoader().getResource(clsPath);

        if (url == null)
            throw new IOException("Class not found on classpath: " + clsPath);

        List<String> res = new ArrayList<>();

        res.add(clsPath);

        try {
            if ("file".equals(url.getProtocol())) {
                File dir = new File(url.toURI()).getParentFile();

                String pkg = clsPath.substring(0, clsPath.lastIndexOf('/') + 1);
                String simple = clsPath.substring(clsPath.lastIndexOf('/') + 1, clsPath.length() - ".class".length());

                File[] clsFiles = dir.listFiles((d, name) ->
                    name.equals(simple + ".class") || name.startsWith(simple + '$'));

                if (clsFiles == null)
                    throw new IOException("Cannot list class directory: " + dir);

                for (File f : clsFiles)
                    res.add(pkg + f.getName());
            }
            else if ("jar".equals(url.getProtocol())) {
                JarURLConnection conn = (JarURLConnection)url.openConnection();

                try (JarFile jar = conn.getJarFile()) {
                    Enumeration<JarEntry> entries = jar.entries();

                    while (entries.hasMoreElements()) {
                        String name = entries.nextElement().getName();

                        if (name.startsWith(nestedPrefix) && name.endsWith(".class"))
                            res.add(name);
                    }
                }
            }
            else
                throw new IOException("Unsupported class resource protocol: " + url.getProtocol());
        }
        catch (URISyntaxException e) {
            throw new IOException(e);
        }

        return res;
    }

    /**
     * Returns a cached tar archive (plain, no gzip) containing all files from {@link #TARGET_LIBS_DIR}
     * plus the test-classes jar. Built once and reused for all container upgrades.
     *
     * @return Path to the tar file on the host.
     */
    protected Path libsArchive() throws IOException {
        Path archive = targetLibsArchive;

        if (archive != null)
            return archive;

        synchronized (IgniteContainer.class) {
            if (targetLibsArchive != null)
                return targetLibsArchive;

            File targetLibsFile = TARGET_LIBS_DIR.toFile();

            LOGGER.info("Building libs archive from: {}", TARGET_LIBS_DIR);

            File archiveFile = File.createTempFile("target-libs", ".tar");
            archiveFile.deleteOnExit();

            try (TarArchiveOutputStream taos = new TarArchiveOutputStream(new FileOutputStream(archiveFile))) {
                taos.setLongFileMode(TarArchiveOutputStream.LONGFILE_POSIX);
                taos.setBigNumberMode(TarArchiveOutputStream.BIGNUMBER_STAR);

                List<File> allFiles = new ArrayList<>();

                collectFiles(targetLibsFile, allFiles);

                LOGGER.info("Found {} entries in {}", allFiles.size(), TARGET_LIBS_DIR);

                for (File srcFile : allFiles) {
                    String entryName = TARGET_LIBS_DIR.relativize(srcFile.toPath()).toString();

                    if (entryName.isEmpty())
                        continue;

                    TarArchiveEntry entry = new TarArchiveEntry(srcFile, entryName);
                    taos.putArchiveEntry(entry);

                    if (entry.isFile())
                        Files.copy(srcFile.toPath(), taos);

                    taos.closeArchiveEntry();
                }

                File testJar = testClassesJar();

                taos.putArchiveEntry(new TarArchiveEntry(testJar, "test-classes.jar"));

                Files.copy(testJar.toPath(), taos);

                taos.closeArchiveEntry();
            }

            LOGGER.info("Libs archive built: {} ({} bytes)", archiveFile, archiveFile.length());

            return targetLibsArchive = archiveFile.toPath();
        }
    }

    /** Recursively collects all files and directories under {@code root} into {@code result}. */
    private static void collectFiles(File root, List<File> result) {
        File[] children = root.listFiles();

        if (children == null)
            return;

        for (File child : children) {
            result.add(child);

            if (child.isDirectory())
                collectFiles(child, result);
        }
    }

    /** Returns the "uid:gid" of the current host user, cached after the first call. */
    private static String hostUserUidGid() throws IOException, InterruptedException {
        String cached = hostUidGid;

        if (cached != null)
            return cached;

        synchronized (IgniteContainer.class) {
            if (hostUidGid != null)
                return hostUidGid;

            ProcessBuilder pb = new ProcessBuilder("sh", "-c", "echo $(id -u):$(id -g)");
            pb.redirectErrorStream(true);

            Process p = pb.start();
            String resultLine;

            try (BufferedReader r = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
                resultLine = r.readLine();
            }

            if (!p.waitFor(5, TimeUnit.SECONDS)) {
                p.destroyForcibly();

                throw new IOException("Timed out waiting for id command");
            }

            if (p.exitValue() != 0)
                throw new IOException("Command 'id' failed with exit code: " + p.exitValue() + ". Output: " + resultLine);

            if (resultLine == null || resultLine.trim().isEmpty())
                throw new IOException("Empty output from 'id' command");

            return hostUidGid = resultLine.trim();
        }
    }

    /**
     * Stop the container gracefully <b>without removing it</b> (container stays in "Exited" state).
     *
     * <p>Sends SIGTERM via {@code docker stop} and waits up to {@value #SHUTDOWN_TIMEOUT_SEC} seconds
     * before SIGKILL. The entrypoint {@code run.sh} uses {@code exec java}, so JVM receives SIGTERM directly
     * and runs its shutdown hooks. The extended timeout ensures Ignite can leave the cluster properly
     * (flush persistence, notify discovery neighbors, close socket connections) so that remaining nodes
     * don't trigger spurious "Failed to check connection to previous node" warnings during teardown.</p>
     */
    protected void stopGraceful() {
        if (!isRunning())
            return;

        LOGGER.info("Graceful stop of node {} (timeout {} s)", hostname, SHUTDOWN_TIMEOUT_SEC);

        try (StopContainerCmd cmd = getDockerClient().stopContainerCmd(getContainerId())) {
            cmd.withTimeout(SHUTDOWN_TIMEOUT_SEC).exec();
        }
        catch (Exception e) {
            LOGGER.warn("Graceful stop (SIGTERM) failed for node {}, attempting SIGKILL fallback. Error: {}",
                hostname, e.getMessage());

            try {
                getDockerClient().killContainerCmd(getContainerId()).exec();
            }
            catch (Exception killEx) {
                LOGGER.error("SIGKILL fallback also failed for node {}", hostname, killEx);
            }
        }

        LOGGER.info("Node {} stopped", hostname);
    }

    /** @return Address the host JVM uses to reach this container's {@code port}. */
    protected String address(int port) {
        return getHost() + ":" + getMappedPort(port);
    }

    /** @return This container's attachment to the single test Docker network. */
    protected ContainerNetwork network() {
        return getContainerInfo().getNetworkSettings().getNetworks().values().iterator().next();
    }
}

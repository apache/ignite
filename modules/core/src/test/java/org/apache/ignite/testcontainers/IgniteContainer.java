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

package org.apache.ignite.testcontainers;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import com.github.dockerjava.api.command.InspectContainerResponse;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;
import static org.testcontainers.utility.MountableFile.forClasspathResource;

/** Ignite container. */
public class IgniteContainer extends GenericContainer<IgniteContainer> {
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger(IgniteContainer.class);

    /** Docker image name. */
    private static final DockerImageName DOCKER_IMAGE_NAME = DockerImageName.parse("apacheignite/ignite:2.18.0");

    /** Work directory. */
    private static final String WORK_DIR = "/opt/ignite/apache-ignite/";

    /** Libs directory in container. */
    private static final File LIBS_DIR = new File(WORK_DIR + "libs");

    /** Target libs directory in container. */
    private static final File TARGET_LIBS_DIR = new File("/opt/ignite/target-libs");

    /** Local target libs directory to copy in container. */
    private static final String LOCAL_TARGET_LIBS_DIR = System.getProperty("local.target.libs", "/tmp/target-libs");

    /** */
    private static final String CFG_PATH = WORK_DIR + "config/test-config.xml";

    /** */
    private static final String ENABLE_EXPERIMENTAL_FLAG = "--enable-experimental";

    /** */
    private static final Pattern CLUSTER_STATE_PATTERN = Pattern.compile("Cluster state: (ACTIVE|INACTIVE)");

    /** */
    private static final Pattern RU_STATUS_PATTERN = Pattern.compile("Rolling upgrade status: (enabled|disabled)");

    /** */
    private static final String WRAPPER_SCRIPT = "/opt/ignite/run-wrapper.sh";

    /** Default thin client port. */
    private static final int THIN_CLIENT_PORT = 10800;

    /** Ignite thin client. */
    private IgniteClient client;

    /** Constructor. */
    public IgniteContainer() {
        this(Network.newNetwork(), "node0", UUID.randomUUID().toString());
    }

    /** Constructor. */
    public IgniteContainer(Network net, String hostname, String nodeId) {
        super(DOCKER_IMAGE_NAME);

        withEnv("CONFIG_URI", "file://" + CFG_PATH);
        withEnv("IGNITE_QUIET", "false");
        withEnv("IGNITE_NODE_NAME", nodeId);
        withEnv("TZ", ZoneId.systemDefault().toString());
        //withEnv("JVM_OPTS", String.format("-Xmx%s", heapSize));

        withCopyFileToContainer(forClasspathResource("docker/test-config.xml"), CFG_PATH);
        withCopyFileToContainer(forClasspathResource("docker/run-wrapper.sh"), WRAPPER_SCRIPT);
        withCopyToContainer(MountableFile.forHostPath(LOCAL_TARGET_LIBS_DIR), TARGET_LIBS_DIR.getAbsolutePath());
        withNetwork(net);
        withNetworkAliases(hostname);
        withExposedPorts(THIN_CLIENT_PORT);

        withCommand("sh", "-c", "chmod +x " + WRAPPER_SCRIPT + " && exec " + WRAPPER_SCRIPT);

        waitingFor(Wait.forLogMessage(".*Node started.*", 1)
            .withStartupTimeout(Duration.ofSeconds(60)));
    }

    /** @return Thin client instance. */
    public IgniteClient client() {
        if (client == null)
            client = Ignition.startClient(clientConfig());

        return client;
    }

    /** */
    public void activateCluster() {
        execControl("--set-state", "ACTIVE", "--yes");

        try {
            waitForCondition(() -> {
                String out = execControl("--state");

                Matcher matcher = CLUSTER_STATE_PATTERN.matcher(out);

                if (matcher.find())
                    return ClusterState.valueOf(matcher.group(1)) == ClusterState.ACTIVE;

                return false;
            }, 30_000);
        }
        catch (IgniteInterruptedCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /**
     * 1. Stop ignite node.
     * 2. Remove current libs dir.
     * 3. Upgrade JAR's (copy libs from target dir).
     * 4. Start ignite node.
     */
    public void upgrade() throws Exception {
        LOGGER.info(">>> Upgrade container {}", getContainerName());

        closeClient();

        exec("Failed to kill Ignite process", "kill", "-INT", "1");

        for (int i = 0; i < 30; i++) {
            ExecResult res = execInContainer("pgrep", "-f", "org.apache.ignite.startup.cmdline.CommandLineStartup");

            if (res.getExitCode() == 1)
                break;

            U.sleep(1_000);
        }

        exec("Failed to remove old libs", "sh", "-c", "rm -rf " + LIBS_DIR + "/*");

        exec("Failed to copy new libs", "sh", "-c", "cp -r " + TARGET_LIBS_DIR + "/* " + LIBS_DIR + "/");

        execInContainer("sh", "-c", WORK_DIR + "run.sh &");

        waitForCondition(() -> {
            try {
                return client().cluster().nodes().size() == 3;
            }
            catch (Exception e) {
                return false;
            }
        }, 30_000);
    }

    /** */
    public RollingUpgradeStatus rollingUpgradeStatus() {
        String out = execControl("--rolling-upgrade", "status");

        LOGGER.info(">>> Rolling upgrade status: {}", out);

        Matcher matcher = RU_STATUS_PATTERN.matcher(out);

        if (matcher.find())
            return RollingUpgradeStatus.valueOf(matcher.group(1).toUpperCase());

        throw new IllegalStateException("Failed to parse rolling upgrade status from output:\n" + out);
    }

    /** */
    public void rollingUpgradeEnable(String targetVer) {
        execControl("--rolling-upgrade", "enable", targetVer, "--yes");
    }

    /** */
    public void rollingUpgradeDisable() {
        execControl("--rolling-upgrade", "disable");
    }

    /** */
    public int nodesCountForVersion(String targetVer) {
        String out = execControl("--rolling-upgrade", "status");

        // Match the version block
        Pattern verPattern = Pattern.compile(
            "Version\\s+" + Pattern.quote(targetVer) + ".*?:\\s*\n" +
                "((?:\\s*Node\\[.*?\\]\\s*\n)*)",
            Pattern.DOTALL
        );

        Matcher verMatcher = verPattern.matcher(out);

        if (!verMatcher.find())
            return 0;

        String nodesBlock = verMatcher.group(1);

        // Count Node entries
        Pattern nodePattern = Pattern.compile("^\\s*Node\\[.*?\\]", Pattern.MULTILINE);
        Matcher nodeMatcher = nodePattern.matcher(nodesBlock);

        int cnt = 0;

        while (nodeMatcher.find())
            cnt++;

        return cnt;
    }

    /** {@inheritDoc} */
    @Override protected void containerIsStopping(InspectContainerResponse containerInfo) {
        closeClient();
    }

    /** */
    private String execControl(String... cmd) {
        String[] fullCmd = new String[cmd.length + 2];

        fullCmd[0] = WORK_DIR + "bin/control.sh";
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

    /** */
    private ExecResult exec(String errMsg, String... cmd) {
        try {
            ExecResult res = execInContainer(cmd);

            if (res.getExitCode() != 0)
                throw new IllegalStateException(errMsg + ": " + res.getStderr());

            return res;
        }
        catch (IOException | InterruptedException e) {
            throw new IgniteException(e);
        }
    }

    /** */
    private void closeClient() {
        if (client != null) {
            client.close();

            client = null;
        }
    }

    /** */
    private ClientConfiguration clientConfig() {
        return new ClientConfiguration()
            .setAddresses("127.0.0.1:" + getMappedPort(THIN_CLIENT_PORT))
            .setRequestTimeout(30_000);
    }

    /** */
    public enum RollingUpgradeStatus {
        /** */
        ENABLED,

        /** */
        DISABLED
    }
}

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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;
import static org.testcontainers.utility.MountableFile.forClasspathResource;

/** Ignite container. */
public class IgniteContainer extends GenericContainer<IgniteContainer> {
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
    private static final String CFG_PATH = ROOT_DIR_PATH + "config/test-config.xml";

    /** */
    private static final String ENABLE_EXPERIMENTAL_FLAG = "--enable-experimental";

    /** */
    private static final Pattern CLUSTER_STATE_PATTERN = Pattern.compile("Cluster state: (ACTIVE|INACTIVE)");

    /** Default thin client port. */
    private static final int THIN_CLIENT_PORT = 10800;

    /** Hostname. */
    private final String hostname;

    /** Node ID. */
    private final String nodeId;

    /** Path to work directory. */
    private final String workDirPath;

    /** Constructor. */
    public IgniteContainer(String ver, Network net, String hostname, String nodeId) {
        super(DockerImageName.parse("apacheignite/ignite:" + ver));

        this.hostname = hostname;
        this.nodeId = nodeId;
        workDirPath = WORK_DIR_PATH + "/" + hostname;

        withEnv("CONFIG_URI", "file://" + CFG_PATH);
        withEnv("IGNITE_QUIET", "false");
        withEnv("IGNITE_NODE_NAME", nodeId);
        withEnv("IGNITE_WORK_DIR", workDirPath);
        withEnv("IGNITE_LOCAL_HOST", "0.0.0.0");
        withEnv("TZ", ZoneId.systemDefault().toString());

        withFileSystemBind(LOCAL_WORK_DIR_PATH, WORK_DIR_PATH, BindMode.READ_WRITE);

        withCopyFileToContainer(forClasspathResource("docker/test-config.xml"), CFG_PATH);

        // Copy compatibility JAR if it exists
        copyCompatibilityJar();

        withNetwork(net);
        withNetworkAliases(hostname);
        withExposedPorts(THIN_CLIENT_PORT, 47100, 47500);

        waitingFor(Wait.forLogMessage(".*Node started.*", 1)
            .withStartupTimeout(Duration.ofSeconds(60)));
    }

    /** */
    private void copyCompatibilityJar() {
        String projectDir = System.getProperty("user.dir");
        Path compatTarget = Paths.get(projectDir + "/target");

        System.out.println(">>> KEK=" + compatTarget);

        if (Files.exists(compatTarget)) {
            try {
                Files.walk(compatTarget)
                    .filter(p -> {
                        System.out.println(">>> P=" + p);

                        return p.toString().endsWith("tests.jar") && p.toString().contains("ignite-compatibility");
                    })
                    .findFirst()
                    .ifPresent(jarPath -> {
                        System.out.println(">>> JAR PATH=" + jarPath);

                        withCopyFileToContainer(MountableFile.forHostPath(jarPath), ROOT_DIR_PATH + "libs/test-compatability.jar");

                        LOGGER.info("Bound compatibility JAR: " + jarPath);
                    });
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        else
            throw new IllegalStateException("Please run `mvn clean install -DskipTests -pl modules/compatibility -am` first");
    }

    /** @return Hostname. */
    public String hostname() {
        return hostname;
    }

    /** @return Node ID. */
    public String nodeId() {
        return nodeId;
    }

    /** */
    public String localWorkDirectory() {
        return LOCAL_WORK_DIR_PATH + "/" + hostname;
    }

    /** Activate cluster. */
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

    /** @return Client address. */
    public String clientAddress() {
        return getHost() + ":" + getMappedPort(THIN_CLIENT_PORT);
    }

    /** */
    public String discoveryAddress() {
        // Возвращаем IPv4 адрес явно
        return "127.0.0.1:" + getMappedPort(47500);
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
}

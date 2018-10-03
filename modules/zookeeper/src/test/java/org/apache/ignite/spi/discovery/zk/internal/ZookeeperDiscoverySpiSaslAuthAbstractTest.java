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
package org.apache.ignite.spi.discovery.zk.internal;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;
import javax.security.auth.login.Configuration;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.zk.ZookeeperDiscoverySpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.zookeeper.client.ZooKeeperSaslClient;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZKDatabase;
import org.apache.zookeeper.server.ZooKeeperServer;

import static org.apache.curator.test.DirectoryUtils.deleteRecursively;

/**
 * Implements methods to prepare SASL tests infrastructure: jaas.conf files, starting up ZooKeeper server,
 * clean up procedures when the test has finished etc.
 */
public abstract class ZookeeperDiscoverySpiSaslAuthAbstractTest extends GridCommonAbstractTest {
    /** */
    private File tmpDir = createTmpDir();

    /** */
    private static final String JAAS_CONF_FILE = "jaas.conf";

    /** */
    private static final String AUTH_PROVIDER = "zookeeper.authProvider.1";

    /** */
    private static final String SASL_CONFIG = "java.security.auth.login.config";

    /** */
    private long joinTimeout = 2_000;

    /** */
    private long sesTimeout = 10_000;

    /** */
    private ServerCnxnFactory serverFactory;

    /** */
    private String hostPort = "localhost:2181";

    /** */
    private int maxCnxns;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String instanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(instanceName);

        ZookeeperDiscoverySpi zkSpi = new ZookeeperDiscoverySpi();

        if (joinTimeout != 0)
            zkSpi.setJoinTimeout(joinTimeout);

        zkSpi.setSessionTimeout(sesTimeout > 0 ? sesTimeout : 10_000);

        zkSpi.setZkConnectionString(hostPort);

        cfg.setDiscoverySpi(zkSpi);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        prepareJaasConfigFile();

        prepareSaslSystemProperties();

        startZooKeeperServer();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopZooKeeperServer();

        stopAllGrids();

        clearSaslSystemProperties();

        clearTmpDir();
    }

    /** */
    private void clearTmpDir() throws Exception {
        deleteRecursively(tmpDir);
    }

    /** */
    protected void clearSaslSystemProperties() {
        resetSaslStaticFields();

        System.clearProperty(AUTH_PROVIDER);

        System.clearProperty(SASL_CONFIG);

        System.clearProperty(ZooKeeperSaslClient.LOGIN_CONTEXT_NAME_KEY);
    }

    /**
     * @throws Exception If failed.
     */
    private void prepareJaasConfigFile() throws Exception {
        U.ensureDirectory(tmpDir, "Temp directory for JAAS configuration file wasn't created", null);

        File saslConfFile = new File(tmpDir, JAAS_CONF_FILE);

        FileWriter fwriter = new FileWriter(saslConfFile);

        writeServerConfigSection(fwriter, "validPassword");

        writeClientConfigSection(fwriter, "ValidZookeeperClient", "validPassword");

        writeClientConfigSection(fwriter, "InvalidZookeeperClient", "invalidPassword");

        fwriter.close();
    }

    /** */
    private void prepareSaslSystemProperties() {
        resetSaslStaticFields();

        System.setProperty(SASL_CONFIG, Paths.get(tmpDir.getPath().toString(), JAAS_CONF_FILE).toString());

        System.setProperty(AUTH_PROVIDER, "org.apache.zookeeper.server.auth.SASLAuthenticationProvider");
    }

    /** */
    private void resetSaslStaticFields() {
        Configuration.setConfiguration(null);

        GridTestUtils.setFieldValue(ZooKeeperSaslClient.class, "initializedLogin", false);
        GridTestUtils.setFieldValue(ZooKeeperSaslClient.class, "login", null);
    }

    /** */
    private void writeClientConfigSection(FileWriter fwriter, String clientName, String pass) throws IOException {
        fwriter.write(clientName + "{\n" +
            "       org.apache.zookeeper.server.auth.DigestLoginModule required\n" +
            "       username=\"zkUser\"\n" +
            "       password=\"" + pass + "\";\n" +
            "};" + "\n");
    }

    /** */
    private void writeServerConfigSection(FileWriter fwriter, String pass) throws IOException {
        fwriter.write("Server {\n" +
            "          org.apache.zookeeper.server.auth.DigestLoginModule required\n" +
            "          user_zkUser=\"" + pass + "\";\n" +
            "};\n");
    }

    /** */
    private File createTmpDir() {
        File jaasConfDir = Paths.get(System.getProperty("java.io.tmpdir"), "zk_disco_spi_test").toFile();

        try {
            U.ensureDirectory(jaasConfDir, "", null);
        }
        catch (IgniteCheckedException e) {
            // ignored
        }

        return jaasConfDir;
    }

    /** */
    private void stopZooKeeperServer() throws Exception {
        shutdownServerInstance(serverFactory);
        serverFactory = null;
    }

    /** */
    private void shutdownServerInstance(ServerCnxnFactory factory)
    {
        if (factory != null) {
            ZKDatabase zkDb = null;
            {
                ZooKeeperServer zs = getServer(factory);
                if (zs != null)
                    zkDb = zs.getZKDatabase();
            }
            factory.shutdown();
            try {
                if (zkDb != null)
                    zkDb.close();
            } catch (IOException ie) {
                // ignore
            }
        }
    }

    /** */
    private ZooKeeperServer getServer(ServerCnxnFactory fac) {
        ZooKeeperServer zs = U.field(fac, "zkServer");

        return zs;
    }

    /** */
    private void startZooKeeperServer() throws Exception {
        serverFactory = createNewServerInstance(serverFactory, hostPort,
            maxCnxns);
        startServerInstance(tmpDir, serverFactory);
    }

    /** */
    private ServerCnxnFactory createNewServerInstance(
        ServerCnxnFactory factory, String hostPort, int maxCnxns)
        throws IOException {
        final int port = getPort(hostPort);

        if (factory == null)
            factory = ServerCnxnFactory.createFactory(port, maxCnxns);

        return factory;
    }

    /** */
    private void startServerInstance(File dataDir,
        ServerCnxnFactory factory) throws IOException,
        InterruptedException {
        ZooKeeperServer zks = new ZooKeeperServer(dataDir, dataDir, 3000);
        factory.startup(zks);
    }

    /** */
    private int getPort(String hostPort) {
        String[] split = hostPort.split(":");
        String portstr = split[split.length-1];
        String[] pc = portstr.split("/");

        if (pc.length > 1)
            portstr = pc[0];

        return Integer.parseInt(portstr);
    }
}

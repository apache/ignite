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

package org.apache.ignite.spi.discovery.zk;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import junit.framework.TestSuite;
import org.apache.curator.test.InstanceSpec;
import org.apache.ignite.spi.discovery.tcp.ipfinder.zk.curator.TestingCluster;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.DiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.config.GridTestProperties;

/**
 * Allows to run regular Ignite tests with {@link org.apache.ignite.spi.discovery.zk.ZookeeperDiscoverySpi}.
 */
public abstract class ZookeeperDiscoverySpiAbstractTestSuite extends TestSuite {
    /** */
    private static TestingCluster testingCluster;

    /**
     * @throws Exception If failed.
     */
    public static void initSuite() throws Exception {
        System.setProperty("zookeeper.forceSync", "false");

        testingCluster = createTestingCluster(3);

        testingCluster.start();

        System.setProperty(GridTestProperties.IGNITE_CFG_PREPROCESSOR_CLS, ZookeeperDiscoverySpiAbstractTestSuite.class.getName());
    }

    /**
     * Called via reflection by {@link org.apache.ignite.testframework.junits.GridAbstractTest}.
     *
     * @param cfg Configuration to change.
     */
    public synchronized static void preprocessConfiguration(IgniteConfiguration cfg) {
        if (testingCluster == null)
            throw new IllegalStateException("Test Zookeeper cluster is not started.");

        ZookeeperDiscoverySpi zkSpi = new ZookeeperDiscoverySpi();

        DiscoverySpi spi = cfg.getDiscoverySpi();

        if (spi instanceof TcpDiscoverySpi)
            zkSpi.setClientReconnectDisabled(((TcpDiscoverySpi)spi).isClientReconnectDisabled());

        zkSpi.setSessionTimeout(30_000);
        zkSpi.setZkConnectionString(testingCluster.getConnectString());

        cfg.setDiscoverySpi(zkSpi);
    }

    /**
     * @param instances Number of instances in
     * @return Test cluster.
     */
    public static TestingCluster createTestingCluster(int instances) {
        String tmpDir;

        if (System.getenv("TMPFS_ROOT") != null)
            tmpDir = System.getenv("TMPFS_ROOT");
        else
            tmpDir = System.getProperty("java.io.tmpdir");

        List<InstanceSpec> specs = new ArrayList<>();

        for (int i = 0; i < instances; i++) {
            File file = new File(tmpDir, "apacheIgniteTestZk-" + i);

            if (file.isDirectory())
                deleteRecursively0(file);
            else {
                if (!file.mkdirs())
                    throw new IgniteException("Failed to create directory for test Zookeeper server: " + file.getAbsolutePath());
            }

            specs.add(new InstanceSpec(file, -1, -1, -1, true, -1, -1, 500));
        }

        return new TestingCluster(specs);
    }

    /**
     * @param file File or directory to delete.
     */
    private static void deleteRecursively0(File file) {
        File[] files = file.listFiles();

        if (files == null)
            return;

        for (File f : files) {
            if (f.isDirectory())
                deleteRecursively0(f);
            else {
                if (!f.delete())
                    throw new IgniteException("Failed to delete file: " + f.getAbsolutePath());
            }
        }
    }

}

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

package org.apache.ignite.loadtests.job;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.context.support.FileSystemXmlApplicationContext;

/**
 * Tests task/job behavior under load.
 */
public class GridJobLoadTest implements Runnable {
    /** Test configuration directory. */
    private static final File TEST_CONF_DIR;

    /**
     *
     */
    static {
        try {
            TEST_CONF_DIR = new File(U.resolveIgniteUrl("/modules/core/src/test/config/job-loadtest").toURI());
        }
        catch (URISyntaxException e) {
            throw new RuntimeException("Failed to initialize directory.", e);
        }
    }

    /** File with test configuration. */
    private static final File TEST_CONFIGURATION_FILE = new File(TEST_CONF_DIR, "job-loadtest.properties");

    /** File with client nodes configuration. */
    private static final File CLIENT_NODE_CONFIGURATION = new File(TEST_CONF_DIR, "client.xml");

    /** File with server nodes configuration. */
    private static final File SERVER_NODE_CONFIGURATION = new File(TEST_CONF_DIR, "server.xml");

    /**
     * Main method.
     *
     * @param args Command-line parameters.
     * @throws Exception if test start failed.
     */
    public static void main(String[] args) throws Exception {
        new GridJobLoadTest().run();
    }

    /** Number of client nodes to run. */
    private int clientNodes;

    /** Number of server nodes to run. */
    private int srvNodes;

    /** Number of submitter threads per client node. */
    private int threadsPerClient;

    /** Parameters for executing jobs. */
    private GridJobLoadTestParams taskParams;

    /** Submission/cancel ratio for submitting threads. */
    private int cancelRate;

    /** Time to sleep between task submissions. */
    private long submitDelay;

    /** Number of nodes running inside this test. */
    private int runningNodes;

    /**
     * Generate new node number.
     *
     * @return a client number unique within this test run.
     */
    private int getNextNodeNum() {
        return ++runningNodes;
    }

    /**
     * Loads test configuration.
     *
     * @throws Exception if configuration is unawailable or broken.
     */
    private void loadTestConfiguration() throws Exception {
        assert TEST_CONFIGURATION_FILE.isFile();

        InputStream in = null;

        Properties p = new Properties();

        try {
            in = new FileInputStream(TEST_CONFIGURATION_FILE);

            p.load(in);
        }
        finally {
            U.closeQuiet(in);
        }

        clientNodes = Integer.parseInt(p.getProperty("client.nodes.count"));
        srvNodes = Integer.parseInt(p.getProperty("server.nodes.count"));
        threadsPerClient = Integer.parseInt(p.getProperty("threads.per.client"));
        cancelRate = Integer.parseInt(p.getProperty("cancel.rate"));
        submitDelay = Long.parseLong(p.getProperty("submit.delay"));

        taskParams = new GridJobLoadTestParams(
            Integer.parseInt(p.getProperty("jobs.count")),
            Integer.parseInt(p.getProperty("jobs.test.duration")),
            Integer.parseInt(p.getProperty("jobs.test.completion.delay")),
            Double.parseDouble(p.getProperty("jobs.failure.probability"))
        );
    }

    /** {@inheritDoc} */
    @Override public void run() {
        List<Ignite> clientIgnites = runGrid();

        assert clientIgnites.size() == clientNodes;

        int threadsCnt = clientNodes * threadsPerClient;

        Executor e = Executors.newFixedThreadPool(threadsCnt);

        for (Ignite ignite : clientIgnites) {
            for (int j = 0; j < threadsPerClient; j++)
                e.execute(new GridJobLoadTestSubmitter(ignite, taskParams, cancelRate, submitDelay));
        }
    }

    /**
     * Run all grid nodes as defined in test configuration.
     *
     * @return list of run nodes.
     */
    private List<Ignite> runGrid() {
        List<Ignite> clientIgnites = new ArrayList<>(clientNodes);

        try {
            loadTestConfiguration();

            for (int i = 0; i < srvNodes; i++)
                startNode("server", SERVER_NODE_CONFIGURATION);

            // Start clients in the second order to cache a client node in Ignite.
            for (int i = 0; i < clientNodes; i++)
                clientIgnites.add(startNode("client", CLIENT_NODE_CONFIGURATION));
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }

        return clientIgnites;
    }

    /**
     * Starts new grid node.
     *
     * @param gridName name of new node.
     * @param springCfg file with spring configuration to use for this node.
     * @return a grid instance local to new node {@link org.apache.ignite.Ignition#start(org.apache.ignite.configuration.IgniteConfiguration)}.
     * @throws Exception if node run failed.
     */
    protected Ignite startNode(String gridName, File springCfg) throws Exception {
        assert springCfg != null;

        ListableBeanFactory springCtx = new FileSystemXmlApplicationContext(
                "file:///" + springCfg.getAbsolutePath());

        Map cfgMap = springCtx.getBeansOfType(IgniteConfiguration.class);

        assert cfgMap != null;
        assert !cfgMap.isEmpty();

        IgniteConfiguration cfg = (IgniteConfiguration)cfgMap.values().iterator().next();

        cfg.setGridName(gridName + "-" + getNextNodeNum());

        return G.start(cfg);
    }
}
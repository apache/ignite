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

package org.apache.ignite.internal.managers.deployment;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.ThinClientConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.client.thin.AbstractThinClientTest;
import org.apache.ignite.internal.client.thin.TestTask;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.testframework.GridTestUtils.waitForAllFutures;

/** */
public class GridDeploymentLocalStoreReuseTest extends AbstractThinClientTest {
    /** */
    private static final int NODE_CNT = 3;

    /** */
    private static final int CLIENT_CNT = 3;

    /** */
    protected static final int EXEC_CNT = 10;

    /** */
    private List<DeploymentListeningLogger> logs;

    /** */
    private List<IgniteClient> clients;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        DeploymentListeningLogger testLog = new DeploymentListeningLogger(log);
        logs.add(testLog);

        return super.getConfiguration(igniteInstanceName)
            .setClientConnectorConfiguration(
                new ClientConnectorConfiguration().setThinClientConfiguration(
                    new ThinClientConfiguration().setMaxActiveComputeTasksPerConnection(1000)))
            .setGridLogger(testLog)
            .setPeerClassLoadingEnabled(true);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        logs = new ArrayList<>(NODE_CNT);

        clients = new ArrayList<>(CLIENT_CNT);

        setLoggerDebugLevel();

        startGrids(NODE_CNT);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        clients.clear();

        super.afterTest();
    }

    /**
     * Verifies that multiple task executions do not cause excessive local deployment cache misses. The "deployment not
     * found ... clsLdrId=null" message is allowed only once per thin client (initial task execution).
     */
    @Test
    public void testNoExcessiveLocalDeployment() {
        try {
            ClusterNode[] allServerNodes = grid(0).cluster().forServers().nodes().toArray(new ClusterNode[0]);

            for (int i = 0; i < CLIENT_CNT; i++)
                clients.add(startClient(allServerNodes));

            List<IgniteInternalFuture<Void>> futs = new ArrayList<>(CLIENT_CNT);

            for (IgniteClient client : clients)
                futs.add(runAsync(() -> executeTasksOnClient(client, EXEC_CNT, 5_000L)));

            waitForAllFutures(futs.toArray(new IgniteInternalFuture[0]));

            List<String> allNotFound = new ArrayList<>();

            for (DeploymentListeningLogger log : logs)
                allNotFound.addAll(log.depNotFound());

            String taskClsName = TestTask.class.getName();

            String notFoundMsg = String.format(
                "Deployment was not found for class with specific class loader [alias=%s, clsLdrId=null]", taskClsName);

            assertEquals(CLIENT_CNT, Collections.frequency(allNotFound, notFoundMsg));
        }
        finally {
            clients.forEach(IgniteClient::close);
        }
    }

    /** */
    private static void executeTasksOnClient(IgniteClient client, int cnt, long timeout) {
        for (int i = 0; i < cnt; i++) {
            CompletableFuture<T2<UUID, Set<UUID>>> fut = client.compute()
                .withTimeout(timeout).
                    <T2<UUID, Set<UUID>>, T2<UUID, Set<UUID>>>executeAsync2(TestTask.class.getName(), null)
                .toCompletableFuture();

            try {
                fut.get();
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    /** */
    private static class DeploymentListeningLogger extends ListeningTestLogger {
        /** */
        private final ConcurrentLinkedQueue<String> depNotFound = new ConcurrentLinkedQueue<>();

        /** */
        public DeploymentListeningLogger(IgniteLogger log) {
            super(log);
        }

        /** {@inheritDoc} */
        @Override public void debug(String msg) {
            if (msg.contains("Deployment was not found for class with specific class loader"))
                depNotFound.add(msg);

            super.debug(msg);
        }

        /** {@inheritDoc} */
        @Override public ListeningTestLogger getLogger(Object ctgr) {
            return this;
        }

        /** */
        public List<String> depNotFound() {
            return depNotFound.stream().collect(Collectors.toUnmodifiableList());
        }
    }
}

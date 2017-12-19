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

package org.apache.ignite.internal;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.compute.ComputeTaskName;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.util.F0;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.resources.ServiceResource;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceContext;
import org.apache.ignite.spi.deployment.uri.UriDeploymentSpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;

/**
 * Test to reproduce IGNITE-6803.
 */
public class GridTaskUriDeploymentServiceDependencyTest extends GridCommonAbstractTest {

    /**
     * This class defines grid task for this example. Grid task is responsible for splitting the task into jobs. This
     * particular implementation splits given string into individual words and creates grid jobs for each word. Task class
     * in that example should be placed in GAR file.
     *
     * Calls service bean during operation.
     */
    @ComputeTaskName("LocalUseServiceTask")
    public static class LocalUseServiceTask extends ComputeTaskAdapter<String, String> {

        /**
         * Cluster singleton dependency.
         */
        @ServiceResource(serviceName = "runner", proxyInterface = Runnable.class)
        private Runnable runnable;

        /**
         * Creates task for every node.
         */
        @Override public Map<ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, String arg) {
            Map<ComputeJob, ClusterNode> result = new HashMap<>();
            for (ClusterNode node : subgrid) {
                result.put(new ComputeJobAdapter() {
                    /*
                     * Simply calls the service.
                     */
                    @Override public Serializable execute() {
                        runnable.run();

                        // This job does not return any result.
                        return null;
                    }
                }, node);
            }
            return result;
        }

        /** */
        @Override public String reduce(List<ComputeJobResult> results) {
            return "OK";
        }
    }

    /**
     * Service class
     */
    class Runner implements Service, Runnable {
        /** */
        @LoggerResource
        private IgniteLogger log;

        /** */
        private AtomicBoolean interrupt = new AtomicBoolean(false);

        /**
         * Service exposes external API method .
         */
        @Override public void run() {
            log.info("Runnable called from URI deployed task");
            System.err.println("Runnable called from URI deployed task");
        }

        /** */
        @Override public void cancel(ServiceContext ctx) {
            interrupt.set(true);
        }

        /** */
        @Override public void init(ServiceContext ctx) throws Exception {
            System.err.println("Service deployed");
        }

        /** */
        @Override public void execute(ServiceContext ctx) throws Exception {
            while (!interrupt.get()) {
                TimeUnit.SECONDS.sleep(10);
                String msg = "I'm service and I'm alive";
                log.debug(msg);
                System.err.println(msg);
            }
        }
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(igniteInstanceName);

        UriDeploymentSpi deploymentSpi = new UriDeploymentSpi();

        deploymentSpi.setUriList(
            Arrays.asList(U.resolveIgniteUrl("modules/extdata/uri/target/resources/").toURI().toString()));

        if (igniteInstanceName.endsWith("2")) {
            // Delay deployment for 2nd grid only.
            Field f = deploymentSpi.getClass().getDeclaredField("delayOnNewOrUpdatedFile");

            f.setAccessible(true);

            f.set(deploymentSpi, true);
        }

        c.setDeploymentSpi(deploymentSpi);

        return c;
    }

    /**
     * @throws Exception If failed.
     */
    public void testDelegatingTask() throws Exception {
        try {
            Ignite g = startGrid(1);

            for (int i = 2; i < 4; i++) {
                startGrid(i).compute().localDeployTask(
                    LocalUseServiceTask.class, LocalUseServiceTask.class.getClassLoader());
            }

            info(">>> Starting task.");

            g.services().deployClusterSingleton("runner", new Runner());
            g.compute().localDeployTask(LocalUseServiceTask.class, LocalUseServiceTask.class.getClassLoader());

            assert "OK".equals(executeAsync(g.compute(g.cluster().forPredicate(
                F0.equalTo(F.first(g.cluster().forRemotes().nodes())))),
                "GarDelegatingTask", "Delegate").get(60000));
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testTaskWithServiceLocalFirst() throws Exception {
        try {
            Ignite g = startGrid(1);

            for (int i = 2; i < 4; i++) {
                startGrid(i);
            }

            info(">>> Starting task.");

            g.services().deployClusterSingleton("runner", new Runner());
            g.compute().localDeployTask(LocalUseServiceTask.class, LocalUseServiceTask.class.getClassLoader());

            assert "OK".equals(executeAsync(g.compute(g.cluster().forPredicate(
                F0.equalTo(F.first(g.cluster().forRemotes().nodes())))),
                "LocalUseServiceTask", "Local").get(60000));

            assert "OK".equals(executeAsync(g.compute(g.cluster().forPredicate(
                F0.equalTo(F.first(g.cluster().forRemotes().nodes())))),
                "GarUseServiceTask", "Gar").get(60000));
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testTaskWithServiceRemoteFirst() throws Exception {
        try {
            Ignite g = startGrid(1);

            for (int i = 2; i < 4; i++) {
                startGrid(i);
            }

            info(">>> Starting task.");

            g.services().deployClusterSingleton("runner", new Runner());
            g.compute().localDeployTask(LocalUseServiceTask.class, LocalUseServiceTask.class.getClassLoader());

            assert "OK".equals(executeAsync(g.compute(g.cluster().forPredicate(
                F0.equalTo(F.first(g.cluster().forRemotes().nodes())))),
                "GarUseServiceTask", "Gar").get(60000));

            assert "OK".equals(executeAsync(g.compute(g.cluster().forPredicate(
                F0.equalTo(F.first(g.cluster().forRemotes().nodes())))),
                "LocalUseServiceTask", "Local").get(60000));
        }
        finally {
            stopAllGrids();
        }
    }
}

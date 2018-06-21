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

package org.apache.ignite.tensorflow.cluster;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.function.Supplier;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.tensorflow.cluster.tfrunning.TensorFlowServerManager;
import org.apache.ignite.tensorflow.core.longrunning.task.util.LongRunningProcessStatus;

/**
 * Prerequisites: be aware that to successfully run this example you need to have Python and TensorFlow installed on
 * your machine. To find out how to install Python please take a look this page: https://www.python.org/downloads/. To
 * install TensorFlow see this web site: https://www.tensorflow.org/install/.
 *
 * Example that shows how to use {@link TensorFlowClusterGatewayManager} and start, maintain and stop TensorFlow
 * cluster.
 */
public class TensorFlowClusterExample {
    /** Run example. */
    public static void main(String... args) throws InterruptedException {
        IgniteConfiguration configuration = new IgniteConfiguration();
        configuration.setClientMode(false);

        try (Ignite ignite = Ignition.start(configuration)) {
            System.out.println(">>> TensorFlow cluster example started.");

            CacheConfiguration<Integer, Integer> cacheConfiguration = new CacheConfiguration<>();
            cacheConfiguration.setAffinity(new RendezvousAffinityFunction(false, 10));
            cacheConfiguration.setName("TEST_CACHE");

            IgniteCache<Integer, Integer> cache = ignite.getOrCreateCache(cacheConfiguration);
            for (int i = 0; i < 1000; i++)
                cache.put(i, i);

            System.out.println(">>> Cache created.");

            TensorFlowClusterGatewayManager mgr = new TensorFlowClusterGatewayManager(ignite);
            TensorFlowClusterGateway gateway = mgr.getOrCreateCluster("TEST_CACHE");

            System.out.println(">>> TensorFlow cluster gateway started.");

            CountDownLatch latch = new CountDownLatch(1);

            gateway.subscribe(cluster -> {
                StringBuilder builder = new StringBuilder();
                builder.append("------------------- TensorFlow Cluster Service Info -------------------").append('\n');

                builder.append("Specification : ").append('\n');

                TensorFlowServerManager srvMgr = new TensorFlowServerManager(
                    (Supplier<Ignite> & Serializable)() -> ignite
                );

                String clusterSpec = srvMgr.formatClusterSpec(cluster.getSpec());
                builder.append(clusterSpec).append('\n');

                Map<UUID, List<LongRunningProcessStatus>> statuses = srvMgr.ping(cluster.getProcesses());

                builder.append("State : ").append('\n');

                for (UUID nodeId : cluster.getProcesses().keySet()) {
                    List<UUID> pr = cluster.getProcesses().get(nodeId);
                    List<LongRunningProcessStatus> st = statuses.get(nodeId);

                    builder.append("Node ").append(nodeId.toString().substring(0, 8)).append(" -> ").append('\n');
                    for (int i = 0; i < pr.size(); i++) {
                        builder.append("\tProcess ")
                            .append(pr.get(i).toString().substring(0, 8))
                            .append(" with status ")
                            .append(st.get(i).getState());

                        if (st.get(i).getException() != null)
                            builder.append(" (").append(st.get(i).getException()).append(")");

                        builder.append('\n');
                    }
                }

                builder.append("-----------------------------------------------------------------------").append('\n');

                System.out.println(builder);

                latch.countDown();
            });

            latch.await();

            mgr.stopClusterIfExists("TEST_CACHE");

            System.out.println(">>> TensorFlow cluster example completed.");
        }
    }
}

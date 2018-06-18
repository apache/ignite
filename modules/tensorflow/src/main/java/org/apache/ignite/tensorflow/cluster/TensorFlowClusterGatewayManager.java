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

import java.util.function.Supplier;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteServices;

/**
 * TensorFlow cluster manager that allows to start, maintain and stop TensorFlow cluster using
 * {@link TensorFlowClusterManager} and Ignite Service Grid.
 */
public class TensorFlowClusterGatewayManager {
    /** Service name template. */
    private static final String SERVICE_NAME_TEMPLATE = "TF_SERVICE_%s";

    /** Service topic name template. */
    private static final String SERVICE_TOPIC_NAME_TEMPLATE = "TF_SERVICE_TOPIC_%s";

    /** Ignite instance supplier. */
    private final Supplier<Ignite> igniteSupplier;

    /**
     * Constructs a new instance of TensorFlow cluster manager with maintenance.
     *
     * @param igniteSupplier Ignite instance supplier.
     */
    public TensorFlowClusterGatewayManager(Supplier<Ignite> igniteSupplier) {
        assert igniteSupplier != null : "Ignite supplier should not be null";

        this.igniteSupplier = igniteSupplier;
    }

    /** {@inheritDoc} */
    public TensorFlowClusterGateway getOrCreateCluster(String upstreamCacheName) {
        String svcName = String.format(SERVICE_NAME_TEMPLATE, upstreamCacheName);
        String topicName = String.format(SERVICE_TOPIC_NAME_TEMPLATE, upstreamCacheName);

        TensorFlowClusterGateway gateway = createTensorFlowClusterGateway(topicName);

        Ignite ignite = igniteSupplier.get();
        IgniteServices services = ignite.services();

        services.deployClusterSingleton(svcName, new TensorFlowClusterMaintainer(upstreamCacheName, topicName));

        return gateway;
    }

    /** {@inheritDoc} */
    public void stopClusterIfExists(String upstreamCacheName) {
        Ignite ignite = igniteSupplier.get();
        IgniteServices services = ignite.services();

        services.cancel(String.format(SERVICE_NAME_TEMPLATE, upstreamCacheName));
    }

    /**
     * Creates TensorFlow cluster gateway.
     *
     * @param topicName Topic name.
     * @return TensorFlow cluster gateway.
     */
    private TensorFlowClusterGateway createTensorFlowClusterGateway(String topicName) {
        TensorFlowClusterGateway gateway = new TensorFlowClusterGateway();

        Ignite ignite = igniteSupplier.get();
        ignite.message().localListen(topicName, gateway);

        return gateway;
    }
}

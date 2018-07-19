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

import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteLogger;

/**
 * TensorFlow cluster manager that allows to start, maintain and stop TensorFlow cluster using
 * {@link TensorFlowClusterManager} and Ignite Service Grid.
 */
public class TensorFlowClusterGatewayManager {
    /** Service name template. */
    private static final String SERVICE_NAME_TEMPLATE = "TF_SERVICE_%s";

    /** Service topic name template. */
    private static final String SERVICE_TOPIC_NAME_TEMPLATE = "TF_SERVICE_TOPIC_%s";

    /** Ignite instance. */
    private final Ignite ignite;

    /** Logger. */
    private final IgniteLogger log;

    /**
     * Constructs a new instance of TensorFlow cluster manager with maintenance.
     *
     * @param ignite Ignite instance.
     */
    public TensorFlowClusterGatewayManager(Ignite ignite) {
        assert ignite != null : "Ignite should not be null";

        this.ignite = ignite;
        this.log = ignite.log().getLogger(TensorFlowClusterGatewayManager.class);
    }

    /**
     * Creates and starts a new TensorFlow cluster for the specified cache if it doesn't exist, otherwise returns
     * existing one.
     *
     * @param clusterId Cluster identifier.
     * @param jobArchive Job archive.
     * @return TensorFlow cluster gateway that allows to subscribe on cluster changes.
     */
    public TensorFlowClusterGateway getOrCreateCluster(UUID clusterId, TensorFlowJobArchive jobArchive) {
        String svcName = String.format(SERVICE_NAME_TEMPLATE, clusterId);
        String topicName = String.format(SERVICE_TOPIC_NAME_TEMPLATE, clusterId);

        TensorFlowClusterGateway gateway = createTensorFlowClusterGateway(topicName);

        ignite.services().deployClusterSingleton(svcName, new TensorFlowClusterMaintainer(clusterId, jobArchive, topicName));
        log.info("Cluster maintainer deployed as a service [clusterId=" + clusterId + "]");

        return gateway;
    }

    /**
     * Stops TensorFlow cluster.
     *
     * @param clusterId Cluster identifier.
     */
    public void stopClusterIfExists(UUID clusterId) {
        ignite.services().cancel(String.format(SERVICE_NAME_TEMPLATE, clusterId));
        log.info("Cluster maintained cancelled as a service [clusterId=" + clusterId + "]");
    }

    /**
     * Creates TensorFlow cluster gateway.
     *
     * @param topicName Topic name.
     * @return TensorFlow cluster gateway.
     */
    private TensorFlowClusterGateway createTensorFlowClusterGateway(String topicName) {
        TensorFlowClusterGateway gateway = new TensorFlowClusterGateway(subscriber -> {
            ignite.message().stopLocalListen(topicName, subscriber);
            log.info("Stop listen to cluster gateway [topicName=" + topicName + "]");
        });

        ignite.message().localListen(topicName, gateway);
        log.info("Start listen to cluster gateway [topicName=" + topicName + "]");

        return gateway;
    }
}

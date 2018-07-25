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

package org.apache.ignite.tensorflow.submitter.command;

import java.util.UUID;
import java.util.function.Supplier;
import org.apache.ignite.Ignite;
import org.apache.ignite.tensorflow.cluster.TensorFlowClusterGatewayManager;

/**
 * Describe command that prints configuration of the specified TensorFlow cluster.
 */
public class AttachCommand implements Runnable {
    /** Ignite supplier. */
    private final Supplier<Ignite> igniteSupplier;

    /** Cluster identifier. */
    private final UUID clusterId;

    /**
     * Constructs a new instance of command "attach".
     *
     * @param igniteSupplier Ignite supplier.
     * @param clusterId Cluster identifier.
     */
    public AttachCommand(Supplier<Ignite> igniteSupplier, UUID clusterId) {
        this.igniteSupplier = igniteSupplier;
        this.clusterId = clusterId;
    }

    /** {@inheritDoc} */
    @Override public void run() {
        try (Ignite ignite = igniteSupplier.get()) {
            TensorFlowClusterGatewayManager mgr = new TensorFlowClusterGatewayManager(ignite);

            mgr.listenToClusterUserScript(clusterId, System.out::println, System.err::println);
        }
    }

    /** */
    public Supplier<Ignite> getIgniteSupplier() {
        return igniteSupplier;
    }

    /** */
    public UUID getClusterId() {
        return clusterId;
    }
}

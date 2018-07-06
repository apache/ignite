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

import java.util.HashSet;
import java.util.UUID;
import java.util.function.Consumer;
import org.apache.ignite.lang.IgniteBiPredicate;

/**
 * TensorFlow cluster gateway that allows to subscribe on changes in cluster configuration.
 */
public class TensorFlowClusterGateway implements IgniteBiPredicate<UUID, TensorFlowCluster> {
    /** */
    private static final long serialVersionUID = -540323262800791340L;

    /** Subscribers. */
    private final HashSet<Consumer<TensorFlowCluster>> subscribers = new HashSet<>();

    /** Last value received from the upstream. */
    private TensorFlowCluster last;

    /**
     * Subscribers the specified subscriber on the upstream events.
     *
     * @param subscriber Subscriber.
     */
    public synchronized void subscribe(Consumer<TensorFlowCluster> subscriber) {
        subscribers.add(subscriber);

        if (last != null)
            subscriber.accept(last);
    }

    /**
     * Unsubscribe the specified subscriber.
     *
     * @param subscriber Subscriber.
     */
    public synchronized void unsubscribe(Consumer<TensorFlowCluster> subscriber) {
        subscribers.remove(subscriber);
    }

    /** {@inheritDoc} */
    @Override public synchronized boolean apply(UUID uuid, TensorFlowCluster cluster) {
        for (Consumer<TensorFlowCluster> subscriber : subscribers)
            subscriber.accept(cluster);

        last = cluster;

        return true;
    }
}

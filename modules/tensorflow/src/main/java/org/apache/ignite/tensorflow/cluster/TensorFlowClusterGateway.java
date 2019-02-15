/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.tensorflow.cluster;

import java.util.HashSet;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.tensorflow.util.SerializableConsumer;

/**
 * TensorFlow cluster gateway that allows to subscribe on changes in cluster configuration.
 */
public class TensorFlowClusterGateway implements IgniteBiPredicate<UUID, Optional<TensorFlowCluster>>, AutoCloseable {
    /** */
    private static final long serialVersionUID = -540323262800791340L;

    /** Callback that will be called on unsubscribe. */
    private final SerializableConsumer<TensorFlowClusterGateway> unsubscribeCb;

    /** Subscribers. */
    private final HashSet<Consumer<Optional<TensorFlowCluster>>> subscribers = new HashSet<>();

    /** Last value received from the upstream. */
    private Optional<TensorFlowCluster> last;

    /**
     * Constructs a new instance of TensorFlow cluster gateway.
     *
     * @param unsubscribeCb Callback that will be called on unsubscribe.
     */
    public TensorFlowClusterGateway(SerializableConsumer<TensorFlowClusterGateway> unsubscribeCb) {
        this.unsubscribeCb = unsubscribeCb;
    }

    /**
     * Subscribers the specified subscriber on the upstream events.
     *
     * @param subscriber Subscriber.
     */
    public synchronized void subscribe(Consumer<Optional<TensorFlowCluster>> subscriber) {
        subscribers.add(subscriber);

        if (last != null)
            subscriber.accept(last);
    }

    /**
     * Unsubscribe the specified subscriber.
     *
     * @param subscriber Subscriber.
     */
    public synchronized void unsubscribe(Consumer<Optional<TensorFlowCluster>> subscriber) {
        subscribers.remove(subscriber);
    }

    /** {@inheritDoc} */
    @Override public synchronized boolean apply(UUID uuid, Optional<TensorFlowCluster> cluster) {
        for (Consumer<Optional<TensorFlowCluster>> subscriber : subscribers)
            subscriber.accept(cluster);

        last = cluster;

        return true;
    }

    /** {@inheritDoc} */
    @Override public void close() {
        subscribers.clear();
        unsubscribeCb.accept(this);
    }
}

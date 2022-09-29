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

package org.apache.ignite.stream;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.datastreamer.DataStreamerImpl;

/**
 * Updates cache with batch of entries. Usually it is enough to configure {@link IgniteDataStreamer#allowOverwrite(boolean)}
 * property and appropriate internal cache receiver will be chosen automatically. But in some cases to achieve best
 * performance custom user-defined implementation may help.
 * <p>
 * Data streamer can be configured to use custom implementation of the receiver instead of default one using
 * {@link IgniteDataStreamer#receiver(StreamReceiver)} method.
 */
public interface StreamReceiver<K, V> extends Serializable {
    /**
     * Updates cache with batch of entries.
     *
     * @param cache Cache.
     * @param entries Collection of entries.
     * @throws org.apache.ignite.IgniteException If failed.
     */
    public void receive(IgniteCache<K, V> cache, Collection<Map.Entry<K, V>> entries) throws IgniteException;

    /**
     * Sets max parallel batches (operations) per node if {@link IgniteDataStreamer#perNodeParallelOperations()} is not
     * positive. In same way takes place only if positive.
     *
     * @param node       Node to send to.
     * @param persistent {@code True} if the cache is persistent. {@code False} otherwise.
     *
     * @return Max parallel batches.
     * @see IgniteDataStreamer#perNodeParallelOperations(int)
     */
    public default int perNodeParallelOperations(ClusterNode node, boolean persistent) {
        return DataStreamerImpl.perNodeParallelOperations(node, persistent);
    }
}

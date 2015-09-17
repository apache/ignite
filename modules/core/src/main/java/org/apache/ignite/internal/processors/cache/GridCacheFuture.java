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

package org.apache.ignite.internal.processors.cache;

import java.util.Collection;
import java.util.UUID;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.lang.IgniteUuid;

/**
 * This interface should be implemented by all distributed futures.
 */
public interface GridCacheFuture<R> extends IgniteInternalFuture<R> {
    /**
     * @return Unique identifier for this future.
     */
    public IgniteUuid futureId();

    /**
     * @return Future version.
     */
    public GridCacheVersion version();

    /**
     * @return Involved nodes.
     */
    public Collection<? extends ClusterNode> nodes();

    /**
     * Callback for when node left.
     *
     * @param nodeId Left node ID.
     * @return {@code True} if future cared about this node.
     */
    public boolean onNodeLeft(UUID nodeId);

    /**
     * @return {@code True} if future should be tracked.
     */
    public boolean trackable();

    /**
     * Marks this future as non-trackable.
     */
    public void markNotTrackable();
}
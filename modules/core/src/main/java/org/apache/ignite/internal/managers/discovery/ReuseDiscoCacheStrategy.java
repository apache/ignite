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

package org.apache.ignite.internal.managers.discovery;

import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheAffinityChangeMessage;
import org.apache.ignite.internal.processors.cache.DynamicCacheChangeBatch;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotDiscoveryMessage;
import org.apache.ignite.internal.processors.cluster.ChangeGlobalStateMessage;

/**
 * Defines messages which are willing to reuse discovery cache for efficiency reasons.
 */
public interface ReuseDiscoCacheStrategy {
    /**
     * Create discovery cache for {@link DynamicCacheChangeBatch} message.
     *
     * @param msg Message.
     * @param topVer Topology version.
     * @param discoCache Disco cache.
     */
    public DiscoCache apply(DynamicCacheChangeBatch msg, AffinityTopologyVersion topVer, DiscoCache discoCache);

    /**
     * Create discovery cache for {@link CacheAffinityChangeMessage} message.
     *
     * @param msg Message.
     * @param topVer Topology version.
     * @param discoCache Disco cache.
     */
    public DiscoCache apply(CacheAffinityChangeMessage msg, AffinityTopologyVersion topVer, DiscoCache discoCache);

    /**
     * Create discovery cache for {@link SnapshotDiscoveryMessage} message.
     *
     * @param msg Message.
     * @param topVer Topology version.
     * @param discoCache Disco cache.
     */
    public DiscoCache apply(SnapshotDiscoveryMessage msg, AffinityTopologyVersion topVer, DiscoCache discoCache);

    /**
     * Create discovery cache for {@link ChangeGlobalStateMessage} message.
     *
     * @param msg Message.
     * @param topVer Topology version.
     * @param discoCache Disco cache.
     */
    public DiscoCache apply(ChangeGlobalStateMessage msg, AffinityTopologyVersion topVer, DiscoCache discoCache);
}

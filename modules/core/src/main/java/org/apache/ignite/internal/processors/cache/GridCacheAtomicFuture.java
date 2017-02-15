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

import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;

/**
 * Update future for atomic cache.
 */
public interface GridCacheAtomicFuture<R> extends GridCacheFuture<R> {
    /**
     * @return Future version.
     */
    public GridCacheVersion version();

    /**
     * Gets future that will be completed when it is safe when update is finished on the given version of topology.
     *
     * @param topVer Topology version to finish.
     * @return Future or {@code null} if no need to wait.
     */
    public IgniteInternalFuture<Void> completeFuture(AffinityTopologyVersion topVer);
}

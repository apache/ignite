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

package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import java.util.Set;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.GridCacheSharedManager;
import org.apache.ignite.internal.processors.cluster.IgniteChangeGlobalStateSupport;

/** */
public interface SnapshotPageStoreManager<T> extends GridCacheSharedManager, IgniteChangeGlobalStateSupport {
    /**
     * @param idx Unique process identifier.
     * @param grpId Tracked cache group id.
     * @param parts Tracking cache group partitions during snapshot.
     * @param hndlr Handler for processing partitions and corresponding partition deltas.
     * @return Future with processing result.
     */
    public IgniteInternalFuture<Boolean> snapshot(
        int idx,
        int grpId,
        Set<Integer> parts,
        SnapshotProcessHandler<T> hndlr
    );
}

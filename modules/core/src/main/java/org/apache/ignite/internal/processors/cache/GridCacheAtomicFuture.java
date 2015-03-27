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

import org.apache.ignite.internal.processors.affinity.*;

import java.util.*;

/**
 * Update future for atomic cache.
 */
public interface GridCacheAtomicFuture<R> extends GridCacheFuture<R> {
    /**
     * @return {@code True} if partition exchange should wait for this future to complete.
     */
    public boolean waitForPartitionExchange();

    /**
     * @return Future topology version.
     */
    public AffinityTopologyVersion topologyVersion();

    /**
     * @return Future keys.
     */
    public Collection<?> keys();

    /**
     * Checks if timeout occurred.
     *
     * @param timeout Timeout to check.
     */
    public void checkTimeout(long timeout);
}

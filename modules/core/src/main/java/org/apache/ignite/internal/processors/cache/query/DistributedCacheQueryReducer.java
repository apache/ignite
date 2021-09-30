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

package org.apache.ignite.internal.processors.cache.query;

import java.util.UUID;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;

/**
 * Reducer for distributed cache query.
 */
public abstract class DistributedCacheQueryReducer<T> extends CacheQueryReducer<T> {
    /**
     * Checks whether node with provided {@code nodeId} is a map node for the query.
     * Note: if all pages were received from this node, then the method will return {@code false}.
     *
     * @param nodeId Node ID.
     * @return {@code true} if node with provided {@code nodeId} is a map node for the query, {@code false} otherwise.
     */
    public abstract boolean remoteQueryNode(UUID nodeId);

    /**
     * Blocks current thread until reducer will be ready to return the very first result item for the query.
     */
    public abstract void awaitInitialization() throws IgniteInterruptedCheckedException;

    /**
     * Cancel cache query and stop reduce pages.
     */
    public abstract void cancel();
}

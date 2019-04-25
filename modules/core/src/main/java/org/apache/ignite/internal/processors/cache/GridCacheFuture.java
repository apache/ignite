/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache;

import java.util.UUID;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.lang.IgniteUuid;

/**
 * This interface should be implemented by all distributed futures.
 */
public interface GridCacheFuture<R> extends IgniteInternalFuture<R> {
    /**
     * Gets start time for this future.
     *
     * @return Start time for this future.
     */
    public long startTime();

    /**
     * Gets duration in milliseconds between start of the future and current time if future
     * is not finished, or between start and finish of this future.
     *
     * @return Time in milliseconds this future has taken to execute.
     */
    public long duration();

    /**
     * @return Unique identifier for this future.
     */
    public IgniteUuid futureId();

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
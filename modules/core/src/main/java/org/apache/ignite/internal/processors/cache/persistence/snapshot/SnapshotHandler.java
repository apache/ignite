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

import java.util.Collection;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.plugin.Extension;
import org.jetbrains.annotations.Nullable;

/**
 * Snapshot operation handler.
 *
 * @param <T> Type of the local processing result.
 */
public interface SnapshotHandler<T> extends Extension {
    /** Snapshot handler type. */
    public SnapshotHandlerType type();

    /**
     * Local processing of a snapshot operation.
     * Called on every node that contains snapshot data.
     *
     * @param ctx Snapshot handler context.
     * @return Result of local processing.
     * @throws IgniteCheckedException If failed.
     */
    public @Nullable T invoke(SnapshotHandlerContext ctx) throws IgniteCheckedException;

    /**
     * Processing of results from all nodes.
     * Called on one of the nodes containing the snapshot data.
     *
     * @param name Snapshot name.
     * @param results Results from all nodes.
     * @throws IgniteCheckedException If failed.
     */
    public default void complete(String name, Collection<SnapshotHandlerResult<T>> results) throws IgniteCheckedException {
        for (SnapshotHandlerResult<T> res : results) {
            if (res.error() == null)
                continue;;

            throw new IgniteCheckedException("Snapshot handler has failed " +
                "[snapshot=" + name +
                ", handler=" + getClass().getName() +
                ", nodeId=" + res.node().id() + "].", res.error());
        }
    }
}

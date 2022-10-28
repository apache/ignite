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
 * <p>
 * The execution of the handler consists of two steps:
 * <ol>
 * <li>Local call of {@link #invoke(SnapshotHandlerContext)} method on all nodes containing the snapshot data or
 * validating snapshot operation.</li>
 * <li>Processing the results of local invocations in the {@link #complete(String, Collection)} method on one of the
 * nodes containing the snapshot data.</li>
 * </ol>
 * Note: If during the execution of a snapshot operation some node exits, then whole operation is rolled back, in which
 *       case the {@link #complete(String, Collection)} method may not be called.
 *
 * @param <T> Type of the local processing result. Could be a warning result, {@link SnapshotHandlerWarning}
 */
public interface SnapshotHandler<T> extends Extension {
    /** Snapshot handler type. */
    public SnapshotHandlerType type();

    /**
     * Local processing of a snapshot operation. Called on every node that contains snapshot data or should check
     * snapshot operation.
     *
     * @param ctx Snapshot handler context.
     * @return Result of local processing. This result will be returned in {@link SnapshotHandlerResult#data()} method
     *      passed into {@link #complete(String, Collection)} handler method.
     * @throws Exception If invocation caused an exception. This exception will be returned in {@link
     *      SnapshotHandlerResult#error()}} method passed into {@link #complete(String, Collection)} handler method.
     * @see SnapshotHandlerWarning
     */
    public @Nullable T invoke(SnapshotHandlerContext ctx) throws Exception;

    /**
     * Processing the results of the {@link #invoke(SnapshotHandlerContext)} method received from all nodes. This method
     * is called on coordinator node for {@link SnapshotHandlerType#CREATE} handler type and on the random node
     * containing the snapshot data for {@link SnapshotHandlerType#RESTORE}.
     * <p>
     * Note: If this method fails, the entire cluster-wide snapshot operation will be aborted and the changes made by it
     * will be rolled back.
     *
     * @param name Snapshot name.
     * @param results Results from all nodes.
     * @throws Exception If the snapshot operation needs to be aborted.
     * @see SnapshotHandlerResult
     */
    public default void complete(String name, Collection<SnapshotHandlerResult<T>> results) throws Exception {
        for (SnapshotHandlerResult<T> res : results) {
            if (res.error() == null)
                continue;

            throw new IgniteCheckedException("Snapshot handler has failed. " + res.error().getMessage() +
                " [snapshot=" + name +
                ", handler=" + getClass().getName() +
                ", nodeId=" + res.node().id() + "].", res.error());
        }
    }
}

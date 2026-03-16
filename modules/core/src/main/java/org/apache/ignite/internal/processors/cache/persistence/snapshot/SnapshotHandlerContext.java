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
import java.util.function.BiConsumer;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.cache.persistence.filename.SnapshotFileTree;
import org.jetbrains.annotations.Nullable;

/**
 * Snapshot operation handler context.
 */
public class SnapshotHandlerContext {
    /** Snapshot metadata. */
    private final SnapshotMetadata metadata;

    /** The full path to the snapshot files. */
    private final SnapshotFileTree sft;

    /** The names of the cache groups on which the operation is performed. */
    private final Collection<String> grps;

    /** Local node. */
    private final ClusterNode locNode;

    /** Warning flag of concurrent inconsistent-by-nature streamer updates. */
    private final boolean streamerWrn;

    /** If {@code true}, calculates and compares partition hashes. Otherwise, only basic snapshot validation is launched.*/
    private final boolean check;

    /** Consumer of total work units per handler type. */
    private final BiConsumer<Class<? extends SnapshotHandler<?>>, Integer> totalCnsmr;

    /** Consumer of processed work unit per handler type. */
    private final BiConsumer<Class<? extends SnapshotHandler<?>>, Integer> progressCnsmr;

    /**
     * @param metadata Snapshot metadata.
     * @param grps The names of the cache groups on which the operation is performed.
     * {@code False} otherwise. Always {@code false} for snapshot restoration.
     * @param locNode Local node.
     * @param sft Snapshot file tree.
     * @param streamerWrn {@code True} if concurrent streaming updates occurred during snapshot operation.
     * @param check If {@code true}, calculates and compares partition hashes. Otherwise, only basic snapshot validation is launched.
     * @param totalCnsmr Consumer of total work units per handler type.
     * @param progressCnsmr Consumer of processed work unit per handler type.
     */
    public SnapshotHandlerContext(
        SnapshotMetadata metadata,
        @Nullable Collection<String> grps,
        ClusterNode locNode,
        SnapshotFileTree sft,
        boolean streamerWrn,
        boolean check,
        @Nullable BiConsumer<Class<? extends SnapshotHandler<?>>, Integer> totalCnsmr,
        @Nullable BiConsumer<Class<? extends SnapshotHandler<?>>, Integer> progressCnsmr
    ) {
        this.metadata = metadata;
        this.grps = grps;
        this.locNode = locNode;
        this.sft = sft;
        this.streamerWrn = streamerWrn;
        this.check = check;
        this.totalCnsmr = totalCnsmr;
        this.progressCnsmr = progressCnsmr;
    }

    /**
     * @return Snapshot metadata.
     */
    public SnapshotMetadata metadata() {
        return metadata;
    }

    /**
     * @return Snapshot file tree.
     */
    public SnapshotFileTree snapshotFileTree() {
        return sft;
    }

    /**
     * @return The names of the cache groups on which the operation is performed. May be {@code null} if the operation
     * is performed on all available cache groups.
     */
    public @Nullable Collection<String> groups() {
        return grps;
    }

    /**
     * @return Local node.
     */
    public ClusterNode localNode() {
        return locNode;
    }

    /**
     * @return {@code True} if concurrent streaming updates occurred during snapshot operation. {@code False} otherwise.
     */
    public boolean streamerWarning() {
        return streamerWrn;
    }

    /** @return If {@code true}, calculates and compares partition hashes. Otherwise, only basic snapshot validation is launched. */
    public boolean check() {
        return check;
    }

    /** @return Consumer of total work units per handler type. */
    public @Nullable BiConsumer<Class<? extends SnapshotHandler<?>>, Integer> totalConsumer() {
        return totalCnsmr;
    }

    /** @return Consumer of processed work unit per handler type. */
    public @Nullable BiConsumer<Class<? extends SnapshotHandler<?>>, Integer> progressConsumer() {
        return progressCnsmr;
    }
}

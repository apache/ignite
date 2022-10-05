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

import java.io.File;
import java.util.Collection;
import org.apache.ignite.cluster.ClusterNode;
import org.jetbrains.annotations.Nullable;

/**
 * Snapshot operation handler context.
 */
public class SnapshotHandlerContext {
    /** Snapshot metadata. {@code Null} for client on not-baseline node. */
    private final @Nullable SnapshotMetadata metadata;

    /** The full path to the snapshot files. {@code Null} for client on not-baseline node. */
    private final @Nullable File snpDir;

    /** The names of the cache groups on which the operation is performed. */
    private final Collection<String> grps;

    /** Local node. */
    private final ClusterNode locNode;

    /**
     * @param metadata Snapshot metadata. {@code Null} for client on not-baseline node.
     * @param grps The names of the cache groups on which the operation is performed.
     * @param locNode Local node.
     * @param snpDir The full path to the snapshot files. {@code Null} for client on not-baseline node.
     */
    public SnapshotHandlerContext(@Nullable SnapshotMetadata metadata, @Nullable Collection<String> grps,
        ClusterNode locNode, @Nullable File snpDir) {
        this.metadata = metadata;
        this.grps = grps;
        this.locNode = locNode;
        this.snpDir = snpDir;
    }

    /**
     * @return Snapshot metadata. {@code Null} for client on not-baseline node.
     */
    public SnapshotMetadata metadata() {
        return metadata;
    }

    /**
     * @return The full path to the snapshot files. {@code Null} for client on not-baseline node.
     */
    public File snapshotDirectory() {
        return snpDir;
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
}

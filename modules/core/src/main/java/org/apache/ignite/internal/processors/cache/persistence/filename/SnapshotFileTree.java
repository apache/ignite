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

package org.apache.ignite.internal.processors.cache.persistence.filename;

import java.io.File;
import org.jetbrains.annotations.Nullable;

/**
 * {@link NodeFileTree} extension with the methods required to work with snapshot file tree.
 * During creation, full snapshot, creates the same file tree as regular node.
 * But, using snapshot directory as root.
 */
public class SnapshotFileTree extends NodeFileTree {
    /** Snapshot name. */
    private final String name;

    /** Snapshot path. */
    @Nullable private final String path;

    /** Node file tree relative to {@link #tempFileTree()}. */
    private final NodeFileTree tmpFt;

    /**
     * @param ft Node file tree.
     */
    public SnapshotFileTree(NodeFileTree ft, String name, @Nullable String path) {
        super(root(ft, name, path), ft.folderName());

        this.name = name;
        this.path = path;
        this.tmpFt = new NodeFileTree(new File(ft.snapshotTempRoot(), name), ft.folderName());
    }

    /** @return Snapshot name. */
    public String name() {
        return name;
    }

    /** @return Snapshot path. */
    @Nullable public String path() {
        return path;
    }

    /** @return Snapshot temp file tree. */
    public NodeFileTree tempFileTree() {
        return tmpFt;
    }

    /**
     * @return Path to the snapshot root directory.
     */
    public static File root(NodeFileTree ft, String name, String path) {
        assert name != null : "Snapshot name cannot be empty or null.";

        return path == null ? new File(ft.snapshotsRoot(), name) : new File(path, name);
    }
}

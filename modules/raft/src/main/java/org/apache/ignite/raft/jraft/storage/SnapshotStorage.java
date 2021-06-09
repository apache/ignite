/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.raft.jraft.storage;

import org.apache.ignite.raft.jraft.Lifecycle;
import org.apache.ignite.raft.jraft.option.SnapshotCopierOptions;
import org.apache.ignite.raft.jraft.storage.snapshot.SnapshotCopier;
import org.apache.ignite.raft.jraft.storage.snapshot.SnapshotReader;
import org.apache.ignite.raft.jraft.storage.snapshot.SnapshotWriter;

/**
 * Snapshot storage.
 */
public interface SnapshotStorage extends Lifecycle<Void>, Storage {
    /**
     * Set filterBeforeCopyRemote to be true.When true, it will filter the data before copy to remote.
     */
    boolean setFilterBeforeCopyRemote();

    /**
     * Create a snapshot writer.
     */
    SnapshotWriter create();

    /**
     * Open a snapshot reader.
     */
    SnapshotReader open();

    /**
     * Copy data from remote uri.
     *
     * @param uri remote uri
     * @param opts copy options
     * @return a SnapshotReader instance
     */
    SnapshotReader copyFrom(final String uri, final SnapshotCopierOptions opts);

    /**
     * Starts a copy job to copy data from remote uri.
     *
     * @param uri remote uri
     * @param opts copy options
     * @return a SnapshotCopier instance
     */
    SnapshotCopier startToCopyFrom(final String uri, final SnapshotCopierOptions opts);

    /**
     * Configure a SnapshotThrottle.
     *
     * @param snapshotThrottle throttle of snapshot
     */
    void setSnapshotThrottle(final SnapshotThrottle snapshotThrottle);
}

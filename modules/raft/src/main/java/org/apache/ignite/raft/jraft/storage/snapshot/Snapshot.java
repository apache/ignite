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
package org.apache.ignite.raft.jraft.storage.snapshot;

import java.util.Set;
import org.apache.ignite.raft.jraft.Status;
import org.apache.ignite.raft.jraft.rpc.Message;

/**
 * Represents a state machine snapshot.
 */
public abstract class Snapshot extends Status {

    /**
     * Snapshot metadata file name.
     */
    public static final String JRAFT_SNAPSHOT_META_FILE = "__raft_snapshot_meta";
    /**
     * Snapshot file prefix.
     */
    public static final String JRAFT_SNAPSHOT_PREFIX = "snapshot_";
    /** Snapshot uri scheme for remote peer */
    public static final String REMOTE_SNAPSHOT_URI_SCHEME = "remote://";

    /**
     * Get the path of the Snapshot
     */
    public abstract String getPath();

    /**
     * List all the existing files in the Snapshot currently
     */
    public abstract Set<String> listFiles();

    /**
     * Get file meta by fileName.
     */
    public abstract Message getFileMeta(final String fileName);
}

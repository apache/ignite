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
package org.apache.ignite.raft.jraft.closure;

import org.apache.ignite.raft.jraft.Closure;
import org.apache.ignite.raft.jraft.entity.RaftOutter.SnapshotMeta;
import org.apache.ignite.raft.jraft.storage.snapshot.SnapshotWriter;

/**
 * Save snapshot closure
 */
public interface SaveSnapshotClosure extends Closure {

    /**
     * Starts to save snapshot, returns the writer.
     *
     * @param meta metadata of snapshot.
     * @return returns snapshot writer.
     */
    SnapshotWriter start(final SnapshotMeta meta);
}

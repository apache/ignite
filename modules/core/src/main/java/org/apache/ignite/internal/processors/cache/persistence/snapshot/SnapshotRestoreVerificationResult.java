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

import java.io.Serializable;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.internal.processors.cache.StoredCacheData;

/**
 * Result of a cache group restore verification job.
 */
public class SnapshotRestoreVerificationResult implements Serializable {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** List of stored cache configurations. */
    private final List<StoredCacheData> ccfgs;

    /** Local node ID. */
    private final UUID locNodeId;

    /**
     * @param ccfgs List of stored cache configurations.
     * @param locNodeId Local node ID.
     */
    public SnapshotRestoreVerificationResult(List<StoredCacheData> ccfgs, UUID locNodeId) {
        this.ccfgs = ccfgs;
        this.locNodeId = locNodeId;
    }

    /**
     * @return List of stored cache configurations.
     */
    public List<StoredCacheData> configs() {
        return ccfgs;
    }

    /**
     * @return Local node ID.
     */
    public UUID localNodeId() {
        return locNodeId;
    }
}

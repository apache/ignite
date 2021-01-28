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
import java.util.Map;
import java.util.Set;
import org.apache.ignite.internal.processors.cache.StoredCacheData;

/**
 * Snapshot restore prepare operation single node validation response.
 */
public class SnapshotRestorePrepareResponse implements Serializable {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    private List<StoredCacheData> ccfgs;

    private Map<String, Set<Integer>> partIds;

    /** */
    public SnapshotRestorePrepareResponse() {
        // No-op.
    }

    /**
     * @param groups List of cache groups snapshot details.
     */
    public SnapshotRestorePrepareResponse(List<StoredCacheData> ccfgs, Map<String, Set<Integer>> partIds) {
        this.ccfgs = ccfgs;
        this.partIds = partIds;
    }

    /** todo */
    public List<StoredCacheData> configs() {
        return ccfgs;
    }

    public Map<String, Set<Integer>> partIds() {
        return partIds;
    }

}

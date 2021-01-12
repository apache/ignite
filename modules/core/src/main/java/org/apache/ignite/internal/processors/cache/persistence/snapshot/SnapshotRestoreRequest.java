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
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;

public class SnapshotRestoreRequest implements Serializable {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Snapshot name. */
    private final String snpName;

    /** The list of cache groups to restore from the snapshot. */
    @GridToStringInclude
    private final Collection<String> grps;

    @GridToStringInclude
    private final Set<UUID> reqNodes;

    /** Request ID. */
    private final UUID reqId = UUID.randomUUID();

    public SnapshotRestoreRequest(String snpName, Collection<String> grps, Set<UUID> reqNodes) {
        this.snpName = snpName;
        this.grps = grps;
        this.reqNodes = reqNodes;
    }

    public UUID requestId() {
        return reqId;
    }

    public Collection<String> groups() {
        return grps;
    }

    public String snapshotName() {
        return snpName;
    }

    public Set<UUID> requiredNodes() {
        return Collections.unmodifiableSet(reqNodes);
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        return Objects.equals(reqId, ((SnapshotRestoreRequest)o).reqId);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(reqId);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SnapshotRestoreRequest.class, this);
    }
}

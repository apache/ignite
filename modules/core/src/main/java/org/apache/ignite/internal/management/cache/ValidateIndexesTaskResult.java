/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.management.cache;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class ValidateIndexesTaskResult extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Exceptions. */
    private @Nullable Map<NodeInfo, Exception> exceptions;

    /** Results from cluster. */
    private @Nullable Map<NodeInfo, ValidateIndexesJobResult> results;

    /**
     * Adds single node job result.
     */
    public void addResult(ClusterNode clusterNode, ValidateIndexesJobResult jobResult) {
        if (results == null)
            results = new HashMap<>();

        results.put(new NodeInfo(clusterNode.id(), clusterNode.consistentId()), jobResult);
    }

    /**
     * @return Single node job result or {@code null} if not found.
     */
    public @Nullable ValidateIndexesJobResult jobResult(ClusterNode clusterNode) {
        return results().get(new NodeInfo(clusterNode.id(), clusterNode.consistentId()));
    }

    /**
     * @return Results from cluster.
     */
    public Map<NodeInfo, ValidateIndexesJobResult> results() {
        return results == null ? Collections.emptyMap() : results;
    }

    /**
     * Adds single node job failure.
     */
    public void addException(ClusterNode clusterNode, Exception exception) {
        if (exceptions == null)
            exceptions = new HashMap<>();

        exceptions.put(new NodeInfo(clusterNode.id(), clusterNode.consistentId()), exception);
    }

    /**
     * @return Exceptions.
     */
    public Map<NodeInfo, Exception> exceptions() {
        return exceptions == null ? Collections.emptyMap() : exceptions;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeMap(out, exceptions);
        U.writeMap(out, results);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(ObjectInput in) throws IOException, ClassNotFoundException {
        exceptions = U.readMap(in);
        results = U.readMap(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ValidateIndexesTaskResult.class, this);
    }

    /**
     * Holds node id and consistent id.
     */
    public static final class NodeInfo implements Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final UUID id;

        /** */
        private final Object consistentId;

        /** */
        private NodeInfo(UUID id, Object consistentId) {
            assert consistentId instanceof Serializable || consistentId instanceof Externalizable;

            this.id = id;
            this.consistentId = consistentId;
        }

        /** */
        public UUID id() {
            return id;
        }

        /** */
        public Object consistentId() {
            return consistentId;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            NodeInfo id1 = (NodeInfo)o;

            return id.equals(id1.id) && consistentId.equals(id1.consistentId);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(id, consistentId);
        }
    }
}

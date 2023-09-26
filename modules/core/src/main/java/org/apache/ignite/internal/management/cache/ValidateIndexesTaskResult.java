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

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;

/**
 *
 */
public class ValidateIndexesTaskResult extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Exceptions. */
    private Map<NodeFullId, Exception> exceptions;

    /** Results from cluster. */
    private Map<NodeFullId, ValidateIndexesJobResult> results;

    /**
     * @param results Results.
     * @param exceptions Exceptions.
     */
    public ValidateIndexesTaskResult(
        Map<NodeFullId, ValidateIndexesJobResult> results,
        Map<NodeFullId, Exception> exceptions
    ) {
        this.exceptions = exceptions;
        this.results = results;
    }

    /**
     * For externalization only.
     */
    public ValidateIndexesTaskResult() {
    }

    /**
     * @return Exceptions.
     */
    public Map<NodeFullId, Exception> exceptions() {
        return exceptions;
    }

    /**
     * @return Results from cluster.
     */
    public Map<NodeFullId, ValidateIndexesJobResult> results() {
        return results;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeMap(out, exceptions);
        U.writeMap(out, results);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        exceptions = U.readMap(in);
        results = U.readMap(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ValidateIndexesTaskResult.class, this);
    }

    /** */
    public static NodeFullId nodeFullId(ClusterNode clusterNode) {
        return new NodeFullId(clusterNode.id(), (Serializable)clusterNode.consistentId());
    }

    /**
     * Holds node id and consistent id.
     */
    public static final class NodeFullId implements Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final UUID id;

        /** */
        private final Serializable consistentId;

        /** */
        private NodeFullId(UUID id, Serializable consistentId) {
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

            NodeFullId id1 = (NodeFullId)o;

            return id.equals(id1.id) && consistentId.equals(id1.consistentId);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(id, consistentId);
        }
    }
}

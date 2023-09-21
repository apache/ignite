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
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class ValidateIndexesTaskResult extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Exceptions. */
    private Map<UUID, Exception> exceptions;

    /** Results from cluster. */
    private Map<UUID, ValidateIndexesJobResult> results;

    /** Map of node id to consistent id. */
    private @Nullable Map<UUID, Object> consistentIds;

    /**
     * @param results Results.
     * @param exceptions Exceptions.
     * @param consistentIds Map of node id to consistent id.
     */
    public ValidateIndexesTaskResult(
        Map<UUID, ValidateIndexesJobResult> results,
        Map<UUID, Exception> exceptions,
        Map<UUID, Object> consistentIds
    ) {
        this.exceptions = exceptions;
        this.results = results;

        assert consistentIds == null || Stream.concat(results.keySet().stream(), exceptions.keySet().stream())
            .collect(Collectors.toSet()).equals(consistentIds.keySet());

        this.consistentIds = consistentIds;
    }

    /**
     * For externalization only.
     */
    public ValidateIndexesTaskResult() {
    }

    /**
     * @return Exceptions.
     */
    public Map<UUID, Exception> exceptions() {
        return exceptions;
    }

    /**
     * @return Results from cluster.
     */
    public Map<UUID, ValidateIndexesJobResult> results() {
        return results;
    }

    /**
     * @return Consistent id of node with id {@cod nodeId}. {@code Null} if {@code nodeId} is not found in the results.
     */
    public @Nullable Object consistentId(UUID nodeId) {
        return consistentIds == null ? null : consistentIds.get(nodeId);
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeMap(out, exceptions);
        U.writeMap(out, results);
        U.writeMap(out, consistentIds);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        exceptions = U.readMap(in);
        results = U.readMap(in);
        consistentIds = U.readMap(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ValidateIndexesTaskResult.class, this);
    }
}

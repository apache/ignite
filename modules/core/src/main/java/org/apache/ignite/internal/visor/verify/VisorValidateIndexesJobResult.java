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

package org.apache.ignite.internal.visor.verify;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import org.apache.ignite.internal.processors.cache.verify.PartitionKey;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;
import org.jetbrains.annotations.NotNull;

/**
 *
 */
public class VisorValidateIndexesJobResult extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Results of indexes validation from node. */
    @GridToStringInclude
    private Map<PartitionKey, ValidateIndexesPartitionResult> partRes;

    /** Results of reverse indexes validation from node. */
    @GridToStringInclude
    private Map<String, ValidateIndexesPartitionResult> idxRes;

    /** Integrity check issues. */
    @GridToStringInclude
    private Collection<IndexIntegrityCheckIssue> integrityCheckFailures;

    /**
     * @param partRes Results of indexes validation from node.
     * @param idxRes Results of reverse indexes validation from node.
     * @param  integrityCheckFailures Collection of indexes integrity check failures.
     */
    public VisorValidateIndexesJobResult(
            @NotNull Map<PartitionKey, ValidateIndexesPartitionResult> partRes,
            @NotNull Map<String, ValidateIndexesPartitionResult> idxRes,
            @NotNull Collection<IndexIntegrityCheckIssue> integrityCheckFailures
    ) {
        this.partRes = partRes;
        this.idxRes = idxRes;
        this.integrityCheckFailures = integrityCheckFailures;
    }

    /**
     * For externalization only.
     */
    public VisorValidateIndexesJobResult() {
    }

    /** {@inheritDoc} */
    @Override public byte getProtocolVersion() {
        return V3;
    }

    /**
     * @return Results of indexes validation from node.
     */
    public Map<PartitionKey, ValidateIndexesPartitionResult> partitionResult() {
        return partRes;
    }

    /**
     * @return Results of reverse indexes validation from node.
     */
    public Map<String, ValidateIndexesPartitionResult> indexResult() {
        return idxRes == null ? Collections.emptyMap() : idxRes;
    }

    /**
     * @return Collection of failed integrity checks.
     */
    public Collection<IndexIntegrityCheckIssue> integrityCheckFailures() {
        return integrityCheckFailures == null ? Collections.emptyList() : integrityCheckFailures;
    }

    /**
     * @return {@code true} If any indexes issues found on node, otherwise returns {@code false}.
     */
    public boolean hasIssues() {
        return (integrityCheckFailures != null && !integrityCheckFailures.isEmpty()) ||
                (partRes != null && partRes.entrySet().stream().anyMatch(e -> !e.getValue().issues().isEmpty())) ||
                (idxRes != null && idxRes.entrySet().stream().anyMatch(e -> !e.getValue().issues().isEmpty()));
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeMap(out, partRes);
        U.writeMap(out, idxRes);
        U.writeCollection(out, integrityCheckFailures);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        partRes = U.readMap(in);

        if (protoVer >= V2)
            idxRes = U.readMap(in);

        if (protoVer >= V3)
            integrityCheckFailures = U.readCollection(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorValidateIndexesJobResult.class, this);
    }
}

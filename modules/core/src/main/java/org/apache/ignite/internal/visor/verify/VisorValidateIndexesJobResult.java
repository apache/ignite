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
import java.util.Map;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.processors.cache.verify.PartitionKey;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.apache.ignite.internal.util.IgniteUtils.readCollection;
import static org.apache.ignite.internal.util.IgniteUtils.readMap;
import static org.apache.ignite.internal.util.IgniteUtils.writeCollection;
import static org.apache.ignite.internal.util.IgniteUtils.writeMap;

/**
 *
 */
public class VisorValidateIndexesJobResult extends IgniteDataTransferObject {
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

    /** Results of checking size cache and index. */
    @GridToStringInclude
    private Map<String, ValidateIndexesCheckSizeResult> checkSizeRes;

    /**
     * Constructor.
     *
     * @param partRes Results of indexes validation from node.
     * @param idxRes Results of reverse indexes validation from node.
     * @param integrityCheckFailures Collection of indexes integrity check failures.
     * @param checkSizeRes Results of checking size cache and index.
     */
    public VisorValidateIndexesJobResult(
        Map<PartitionKey, ValidateIndexesPartitionResult> partRes,
        @Nullable Map<String, ValidateIndexesPartitionResult> idxRes,
        @Nullable Collection<IndexIntegrityCheckIssue> integrityCheckFailures,
        @Nullable Map<String, ValidateIndexesCheckSizeResult> checkSizeRes
    ) {
        this.partRes = partRes;
        this.idxRes = idxRes;
        this.integrityCheckFailures = integrityCheckFailures;
        this.checkSizeRes = checkSizeRes;
    }

    /**
     * For externalization only.
     */
    public VisorValidateIndexesJobResult() {
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
        return idxRes == null ? emptyMap() : idxRes;
    }

    /**
     * @return Collection of failed integrity checks.
     */
    public Collection<IndexIntegrityCheckIssue> integrityCheckFailures() {
        return integrityCheckFailures == null ? emptyList() : integrityCheckFailures;
    }

    /**
     * Return results of checking size cache and index.
     *
     * @return Results of checking size cache and index.
     */
    public Map<String, ValidateIndexesCheckSizeResult> checkSizeResult() {
        return checkSizeRes == null ? emptyMap() : checkSizeRes;
    }

    /**
     * @return {@code true} If any indexes issues found on node, otherwise returns {@code false}.
     */
    public boolean hasIssues() {
        return (integrityCheckFailures != null && !integrityCheckFailures.isEmpty()) ||
            (partRes != null && partRes.entrySet().stream().anyMatch(e -> !e.getValue().issues().isEmpty())) ||
            (idxRes != null && idxRes.entrySet().stream().anyMatch(e -> !e.getValue().issues().isEmpty())) ||
            (checkSizeRes != null && checkSizeRes.entrySet().stream().anyMatch(e -> !e.getValue().issues().isEmpty()));
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        writeMap(out, partRes);
        writeMap(out, idxRes);
        writeCollection(out, integrityCheckFailures);
        writeMap(out, checkSizeRes);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(
        byte protoVer,
        ObjectInput in
    ) throws IOException, ClassNotFoundException {
        partRes = readMap(in);
        idxRes = readMap(in);
        integrityCheckFailures = readCollection(in);
        checkSizeRes = readMap(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorValidateIndexesJobResult.class, this);
    }
}

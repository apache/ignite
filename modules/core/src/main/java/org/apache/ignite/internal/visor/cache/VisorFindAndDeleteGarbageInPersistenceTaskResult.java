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

package org.apache.ignite.internal.visor.cache;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Result of running {@link VisorFindAndDeleteGarbageInPersistenceTask} which would contain result
 * from all participating nodes.
 */
public class VisorFindAndDeleteGarbageInPersistenceTaskResult extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Results of indexes validation from node. */
    @GridToStringInclude
    private Map<UUID, VisorFindAndDeleteGarbageInPersistenceJobResult> result;

    /** Exceptions while indexes validation from node. */
    @GridToStringInclude
    private Map<UUID, Exception> exceptions;

    /**
     *
     */
    public VisorFindAndDeleteGarbageInPersistenceTaskResult() {
    }

    /**
     * For externalization only.
     * @param jobResults Per node job result.
     * @param exceptions Per node execution problems.
     */
    public VisorFindAndDeleteGarbageInPersistenceTaskResult(
        Map<UUID, VisorFindAndDeleteGarbageInPersistenceJobResult> jobResults,
        Map<UUID, Exception> exceptions
    ) {
        this.result = jobResults;
        this.exceptions = exceptions;
    }

    /**
     * @return Results of indexes validation from node.
     */
    public Map<UUID, VisorFindAndDeleteGarbageInPersistenceJobResult> result() {
        return result;
    }

    /**
     * @return Exception of execution on nodes.
     */
    public Map<UUID, Exception> exceptions() {
        return exceptions;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeMap(out, result);
        U.writeMap(out, exceptions);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        result = U.readMap(in);
        exceptions = U.readMap(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorFindAndDeleteGarbageInPersistenceTaskResult.class, this);
    }
}

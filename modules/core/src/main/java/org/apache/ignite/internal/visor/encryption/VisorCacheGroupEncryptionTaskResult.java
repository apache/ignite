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

package org.apache.ignite.internal.visor.encryption;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Multinode cache group encryption task result.
 *
 * @param <T> Job result type.
 */
@SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
public class VisorCacheGroupEncryptionTaskResult<T> extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Per node job result. */
    @GridToStringInclude
    private Map<UUID, T> results;

    /** Per node execution problems. */
    @GridToStringInclude
    private Map<UUID, IgniteException> exceptions;

    /**
     * @param results Per node job result.
     * @param exceptions Per node execution problems.
     */
    public VisorCacheGroupEncryptionTaskResult(Map<UUID, T> results, Map<UUID, IgniteException> exceptions) {
        this.results = results;
        this.exceptions = exceptions;
    }

    /** */
    public VisorCacheGroupEncryptionTaskResult() {
        // No-op.
    }

    /** @return Per node job result. */
    public Map<UUID, T> results() {
        return results == null ? Collections.emptyMap() : results;
    }

    /** @return Per node execution problems. */
    public Map<UUID, IgniteException> exceptions() {
        return exceptions == null ? Collections.emptyMap() : exceptions;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeMap(out, results);
        U.writeMap(out, exceptions);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte ver, ObjectInput in) throws IOException, ClassNotFoundException {
        results = U.readMap(in);
        exceptions = U.readMap(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorCacheGroupEncryptionTaskResult.class, this);
    }
}

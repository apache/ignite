/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.schema;

import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.S;

import java.util.UUID;

/**
 * Schema operation client future.
 */
public class SchemaOperationClientFuture extends GridFutureAdapter<Object> {
    /** Operation ID. */
    private final UUID opId;

    /**
     * Constructor.
     *
     * @param opId Operation ID.
     */
    public SchemaOperationClientFuture(UUID opId) {
        this.opId = opId;
    }

    /**
     * @return Operation ID.
     */
    public UUID operationId() {
        return opId;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SchemaOperationClientFuture.class, this);
    }
}

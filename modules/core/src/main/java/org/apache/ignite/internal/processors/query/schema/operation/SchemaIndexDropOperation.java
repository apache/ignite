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

package org.apache.ignite.internal.processors.query.schema.operation;

import org.apache.ignite.internal.util.typedef.internal.S;

import java.util.UUID;

/**
 * Schema index drop operation.
 */
public class SchemaIndexDropOperation extends SchemaIndexAbstractOperation {
    /** */
    private static final long serialVersionUID = 0L;

    /** Index name. */
    private final String idxName;

    /** Ignore operation if index doesn't exist. */
    private final boolean ifExists;

    /**
     * Constructor.
     *
     * @param opId Operation id.
     * @param cacheName Cache name.
     * @param schemaName Schema name.
     * @param idxName Index name.
     * @param ifExists Ignore operation if index doesn't exist.
     */
    public SchemaIndexDropOperation(UUID opId, String cacheName, String schemaName, String idxName, boolean ifExists) {
        super(opId, cacheName, schemaName);

        this.idxName = idxName;
        this.ifExists = ifExists;
    }

    /** {@inheritDoc} */
    @Override public String indexName() {
        return idxName;
    }

    /**
     * @return Ignore operation if index doesn't exist.
     */
    public boolean ifExists() {
        return ifExists;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SchemaIndexDropOperation.class, this, "parent", super.toString());
    }
}

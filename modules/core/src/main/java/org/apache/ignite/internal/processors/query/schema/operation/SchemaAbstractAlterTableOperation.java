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

import java.util.UUID;

/**
 * Parent class for ALTER TABLE command variants.
 */
public class SchemaAbstractAlterTableOperation extends SchemaAbstractOperation {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Constructor.
     *
     * @param opId Operation ID.
     * @param cacheName Cache name.
     * @param schemaName Schema name.
     */
    public SchemaAbstractAlterTableOperation(UUID opId, String cacheName, String schemaName) {
        super(opId, cacheName, schemaName);
    }
}

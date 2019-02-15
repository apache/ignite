/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.query.schema.operation;

import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;

import java.util.UUID;

/**
 * Schema index create operation.
 */
public class SchemaIndexCreateOperation extends SchemaIndexAbstractOperation {
    /** */
    private static final long serialVersionUID = 0L;

    /** Table name. */
    private final String tblName;

    /** Index. */
    @GridToStringInclude
    private final QueryIndex idx;

    /** Ignore operation if index exists. */
    private final boolean ifNotExists;

    /** Index creation parallelism level */
    private final int parallel;

    /**
     * Constructor.
     *
     * @param opId Operation id.
     * @param cacheName Cache name.
     * @param schemaName Schame name.
     * @param tblName Table name.
     * @param idx Index params.
     * @param ifNotExists Ignore operation if index exists.
     * @param parallel Index creation parallelism level.
     */
    public SchemaIndexCreateOperation(UUID opId, String cacheName, String schemaName, String tblName, QueryIndex idx,
        boolean ifNotExists, int parallel) {
        super(opId, cacheName, schemaName);

        this.tblName = tblName;
        this.idx = idx;
        this.ifNotExists = ifNotExists;
        this.parallel = parallel;
    }

    /** {@inheritDoc} */
    @Override public String indexName() {
        return QueryUtils.indexName(tblName, idx);
    }

    /**
     * @return Table name.
     */
    public String tableName() {
        return tblName;
    }

    /**
     * @return Index params.
     */
    public QueryIndex index() {
        return idx;
    }

    /**
     * @return Ignore operation if index exists.
     */
    public boolean ifNotExists() {
        return ifNotExists;
    }

    /**
     * Gets index creation parallelism level.
     *
     * @return Index creation parallelism level.
     */
    public int parallel() {
        return parallel;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SchemaIndexCreateOperation.class, this, "parent", super.toString());
    }
}

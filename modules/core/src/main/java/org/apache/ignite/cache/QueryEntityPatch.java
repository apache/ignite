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

package org.apache.ignite.cache;

import java.util.Collection;
import org.apache.ignite.internal.processors.query.schema.operation.SchemaAbstractOperation;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Query entity patch which contain {@link SchemaAbstractOperation} operations for changing query entity.
 * This patch can only add properties to entity and can't remove them.
 * Other words, the patch will contain only add operations
 * (e.g.:
 * {@link org.apache.ignite.internal.processors.query.schema.operation.SchemaAlterTableAddColumnOperation},
 * {@link org.apache.ignite.internal.processors.query.schema.operation.SchemaIndexCreateOperation}
 * ) and not remove ones.
 *
 * It contain only add operation because at the moment we don't have history of schema operations
 * and by current state we can't understand some property was already deleted or it has not been added yet.
 */
public class QueryEntityPatch {
    /** Empty query entity patch. */
    private static final QueryEntityPatch EMPTY_QUERY_ENTITY_PATCH = new QueryEntityPatch(null, null);

    /** Message which described conflicts during creating this patch. */
    private String conflictsMessage;

    /** Operations for modification query entity. */
    private Collection<SchemaAbstractOperation> patchOperations;

    /**
     * Create patch.
     */
    private QueryEntityPatch(String conflictsMessage, Collection<SchemaAbstractOperation> patchOperations) {
        this.conflictsMessage = conflictsMessage;
        this.patchOperations = patchOperations;
    }

    /**
     * Builder method for patch with conflicts.
     *
     * @param conflicts Conflicts.
     * @return Query entity patch with conflicts.
     */
    public static QueryEntityPatch conflict(String conflicts) {
        return new QueryEntityPatch(conflicts, null);
    }

    /**
     * Builder method for empty patch.
     *
     * @return Query entity patch.
     */
    public static QueryEntityPatch empty() {
        return EMPTY_QUERY_ENTITY_PATCH;
    }

    /**
     * Builder method for patch with operations.
     *
     * @param patchOperations Operations for modification.
     * @return Query entity patch which contain {@link SchemaAbstractOperation} operations for changing query entity.
     */
    public static QueryEntityPatch patch(Collection<SchemaAbstractOperation> patchOperations) {
        return new QueryEntityPatch(null, patchOperations);
    }

    /**
     * Check for conflict in this patch.
     *
     * @return {@code true} if patch has conflict.
     */
    public boolean hasConflict() {
        return conflictsMessage != null;
    }

    /**
     * @return {@code true} if patch is empty and can't be applying.
     */
    public boolean isEmpty() {
        return patchOperations == null || patchOperations.isEmpty();
    }

    /**
     * @return Conflicts.
     */
    public String getConflictsMessage() {
        return conflictsMessage;
    }

    /**
     * @return Patch operations for applying.
     */
    public Collection<SchemaAbstractOperation> getPatchOperations() {
        return patchOperations;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(QueryEntityPatch.class, this);
    }
}

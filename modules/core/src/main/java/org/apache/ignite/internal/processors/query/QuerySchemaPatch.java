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

package org.apache.ignite.internal.processors.query;

import java.util.Collection;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.internal.processors.query.schema.operation.SchemaAbstractOperation;
import org.jetbrains.annotations.NotNull;

/**
 * Query schema patch which contains some operations for changing query entities.
 */
public class QuerySchemaPatch {
    /** Conflicts which appears during creating this patch. */
    private String conflicts;

    /** Operations for modification query entity */
    private Collection<SchemaAbstractOperation> patchOperations;

    /** Entities which should be added by whole */
    private Collection<QueryEntity> entityToAdd;

    /**
     * Create patch.
     */
    public QuerySchemaPatch(
        @NotNull Collection<SchemaAbstractOperation> patchOperations,
        @NotNull Collection<QueryEntity> entityToAdd,
        String conflicts) {
        this.patchOperations = patchOperations;
        this.entityToAdd = entityToAdd;
        this.conflicts = conflicts;
    }

    /**
     * @return conflicts
     */
    public String getConflicts() {
        return conflicts;
    }

    /**
     * @return {@code True} if patch is empty and can't be applying.
     */
    public boolean isEmpty() {
        return patchOperations.isEmpty() && entityToAdd.isEmpty();
    }

    /**
     * @return Patch operations for applying.
     */
    @NotNull public Collection<SchemaAbstractOperation> getPatchOperations() {
        return patchOperations;
    }

    /**
     * @return Entities which should be added by whole.
     */
    @NotNull public Collection<QueryEntity> getEntityToAdd() {
        return entityToAdd;
    }
}

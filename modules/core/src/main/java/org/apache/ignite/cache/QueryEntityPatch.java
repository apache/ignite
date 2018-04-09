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

/**
 * Query entity patch which contain some operations for changing query entity.
 */
public class QueryEntityPatch {
    /** Conflicts which appears during creating this patch. */
    private String conflicts;

    /** Operations for modification query entity */
    private Collection<SchemaAbstractOperation> patchOperations;

    /**
     * Create patch.
     */
    private QueryEntityPatch(String conflicts, Collection<SchemaAbstractOperation> patchOperations) {
        this.conflicts = conflicts;
        this.patchOperations = patchOperations;
    }

    /**
     * Builder method for patch with conflicts.
     *
     * @param conflict conflicts.
     * @return query entity patch with conflicts.
     */
    public static QueryEntityPatch conflict(String conflict) {
        return new QueryEntityPatch(conflict, null);
    }

    /**
     * Builder method for empty patch.
     *
     * @return query entity patch.
     */
    public static QueryEntityPatch empty() {
        return new QueryEntityPatch(null, null);
    }

    /**
     * Builder method for patch with operations.
     *
     * @param patchOperations operations for modification.
     * @return Query entity patch which contain some operations for changing query entity.
     */
    public static QueryEntityPatch patch(Collection<SchemaAbstractOperation> patchOperations) {
        return new QueryEntityPatch(null, patchOperations);
    }

    /**
     * Check for conflict in this patch.
     *
     * @return {@code True} if patch has conflict.
     */
    public boolean hasConflict() {
        return conflicts != null;
    }

    /**
     * @return {@code True} if patch is empty and can't be applying.
     */
    public boolean isEmpty() {
        return patchOperations == null || patchOperations.isEmpty();
    }

    /**
     * @return conflicts
     */
    public String getConflicts() {
        return conflicts;
    }

    /**
     * @return Patch operations for applying.
     */
    public Collection<SchemaAbstractOperation> getPatchOperations() {
        return patchOperations;
    }
}

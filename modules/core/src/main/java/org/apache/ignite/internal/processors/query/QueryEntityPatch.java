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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.internal.processors.query.schema.operation.SchemaAbstractOperation;
import org.apache.ignite.internal.processors.query.schema.operation.SchemaAlterTableAddColumnOperation;
import org.apache.ignite.internal.processors.query.schema.operation.SchemaIndexCreateOperation;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.NotNull;

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
class QueryEntityPatch {
    /** Empty query entity patch. */
    private static final QueryEntityPatch EMPTY_QUERY_ENTITY_PATCH = new QueryEntityPatch(null, null);

    /** Message which described conflicts during creating this patch. */
    private final String conflictsMessage;

    /** Operations for modification query entity. */
    private final Collection<SchemaAbstractOperation> patchOperations;

    /**
     * Create patch.
     */
    private QueryEntityPatch(String conflictsMessage, Collection<SchemaAbstractOperation> patchOperations) {
        this.conflictsMessage = conflictsMessage;
        this.patchOperations = patchOperations;
    }

    /**
     * Make query entity patch. This patch can only add properties to entity and can't remove them.
     * Other words, the patch will contain only add operations(e.g. add column, create index) and not remove ones.
     *
     * @param target Query entity to which this entity should be expanded.
     * @return Patch which contains operations for expanding this entity.
     */
    @NotNull static QueryEntityPatch makePatch(QueryEntity locEntity, QueryEntity target) {
        if (target == null)
            return EMPTY_QUERY_ENTITY_PATCH;

        StringBuilder conflicts = new StringBuilder();

        checkEquals(conflicts, "keyType", locEntity.getKeyType(), target.getKeyType());
        checkEquals(conflicts, "valType", locEntity.getValueType(), target.getValueType());
        checkEquals(conflicts, "keyFieldName", locEntity.getKeyFieldName(), target.getKeyFieldName());
        checkEquals(conflicts, "valueFieldName", locEntity.getValueFieldName(), target.getValueFieldName());
        checkEquals(conflicts, "tableName", locEntity.getTableName(), target.getTableName());

        List<QueryField> qryFieldsToAdd = checkFields(locEntity, target, conflicts);

        Collection<QueryIndex> indexesToAdd = checkIndexes(locEntity, target, conflicts);

        if (conflicts.length() != 0)
            return new QueryEntityPatch(locEntity.getTableName() + " conflict: \n" + conflicts.toString(), null);

        Collection<SchemaAbstractOperation> patchOperations = new ArrayList<>();

        if (!qryFieldsToAdd.isEmpty())
            patchOperations.add(new SchemaAlterTableAddColumnOperation(
                UUID.randomUUID(),
                null,
                null,
                locEntity.getTableName(),
                qryFieldsToAdd,
                true,
                true
            ));

        if (!indexesToAdd.isEmpty()) {
            for (QueryIndex idx : indexesToAdd) {
                patchOperations.add(new SchemaIndexCreateOperation(
                    UUID.randomUUID(),
                    null,
                    null,
                    locEntity.getTableName(),
                    idx,
                    true,
                    0
                ));
            }
        }

        return new QueryEntityPatch(null, patchOperations);
    }

    /**
     * Comparing local entity fields and target entity fields.
     *
     * @param target Query entity for check.
     * @param conflicts Storage of conflicts.
     * @return Fields which exist in target and not exist in local.
     */
    @NotNull private static List<QueryField> checkFields(QueryEntity locEntity, QueryEntity target, StringBuilder conflicts) {
        List<QueryField> qryFieldsToAdd = new ArrayList<>();

        for (Map.Entry<String, String> targetField : target.getFields().entrySet()) {
            String targetFieldName = targetField.getKey();
            String targetFieldType = targetField.getValue();
            String targetFieldAlias = target.getAliases().get(targetFieldName);

            if (locEntity.getFields().containsKey(targetFieldName)) {
                checkEquals(
                    conflicts,
                    "alias of " + targetFieldName,
                    locEntity.getAliases().get(targetFieldName),
                    targetFieldAlias
                );

                checkEquals(
                    conflicts,
                    "fieldType of " + targetFieldName,
                    locEntity.getFields().get(targetFieldName),
                    targetFieldType
                );

                checkEquals(
                    conflicts,
                    "nullable of " + targetFieldName,
                    contains(locEntity.getNotNullFields(), targetFieldName),
                    contains(target.getNotNullFields(), targetFieldName)
                );

                checkEquals(
                    conflicts,
                    "default value of " + targetFieldName,
                    getFromMap(locEntity.getDefaultFieldValues(), targetFieldName),
                    getFromMap(target.getDefaultFieldValues(), targetFieldName)
                );

                checkEquals(conflicts,
                    "precision of " + targetFieldName,
                    getFromMap(locEntity.getFieldsPrecision(), targetFieldName),
                    getFromMap(target.getFieldsPrecision(), targetFieldName));

                checkEquals(
                    conflicts,
                    "scale of " + targetFieldName,
                    getFromMap(locEntity.getFieldsScale(), targetFieldName),
                    getFromMap(target.getFieldsScale(), targetFieldName));
            }
            else {
                boolean isAliasConflictsFound = findAliasConflicts(locEntity, targetFieldAlias, targetFieldName, conflicts);

                if (!isAliasConflictsFound) {
                    Integer precision = getFromMap(target.getFieldsPrecision(), targetFieldName);
                    Integer scale = getFromMap(target.getFieldsScale(), targetFieldName);

                    qryFieldsToAdd.add(new QueryField(
                        targetFieldName,
                        targetFieldType,
                        targetFieldAlias,
                        !contains(target.getNotNullFields(), targetFieldName),
                        precision == null ? -1 : precision,
                        scale == null ? -1 : scale
                    ));
                }
            }
        }

        return qryFieldsToAdd;
    }

    /**
     * Comparing local fields and target fields.
     *
     * @param target Query entity for check.
     * @param conflicts Storage of conflicts.
     * @return Indexes which exist in target and not exist in local.
     */
    @NotNull private static Collection<QueryIndex> checkIndexes(QueryEntity locEntity, QueryEntity target, StringBuilder conflicts) {
        HashSet<QueryIndex> indexesToAdd = new HashSet<>();

        Map<String, QueryIndex> curIndexes = new HashMap<>();

        for (QueryIndex idx : locEntity.getIndexes()) {
            if (curIndexes.put(idx.getName(), idx) != null)
                throw new IllegalStateException("Duplicate key");
        }

        for (QueryIndex qryIdx : target.getIndexes()) {
            if (curIndexes.containsKey(qryIdx.getName())) {
                checkEquals(
                    conflicts,
                    "index " + qryIdx.getName(),
                    curIndexes.get(qryIdx.getName()),
                    qryIdx
                );
            }
            else
                indexesToAdd.add(qryIdx);
        }
        return indexesToAdd;
    }

    /**
     * @param collection Collection for checking.
     * @param elementToCheck Element for checking to containing in collection.
     * @return {@code true} if collection contain elementToCheck.
     */
    private static boolean contains(Collection<String> collection, String elementToCheck) {
        return collection != null && collection.contains(elementToCheck);
    }

    /**
     * @return Value from sourceMap or null if map is null.
     */
    private static <V> V getFromMap(Map<String, V> sourceMap, String key) {
        return sourceMap == null ? null : sourceMap.get(key);
    }

    /**
     * Comparing two objects and add formatted text to conflicts if needed.
     *
     * @param conflicts Storage of conflicts resulting error message.
     * @param name Name of comparing object.
     * @param local Local object.
     * @param received Received object.
     */
    private static <V> void checkEquals(StringBuilder conflicts, String name, V local, V received) {
        if (!Objects.equals(local, received))
            conflicts.append(String.format("%s is different: local=%s, received=%s\n", name, local, received));
    }

    /**
     * Checks if received query entity field has the alias which is already used by a field on the local node.
     *
     * @return Whether conflicts were found.
     */
    private static boolean findAliasConflicts(
        QueryEntity locEntity,
        String targetFieldAlias,
        String targetFieldName,
        StringBuilder conflicts
    ) {
        for (Map.Entry<String, String> entry : locEntity.getAliases().entrySet()) {
            if (Objects.equals(entry.getValue(), targetFieldAlias)) {
                conflicts.append(String.format(
                    "multiple fields are associated with the same alias: alias=%s, localField=%s, receivedField=%s\n",
                    targetFieldAlias,
                    entry.getKey(),
                    targetFieldName)
                );

                return true;
            }
        }

        return false;
    }

    /**
     * Check for conflict in this patch.
     *
     * @return {@code true} if patch has conflict.
     */
    boolean hasConflict() {
        return conflictsMessage != null;
    }

    /**
     * @return {@code true} if patch is empty and can't be applying.
     */
    boolean isEmpty() {
        return patchOperations == null || patchOperations.isEmpty();
    }

    /**
     * @return Conflicts.
     */
    String getConflictsMessage() {
        return conflictsMessage;
    }

    /**
     * @return Patch operations for applying.
     */
    Collection<SchemaAbstractOperation> getPatchOperations() {
        return patchOperations;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(QueryEntityPatch.class, this);
    }
}

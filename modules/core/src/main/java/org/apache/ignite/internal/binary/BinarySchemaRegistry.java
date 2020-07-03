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

package org.apache.ignite.internal.binary;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.jetbrains.annotations.Nullable;

/**
 * Binary schema registry. Contains all well-known object schemas.
 * <p>
 * We rely on the fact that usually object has only few different schemas. For this reason we inline several
 * of them with optional fallback to normal hash map lookup.
 *
 */
public class BinarySchemaRegistry {
    /** Empty schema ID. */
    private static final int EMPTY = 0;

    /** Whether registry still works in inline mode. */
    private volatile boolean inline = true;

    /** First schema ID. */
    private int schemaId1;

    /** Second schema ID. */
    private int schemaId2;

    /** Third schema ID. */
    private int schemaId3;

    /** Fourth schema ID. */
    private int schemaId4;

    /** First schema. */
    private BinarySchema schema1;

    /** Second schema. */
    private BinarySchema schema2;

    /** Third schema. */
    private BinarySchema schema3;

    /** Fourth schema. */
    private BinarySchema schema4;

    /** Schemas with COW semantics. */
    private volatile HashMap<Integer, BinarySchema> schemas;

    /**
     * Get schema for the given ID. We rely on very relaxed memory semantics here assuming that it is not critical
     * to return false-positive {@code null} values.
     *
     * @param schemaId Schema ID.
     * @return Schema or {@code null}.
     */
    @Nullable public BinarySchema schema(int schemaId) {
        if (inline) {
            if (schemaId == schemaId1)
                return schema1;
            else if (schemaId == schemaId2)
                return schema2;
            else if (schemaId == schemaId3)
                return schema3;
            else if (schemaId == schemaId4)
                return schema4;
        }
        else {
            HashMap<Integer, BinarySchema> schemas0 = schemas;

            // Null can be observed here due to either data race or race condition when switching to non-inlined mode.
            // Both of them are benign for us because they lead only to unnecessary schema re-calc.
            if (schemas0 != null)
                return schemas0.get(schemaId);
        }

        return null;
    }

    /**
     * Add schema.
     *
     * @param schemaId Schema ID.
     * @param schema Schema.
     */
    public synchronized void addSchema(int schemaId, BinarySchema schema) {
        if (inline) {
            // Check if this is already known schema.
            if (schemaId == schemaId1 || schemaId == schemaId2 || schemaId == schemaId3 || schemaId == schemaId4)
                return;

            // Try positioning new schema in inline mode.
            if (schemaId1 == EMPTY) {
                schemaId1 = schemaId;

                schema1 = schema;

                inline = true; // Forcing HB edge just in case.

                return;
            }

            if (schemaId2 == EMPTY) {
                schemaId2 = schemaId;

                schema2 = schema;

                inline = true; // Forcing HB edge just in case.

                return;
            }

            if (schemaId3 == EMPTY) {
                schemaId3 = schemaId;

                schema3 = schema;

                inline = true; // Forcing HB edge just in case.

                return;
            }

            if (schemaId4 == EMPTY) {
                schemaId4 = schemaId;

                schema4 = schema;

                inline = true; // Forcing HB edge just in case.

                return;
            }

            // No luck, switching to hash map mode.
            HashMap<Integer, BinarySchema> newSchemas = new HashMap<>();

            newSchemas.put(schemaId1, schema1);
            newSchemas.put(schemaId2, schema2);
            newSchemas.put(schemaId3, schema3);
            newSchemas.put(schemaId4, schema4);

            newSchemas.put(schemaId, schema);

            schemas = newSchemas;

            inline = false;
        }
        else {
            HashMap<Integer, BinarySchema> newSchemas = new HashMap<>(schemas);

            newSchemas.put(schemaId, schema);

            schemas = newSchemas;
        }
    }

    /**
     * @return List of known schemas.
     */
    public synchronized List<BinarySchema> schemas() {
        List<BinarySchema> res = new ArrayList<>();

        if (inline) {
            if (schemaId1 != EMPTY)
                res.add(schema1);
            if (schemaId2 != EMPTY)
                res.add(schema2);
            if (schemaId3 != EMPTY)
                res.add(schema3);
            if (schemaId4 != EMPTY)
                res.add(schema4);
        }
        else
            res.addAll(schemas.values());

        return res;
    }
}

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

package org.apache.ignite.internal.portable;

import java.util.Map;

/**
 * Portable object schema.
 */
public class PortableObjectSchema {
    /** Schema ID. */
    private final int schemaId;

    /** Fields. */
    private final Map<Integer, Integer> fields;

    /**
     * Constructor.
     *
     * @param schemaId Schema ID.
     * @param fields Fields.
     */
    public PortableObjectSchema(int schemaId, Map<Integer, Integer> fields) {
        this.schemaId = schemaId;
        this.fields = fields;
    }

    /**
     * Get schema ID.
     *
     * @return Schema ID.
     */
    public int schemaId() {
        return schemaId;
    }

    /**
     * Get field offset position.
     *
     * @param fieldId Field ID.
     * @return Field offset position.
     */
    public int fieldOffsetPosition(int fieldId) {
        Integer pos = fields.get(fieldId);

        return pos != null ? pos : 0;
    }
}

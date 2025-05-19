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

package org.apache.ignite.internal.processors.query.calcite.schema;

import java.lang.reflect.Type;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.impl.ViewTable;
import org.jetbrains.annotations.Nullable;

/**
 * Ignite SQL view table implementation.
 */
public class ViewTableImpl extends ViewTable {
    /** Fields origins. */
    private final Map<String, List<String>> origins;

    /** Ctor. */
    public ViewTableImpl(
        Type elementType,
        RelProtoDataType rowType,
        String viewSql,
        List<String> schemaPath,
        Map<String, List<String>> origins
    ) {
        super(elementType, rowType, viewSql, schemaPath, null);

        this.origins = Collections.unmodifiableMap(origins);
    }

    /** Get field origin. */
    public @Nullable List<String> fieldOrigin(String fieldName) {
        return origins.get(fieldName);
    }
}

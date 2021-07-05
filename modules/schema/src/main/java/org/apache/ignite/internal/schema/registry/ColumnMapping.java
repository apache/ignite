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

package org.apache.ignite.internal.schema.registry;

import org.apache.ignite.internal.schema.SchemaDescriptor;

/**
 * Column mapping.
 */
class ColumnMapping {
    /** Mapping. */
    private final int[] mapping;

    /** First mapped column index. */
    private final int firstColIdx;

    /**
     * @param schema Source schema descriptor.
     */
    ColumnMapping(SchemaDescriptor schema) {
        firstColIdx = schema.keyColumns().length();
        mapping = new int[schema.valueColumns().length()];
    }

    /**
     * Add column mapping.
     *
     * @param from Column index in source schema.
     * @param to Column index in schema.
     */
    void add(int from, int to) {
        assert from >= firstColIdx && from < firstColIdx + mapping.length;

        mapping[from - firstColIdx] = to;
    }

    /**
     * Gets mapped column idx.
     *
     * @param idx Column index in source.
     * @return Column index in targer schema.
     */
    int map(int idx) {
        if (idx < firstColIdx)
            return idx;

        return mapping[idx - firstColIdx];
    }
}

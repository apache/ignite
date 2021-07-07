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

package org.apache.ignite.internal.schema.mapping;

/**
 * Column mapper implementation.
 */
class ColumnMapperImpl implements ColumnMapper, ColumnMapperBuilder {
    /** Mapping. */
    private final int[] mapping;

    /**
     * @param cols Number of columns.
     */
    ColumnMapperImpl(int cols) {
        mapping = new int[cols];

        for (int i = 0; i < mapping.length; i++)
            mapping[i] = i;
    }

    /** {@inheritDoc} */
    @Override public void add(int from, int to) {
        mapping[from] = to;
    }

    /** {@inheritDoc} */
    @Override public int map(int idx) {
        if (idx > mapping.length)
            return -1;

        return mapping[idx];
    }

    /** {@inheritDoc} */
    @Override public ColumnMapper build() {
        return this;
    }
}

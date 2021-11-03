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

import java.io.Serializable;
import org.apache.ignite.internal.schema.Column;
import org.jetbrains.annotations.NotNull;

/**
 * Column mapper interface.
 */
public interface ColumnMapper extends Serializable {
    /**
     * Add new column.
     *
     * @param col Column descriptor.
     * @return {@code this} for chaining.
     */
    public ColumnMapper add(@NotNull Column col);

    /**
     * Remap column with new index.
     *
     * @param from Source column index.
     * @param to   Target column index.
     * @return {@code this} for chaining.
     */
    public ColumnMapper add(int from, int to);

    /**
     * Map column idx in source schema to column idx in target schema.
     *
     * @param idx Column index in source schema.
     * @return Column index in target schema or {@code -1} if no column exists in target schema.
     */
    int map(int idx);

    /**
     * Returns a column descriptor with proper default for the given column idx if column doesn't exists in target schema ({@link #map(int)}
     * returns {@code -1}).
     *
     * @param idx Column index in source schema.
     * @return Column descriptor or {@code null}.
     */
    Column mappedColumn(int idx);
}

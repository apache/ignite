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

package org.apache.ignite.schema;

import java.util.List;

/**
 * Sorted index descriptor.
 */
public interface SortedIndex extends TableIndex, ColumnarIndex {
    /** {@inheritDoc} */
    @Override List<SortedIndexColumn> columns();

    /** {@inheritDoc} */
    @Override List<SortedIndexColumn> indexedColumns();

    /**
     * Unique index flag.
     * <p>
     * Limitation: Index MUST have all affinity columns declared explicitly.
     * This requirement allows omitting cluster wide constraint checks.
     *
     * @return Unique flag.
     */
    default boolean unique() {
        return false;
    }

    /** {@inheritDoc} */
    @Override default String type() {
        return "SORTED";
    }
}

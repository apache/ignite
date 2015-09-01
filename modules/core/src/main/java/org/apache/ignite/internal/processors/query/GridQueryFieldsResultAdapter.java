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

import java.util.Collections;
import java.util.List;
import org.apache.ignite.internal.util.lang.GridCloseableIterator;
import org.jetbrains.annotations.Nullable;

/**
 * Convenience adapter for {@link GridQueryFieldsResult}.
 */
public class GridQueryFieldsResultAdapter implements GridQueryFieldsResult {
    /** Meta data. */
    private final List<GridQueryFieldMetadata> metaData;

    /** Result iterator. */
    private final GridCloseableIterator<List<?>> it;

    /**
     * Creates query field result composed of field metadata and iterator
     * over queried fields.
     *
     * @param metaData Meta data.
     * @param it Result iterator.
     */
    public GridQueryFieldsResultAdapter(@Nullable List<GridQueryFieldMetadata> metaData,
                                        GridCloseableIterator<List<?>> it) {
        this.metaData = metaData != null ? Collections.unmodifiableList(metaData) : null;
        this.it = it;
    }

    /** {@inheritDoc} */
    @Override public List<GridQueryFieldMetadata> metaData() {
        return metaData;
    }

    /** {@inheritDoc} */
    @Override public GridCloseableIterator<List<?>> iterator() {
        return it;
    }
}
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

package org.apache.ignite.internal.schema.definition.index;

import java.util.Collections;
import java.util.List;
import org.apache.ignite.internal.schema.definition.AbstractSchemaObject;
import org.apache.ignite.internal.tostring.IgniteToStringInclude;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.schema.definition.index.SortedIndexColumnDefinition;
import org.apache.ignite.schema.definition.index.SortedIndexDefinition;

/**
 * Sorted index.
 */
public class SortedIndexDefinitionImpl extends AbstractSchemaObject implements SortedIndexDefinition {
    /** Columns. */
    @IgniteToStringInclude
    private final List<SortedIndexColumnDefinition> cols;

    /** Unique flag. */
    private final boolean unique;

    /**
     * Constructor.
     *
     * @param name Index name.
     * @param cols Index columns.
     * @param unique Unique flag.
     */
    public SortedIndexDefinitionImpl(String name, List<SortedIndexColumnDefinition> cols, boolean unique) {
        super(name);

        this.cols = Collections.unmodifiableList(cols);
        this.unique = unique;
    }

    /** {@inheritDoc} */
    @Override public boolean unique() {
        return unique;
    }

    /** {@inheritDoc} */
    @Override public List<SortedIndexColumnDefinition> columns() {
        return cols;
    }

    /** {@inheritDoc} */
    @Override public List<SortedIndexColumnDefinition> indexedColumns() {
        return cols;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SortedIndexDefinition.class, this,
            "type", type(),
            "name", name());
    }
}

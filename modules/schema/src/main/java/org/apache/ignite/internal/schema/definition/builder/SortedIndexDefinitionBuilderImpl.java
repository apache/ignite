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

package org.apache.ignite.internal.schema.definition.builder;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.ignite.internal.schema.definition.index.SortedIndexColumnDefinitionImpl;
import org.apache.ignite.internal.schema.definition.index.SortedIndexDefinitionImpl;
import org.apache.ignite.schema.definition.builder.SortedIndexDefinitionBuilder;
import org.apache.ignite.schema.definition.index.SortOrder;
import org.apache.ignite.schema.definition.index.SortedIndexColumnDefinition;
import org.apache.ignite.schema.definition.index.SortedIndexDefinition;

/**
 * Sorted index builder.
 */
public class SortedIndexDefinitionBuilderImpl extends AbstractIndexBuilder implements SortedIndexDefinitionBuilder {
    /** Index columns. */
    protected final Map<String, SortedIndexColumnBuilderImpl> cols = new HashMap<>();

    /**
     * Constructor.
     *
     * @param name Index name.
     */
    public SortedIndexDefinitionBuilderImpl(String name) {
        super(name);
    }

    /** {@inheritDoc} */
    @Override
    public SortedIndexDefinitionBuilderImpl withHints(Map<String, String> hints) {
        super.withHints(hints);

        return this;
    }

    /** {@inheritDoc} */
    @Override
    public SortedIndexColumnBuilderImpl addIndexColumn(String name) {
        return new SortedIndexColumnBuilderImpl(this).withName(name);
    }

    /**
     * @param idxBuilder Index builder.
     */
    protected void addIndexColumn(SortedIndexColumnBuilderImpl idxBuilder) {
        if (cols.put(idxBuilder.name(), idxBuilder) != null) {
            throw new IllegalArgumentException("Index with same name already exists: " + idxBuilder.name());
        }
    }

    /** {@inheritDoc} */
    @Override
    public SortedIndexDefinitionBuilderImpl unique(boolean unique) {
        super.unique(unique);

        return this;
    }

    /**
     * @return Index columns.
     */
    public List<SortedIndexColumnDefinition> columns() {
        return cols.values().stream().map(c -> new SortedIndexColumnDefinitionImpl(c.name, c.asc)).collect(Collectors.toList());
    }

    /** {@inheritDoc} */
    @Override
    public SortedIndexDefinition build() {
        assert !cols.isEmpty();

        return new SortedIndexDefinitionImpl(name, columns(), unique());
    }

    /**
     * Index column builder.
     */
    protected static class SortedIndexColumnBuilderImpl implements SortedIndexColumnBuilder {
        /** Index builder. */
        private final SortedIndexDefinitionBuilderImpl parent;

        /** Columns name. */
        protected String name;

        /** Index order flag. */
        protected SortOrder asc = SortOrder.ASC;

        /**
         * Constructor.
         *
         * @param parent Parent builder.
         */
        SortedIndexColumnBuilderImpl(SortedIndexDefinitionBuilderImpl parent) {
            this.parent = parent;
        }

        /** {@inheritDoc} */
        @Override
        public SortedIndexColumnBuilderImpl desc() {
            asc = SortOrder.DESC;

            return this;
        }

        /** {@inheritDoc} */
        @Override
        public SortedIndexColumnBuilderImpl asc() {
            asc = SortOrder.ASC;

            return this;
        }

        /** {@inheritDoc} */
        @Override
        public SortedIndexColumnBuilderImpl withName(String name) {
            this.name = name;

            return this;
        }

        public String name() {
            return name;
        }

        /** {@inheritDoc} */
        @Override
        public SortedIndexDefinitionBuilderImpl done() {
            parent.addIndexColumn(this);

            return parent;
        }
    }
}

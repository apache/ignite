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

package org.apache.ignite.internal.schema.builder;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.ignite.internal.schema.SortedIndexColumnImpl;
import org.apache.ignite.internal.schema.SortedIndexImpl;
import org.apache.ignite.schema.SortOrder;
import org.apache.ignite.schema.SortedIndex;
import org.apache.ignite.schema.SortedIndexColumn;
import org.apache.ignite.schema.builder.SortedIndexBuilder;

/**
 * Sorted index builder.
 */
public class SortedIndexBuilderImpl extends AbstractIndexBuilder implements SortedIndexBuilder {
    /** Index columns. */
    protected final Map<String, SortedIndexColumnBuilderImpl> cols = new HashMap<>();

    /** Unique flag. */
    protected boolean unique;

    /**
     * Constructor.
     *
     * @param name Index name.
     */
    public SortedIndexBuilderImpl(String name) {
        super(name);
    }

    /** {@inheritDoc} */
    @Override public SortedIndexBuilderImpl withHints(Map<String, String> hints) {
        super.withHints(hints);

        return this;
    }

    /** {@inheritDoc} */
    @Override public SortedIndexColumnBuilderImpl addIndexColumn(String name) {
        return new SortedIndexColumnBuilderImpl(this).withName(name);
    }

    /** {@inheritDoc} */
    @Override public SortedIndexBuilderImpl unique() {
        unique = true;

        return this;
    }

    /**
     * @param idxBuilder Index builder.
     */
    protected void addIndexColumn(SortedIndexColumnBuilderImpl idxBuilder) {
        if (cols.put(idxBuilder.name(), idxBuilder) != null)
            throw new IllegalArgumentException("Index with same name already exists: " + idxBuilder.name());
    }

    /**
     * @return Index columns.
     */
    public List<SortedIndexColumn> columns() {
        return cols.values().stream().map(c -> new SortedIndexColumnImpl(c.name, c.asc)).collect(Collectors.toList());
    }

    /** {@inheritDoc} */
    @Override public SortedIndex build() {
        assert !cols.isEmpty();

        return new SortedIndexImpl(
            name,
            columns(),
            unique);
    }

    /**
     * Index column builder.
     */
    protected static class SortedIndexColumnBuilderImpl implements SortedIndexColumnBuilder {
        /** Index builder. */
        private final SortedIndexBuilderImpl parent;

        /** Columns name. */
        protected String name;

        /** Index order flag. */
        protected SortOrder asc = SortOrder.ASC;

        /**
         * Constructor.
         *
         * @param parent Parent builder.
         */
        SortedIndexColumnBuilderImpl(SortedIndexBuilderImpl parent) {
            this.parent = parent;
        }

        /** {@inheritDoc} */
        @Override public SortedIndexColumnBuilderImpl desc() {
            asc = SortOrder.DESC;

            return this;
        }

        /** {@inheritDoc} */
        @Override public SortedIndexColumnBuilderImpl asc() {
            asc = SortOrder.ASC;

            return this;
        }

        /** {@inheritDoc} */
        @Override public SortedIndexColumnBuilderImpl withName(String name) {
            this.name = name;

            return this;
        }

        public String name() {
            return name;
        }

        /** {@inheritDoc} */
        @Override public SortedIndexBuilderImpl done() {
            parent.addIndexColumn(this);

            return parent;
        }
    }
}

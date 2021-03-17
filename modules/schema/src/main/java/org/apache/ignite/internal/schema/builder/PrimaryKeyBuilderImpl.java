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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.ignite.internal.schema.PrimaryIndexImpl;
import org.apache.ignite.schema.PrimaryIndex;
import org.apache.ignite.schema.SortedIndexColumn;
import org.apache.ignite.schema.builder.PrimaryIndexBuilder;

import static org.apache.ignite.schema.PrimaryIndex.PRIMARY_KEY_INDEX_NAME;

/**
 * Primary index builder.
 */
public class PrimaryKeyBuilderImpl extends SortedIndexBuilderImpl implements PrimaryIndexBuilder {
    /** */
    private List<String> affCols;

    /**
     * Constructor.
     */
    public PrimaryKeyBuilderImpl() {
        super(PRIMARY_KEY_INDEX_NAME);

        super.unique();
    }

    /** {@inheritDoc} */
    @Override public PrimaryKeyBuilderImpl withHints(Map<String, String> hints) {
        super.withHints(hints);

        return this;
    }

    /** {@inheritDoc} */
    @Override public PrimaryKeyBuilderImpl unique() {
        return this; // Nothing to do.
    }

    @Override public PkIndexColumnBuilderImpl addIndexColumn(String name) {
        return new PkIndexColumnBuilderImpl(this).withName(name);
    }

    /** {@inheritDoc} */
    @Override public PrimaryKeyBuilderImpl withAffinityColumns(String... affCols) {
        this.affCols = Arrays.asList(affCols);

        return this;
    }

    /** {@inheritDoc} */
    @Override public PrimaryIndex build() {
        if (affCols == null)
            affCols = columns().stream().map(SortedIndexColumn::name).collect(Collectors.toList());
        else
            assert affCols.stream().allMatch(cols::containsKey) : "Affinity column should be a valid PK index column.";

        return new PrimaryIndexImpl(columns(), affCols);
    }

    /**
     * Index column builder.
     */
    private static class PkIndexColumnBuilderImpl extends SortedIndexColumnBuilderImpl implements PrimaryIndexColumnBuilder {
        /**
         * Constructor.
         *
         * @param parent Parent builder.
         */
        PkIndexColumnBuilderImpl(PrimaryKeyBuilderImpl parent) {
            super(parent);
        }

        /** {@inheritDoc} */
        @Override public PkIndexColumnBuilderImpl withName(String name) {
            super.withName(name);

            return this;
        }

        /** {@inheritDoc} */
        @Override public PkIndexColumnBuilderImpl desc() {
            super.desc();

            return this;
        }

        /** {@inheritDoc} */
        @Override public PkIndexColumnBuilderImpl asc() {
            super.asc();

            return this;
        }

        /** {@inheritDoc} */
        @Override public PrimaryKeyBuilderImpl done() {
            return (PrimaryKeyBuilderImpl)super.done();
        }
    }
}

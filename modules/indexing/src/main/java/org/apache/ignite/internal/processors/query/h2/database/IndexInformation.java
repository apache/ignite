/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.h2.database;

import org.jetbrains.annotations.Nullable;

/**
 * Index information.
 */
public class IndexInformation {
    /** */
    private final boolean unique;

    /** */
    private final boolean pk;

    /** */
    private final String name;

    /** */
    private final String type;

    /** */
    private final String keySql;

    /** */
    private final Integer inlineSize;

    /**
     * @param pk PK.
     * @param unique Unique.
     * @param name Name.
     * @param type Type.
     * @param keySql Key sql. Can be {@code null} in case columns key is not applicable for the index.
     * @param inlineSize Inline size. Can be {@code null} in case inline size is not applicable for the index.
     */
    public IndexInformation(boolean pk, boolean unique, String name, H2IndexType type, @Nullable String keySql,
        @Nullable Integer inlineSize) {
        this.pk = pk;
        this.unique = unique;
        this.name = name;
        this.type = type.name();
        this.keySql = keySql;
        this.inlineSize = inlineSize;
    }

    /**
     * @return {@code true} For unique index.
     */
    public boolean unique() {
        return unique;
    }

    /**
     * @return {@code true} For PK index.
     */
    public boolean pk() {
        return pk;
    }

    /**
     * @return Name of index.
     */
    public String name() {
        return name;
    }

    /**
     * @return Type of index.
     */
    public String type() {
        return type;
    }

    /**
     * Get string representation of index key.
     *
     * @return String representation of index key. Can be {@code null} in case columns key is not applicable for the index.
     */
    @Nullable public String keySql() {
        return keySql;
    }

    /**
     * @return Inline size for the index. Can be {@code null} in case inline size is not applicable for the index.
     */
    @Nullable public Integer inlineSize() {
        return inlineSize;
    }
}

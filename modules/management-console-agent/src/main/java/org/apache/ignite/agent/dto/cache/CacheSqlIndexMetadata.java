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

package org.apache.ignite.agent.dto.cache;

import java.util.Collection;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * DTO cache sql index metadata.
 */
public class CacheSqlIndexMetadata {
    /** Index name. */
    private String name;

    /** Fields. */
    private Collection<String> fields;

    /** Descendings. */
    private Collection<String> descendings;

    /** Unique. */
    private boolean unique;

    /**
     * @return Index name.
     */
    public String getName() {
        return name;
    }

    /**
     * @param name Index name.
     * @return {@code This} for chaining method calls.
     */
    public CacheSqlIndexMetadata setName(String name) {
        this.name = name;

        return this;
    }

    /**
     * @return Fields.
     */
    public Collection<String> getFields() {
        return fields;
    }

    /**
     * @param fields Fields.
     * @return {@code This} for chaining method calls.
     */
    public CacheSqlIndexMetadata setFields(Collection<String> fields) {
        this.fields = fields;

        return this;
    }

    /**
     * @return Descendings.
     */
    public Collection<String> getDescendings() {
        return descendings;
    }

    /**
     * @param descendings Descendings.
     * @return {@code This} for chaining method calls.
     */
    public CacheSqlIndexMetadata setDescendings(Collection<String> descendings) {
        this.descendings = descendings;

        return this;
    }

    /**
     * @return @{code True} if it's unique index.
     */
    public boolean isUnique() {
        return unique;
    }

    /**
     * @param unique Unique.
     * @return {@code This} for chaining method calls.
     */
    public CacheSqlIndexMetadata setUnique(boolean unique) {
        this.unique = unique;

        return this;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheSqlIndexMetadata.class, this);
    }
}

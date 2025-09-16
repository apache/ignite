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

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import javax.cache.CacheException;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * Extended query entity with not-null fields support.
 */
public class QueryEntityEx extends QueryEntity {
    /** */
    private static final long serialVersionUID = 0L;

    /** Fields that must have non-null value. */
    private Set<String> notNullFields;

    /** Whether to preserve order specified by {@link #getKeyFields()} or not. */
    private boolean preserveKeysOrder;

    /** Whether a primary key should be autocreated or not. */
    private boolean implicitPk;

    /** Whether absent PK parts should be filled with defaults or not. */
    private boolean fillAbsentPKsWithDefaults;

    /** INLINE_SIZE for PK index. */
    private Integer pkInlineSize;

    /** INLINE_SIZE for affinity field index. */
    private Integer affKeyInlineSize;

    /**
     * Default constructor.
     */
    public QueryEntityEx() {
        // No-op.
    }

    /**
     * Copying constructor.
     *
     * @param other Instance to copy.
     */
    public QueryEntityEx(QueryEntity other) {
        super(other);

        if (other instanceof QueryEntityEx) {
            QueryEntityEx other0 = (QueryEntityEx)other;

            notNullFields = other0.notNullFields != null ? new HashSet<>(other0.notNullFields) : null;

            preserveKeysOrder = other0.preserveKeysOrder;

            implicitPk = other0.implicitPk;

            fillAbsentPKsWithDefaults = other0.fillAbsentPKsWithDefaults;
            pkInlineSize = other0.pkInlineSize != null ? other0.pkInlineSize : -1;
            affKeyInlineSize = other0.affKeyInlineSize != null ? other0.affKeyInlineSize : -1;
        }
    }

    /** {@inheritDoc} */
    @Override @Nullable public Set<String> getNotNullFields() {
        return notNullFields;
    }

    /** {@inheritDoc} */
    @Override public QueryEntity setNotNullFields(@Nullable Set<String> notNullFields) {
        this.notNullFields = notNullFields;

        return this;
    }

    /**
     * @return {@code true} if order should be preserved, {@code false} otherwise.
     */
    public boolean isPreserveKeysOrder() {
        return preserveKeysOrder;
    }

    /**
     * @param preserveKeysOrder Whether the order should be preserved or not.
     * @return {@code this} for chaining.
     */
    public QueryEntity setPreserveKeysOrder(boolean preserveKeysOrder) {
        this.preserveKeysOrder = preserveKeysOrder;

        return this;
    }

    /** */
    public boolean implicitPk() {
        return implicitPk;
    }

    /** */
    public QueryEntity implicitPk(boolean implicitPk) {
        this.implicitPk = implicitPk;

        return this;
    }

    /**
     * @return {@code true} if absent PK parts should be filled with defaults, {@code false} otherwise.
     */
    public boolean fillAbsentPKsWithDefaults() {
        return fillAbsentPKsWithDefaults;
    }

    /**
     * @param fillAbsentPKsWithDefaults Whether absent PK parts should be filled with defaults or not.
     * @return {@code this} for chaining.
     */
    public QueryEntity fillAbsentPKsWithDefaults(boolean fillAbsentPKsWithDefaults) {
        this.fillAbsentPKsWithDefaults = fillAbsentPKsWithDefaults;

        return this;
    }

    /**
     * Returns INLINE_SIZE for PK index.
     *
     * @return INLINE_SIZE for PK index.
     */
    public Integer getPrimaryKeyInlineSize() {
        return pkInlineSize;
    }

    /**
     * Sets INLINE_SIZE for PK index.
     *
     * @param pkInlineSize INLINE_SIZE for PK index, when {@code null} - inline size is calculated automativally.
     * @return {@code this} for chaining.
     */
    public QueryEntity setPrimaryKeyInlineSize(Integer pkInlineSize) {
        if (pkInlineSize != null && pkInlineSize < 0) {
            throw new CacheException("Inline size for sorted primary key cannot be negative. "
                + "[inlineSize=" + pkInlineSize + ']');
        }

        this.pkInlineSize = pkInlineSize;

        return this;
    }

    /**
     * Returns INLINE_SIZE for affinity field index.
     *
     * @return INLINE_SIZE for affinity field index.
     */
    public Integer getAffinityKeyInlineSize() {
        return affKeyInlineSize;
    }

    /**
     * Sets INLINE_SIZE for AFFINITY_KEY index.
     *
     * @param affKeyInlineSize INLINE_SIZE for AFFINITY_KEY index, when {@code null} - inline size is calculated automativally.
     * @return {@code this} for chaining.
     */
    public QueryEntity setAffinityKeyInlineSize(Integer affKeyInlineSize) {
        if (affKeyInlineSize != null && affKeyInlineSize < 0) {
            throw new CacheException("Inline size for affinity fieled index cannot be negative. "
                + "[inlineSize=" + affKeyInlineSize + ']');
        }

        this.affKeyInlineSize = affKeyInlineSize;

        return this;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        QueryEntityEx entity = (QueryEntityEx)o;

        return super.equals(entity) && Objects.equals(notNullFields, entity.notNullFields)
            && preserveKeysOrder == entity.preserveKeysOrder
            && implicitPk == entity.implicitPk
            && Objects.equals(pkInlineSize, entity.pkInlineSize)
            && Objects.equals(affKeyInlineSize, entity.affKeyInlineSize);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = super.hashCode();

        res = 31 * res + (notNullFields != null ? notNullFields.hashCode() : 0);
        res = 31 * res + (preserveKeysOrder ? 1 : 0);
        res = 31 * res + (implicitPk ? 1 : 0);
        res = 31 * res + (pkInlineSize != null ? pkInlineSize.hashCode() : 0);
        res = 31 * res + (affKeyInlineSize != null ? affKeyInlineSize.hashCode() : 0);
        return res;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(QueryEntityEx.class, this);
    }
}

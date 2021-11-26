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
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.internal.util.typedef.F;
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

    /**
     * Used for compisite primary key.
     * {@code true} if the PK index is created on fields of PK;
     * {@code false} in case the PK index is created on the whole key (composite binary object).
     * {@code null} - compatible behavior (unwrap for a table created by SQL and wrapped key for a table created by API).
     */
    private Boolean unwrapPk;

    /** INLINE_SIZE for PK index. */
    private Integer pkInlineSize = -1;

    /** INLINE_SIZE for affinity field index. */
    private Integer affFieldInlineSize = -1;

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

            unwrapPk = other0.unwrapPk;
            pkInlineSize = other0.pkInlineSize != null ? other0.pkInlineSize : -1;
            affFieldInlineSize = other0.affFieldInlineSize != null ? other0.affFieldInlineSize : -1;
        }
    }

    /**
     * Copy all members of the class (doesn't copy the member of the parent class).
     *
     * @param other Instance to copy.
     * @return {@code this} for chaining.
     */
    public QueryEntityEx copyExtended(QueryEntityEx other) {
        notNullFields = other.notNullFields != null ? new HashSet<>(other.notNullFields) : null;

        preserveKeysOrder = other.preserveKeysOrder;

        unwrapPk = other.unwrapPk;
        pkInlineSize = other.pkInlineSize != null ? other.pkInlineSize : -1;
        affFieldInlineSize = other.affFieldInlineSize != null ? other.affFieldInlineSize : -1;

        return this;
    }

    /** {@inheritDoc} */
    @Override @Nullable public Set<String> getNotNullFields() {
        return notNullFields;
    }

    /** {@inheritDoc} */
    @Override public QueryEntityEx setNotNullFields(@Nullable Set<String> notNullFields) {
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
    public QueryEntityEx setPreserveKeysOrder(boolean preserveKeysOrder) {
        this.preserveKeysOrder = preserveKeysOrder;

        return this;
    }

    /** {@inheritDoc} */
    @Override public int getPrimaryKeyInlineSize() {
        return pkInlineSize != null ? pkInlineSize : -1;
    }

    /**
     * Sets INLINE_SIZE for PK index. Implemented at the child.
     *
     * @param pkInlineSize INLINE_SIZE for PK index.
     * @return {@code this} for chaining.
     */
    public QueryEntityEx setPrimaryKeyInlineSize(int pkInlineSize) {
        this.pkInlineSize = pkInlineSize;

        return this;
    }

    /** {@inheritDoc} */
    @Override public int getAffinityFieldInlineSize() {
        return affFieldInlineSize != null ? affFieldInlineSize : -1;
    }

    /**
     * Sets INLINE_SIZE for AFFINITY_KEY index. Implemented at the child.
     *
     * @param affFieldInlineSize INLINE_SIZE for AFFINITY_KEY index.
     * @return {@code this} for chaining.
     */
    public QueryEntityEx setAffinityKeyInlineSize(int affFieldInlineSize) {
        this.affFieldInlineSize = affFieldInlineSize;

        return this;
    }

    /** {@inheritDoc} */
    @Override public Boolean isUnwrapPrimaryKeyFields() {
        return unwrapPk;
    }

    /**
     * The property is used for compisite primary key.
     *
     * @param unwrapPk {@code true} if the PK index is created on fields of PK;
     *                             {@code false} in case the PK index is created on the whole key (composite binary object).
     *                             {@code null} - compatible behavior (unwrap for a table created by SQL and wrapped key
     *                             for a table created by API).
     * @return {@code this} for chaining.
     */
    public QueryEntityEx setUnwrapPrimaryKeyFields(Boolean unwrapPk) {
        this.unwrapPk = unwrapPk;

        return this;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        QueryEntityEx entity = (QueryEntityEx)o;

        return super.equals(entity) && F.eq(notNullFields, entity.notNullFields)
                && preserveKeysOrder == entity.preserveKeysOrder
                && unwrapPk == entity.unwrapPk
                && equalsIntegersWithDefault(pkInlineSize, entity.pkInlineSize, -1)
                && equalsIntegersWithDefault(affFieldInlineSize, entity.affFieldInlineSize, -1);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = super.hashCode();

        res = 31 * res + (notNullFields != null ? notNullFields.hashCode() : 0);
        res = 31 * res + (preserveKeysOrder ? 1 : 0);
        res = 31 * res + Objects.hashCode(unwrapPk);
        res = 31 * res + (pkInlineSize != null ? pkInlineSize.hashCode() : Integer.hashCode(-1));
        res = 31 * res + (affFieldInlineSize != null ? affFieldInlineSize.hashCode() : Integer.hashCode(-1));

        return res;
    }

    /** */
    private static boolean equalsIntegersWithDefault(Integer i0, Integer i1, int dflt) {
        return (i0 == i1 || (i0 == null && i1 == dflt) || (i1 == null && i0 == dflt));
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(QueryEntityEx.class, this);
    }
}

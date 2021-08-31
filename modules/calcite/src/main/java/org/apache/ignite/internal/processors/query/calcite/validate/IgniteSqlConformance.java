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
package org.apache.ignite.internal.processors.query.calcite.validate;

import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlConformanceEnum;

/** Ignite's {@link SqlConformance) based on {@link SqlConformanceEnum#DEFAULT}. */
public enum IgniteSqlConformance implements SqlConformance {
    /** Ignite's SqlConformance instance. */
    DEFAULT;

    /** The implementation this conformance is based on. */
    private static final SqlConformance IMPL = SqlConformanceEnum.DEFAULT;

    /** {@inheritDoc} */
    @Override public boolean isLiberal() {
        return IMPL.isLiberal();
    }

    /** {@inheritDoc} */
    @Override public boolean allowCharLiteralAlias() {
        return IMPL.allowCharLiteralAlias();
    }

    /** {@inheritDoc} */
    @Override public boolean isGroupByAlias() {
        return IMPL.isGroupByAlias();
    }

    /** {@inheritDoc} */
    @Override public boolean isGroupByOrdinal() {
        return IMPL.isGroupByOrdinal();
    }

    /** {@inheritDoc} */
    @Override public boolean isHavingAlias() {
        return IMPL.isHavingAlias();
    }

    /** {@inheritDoc} */
    @Override public boolean isSortByOrdinal() {
        return IMPL.isSortByOrdinal();
    }

    /** {@inheritDoc} */
    @Override public boolean isSortByAlias() {
        return IMPL.isSortByAlias();
    }

    /** {@inheritDoc} */
    @Override public boolean isSortByAliasObscures() {
        return IMPL.isSortByAliasObscures();
    }

    /** {@inheritDoc} */
    @Override public boolean isFromRequired() {
        return IMPL.isFromRequired();
    }

    /** {@inheritDoc} */
    @Override public boolean splitQuotedTableName() {
        return IMPL.splitQuotedTableName();
    }

    /** {@inheritDoc} */
    @Override public boolean allowHyphenInUnquotedTableName() {
        return IMPL.allowHyphenInUnquotedTableName();
    }

    /** {@inheritDoc} */
    @Override public boolean isBangEqualAllowed() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean isPercentRemainderAllowed() {
        return IMPL.isPercentRemainderAllowed();
    }

    /** {@inheritDoc} */
    @Override public boolean isMinusAllowed() {
        return IMPL.isMinusAllowed();
    }

    /** {@inheritDoc} */
    @Override public boolean isApplyAllowed() {
        return IMPL.isApplyAllowed();
    }

    /** {@inheritDoc} */
    @Override public boolean isInsertSubsetColumnsAllowed() {
        return IMPL.isInsertSubsetColumnsAllowed();
    }

    /** {@inheritDoc} */
    @Override public boolean allowAliasUnnestItems() {
        return IMPL.allowAliasUnnestItems();
    }

    /** {@inheritDoc} */
    @Override public boolean allowNiladicParentheses() {
        return IMPL.allowNiladicParentheses();
    }

    /** {@inheritDoc} */
    @Override public boolean allowExplicitRowValueConstructor() {
        return IMPL.allowExplicitRowValueConstructor();
    }

    /** {@inheritDoc} */
    @Override public boolean allowExtend() {
        return IMPL.allowExtend();
    }

    /** {@inheritDoc} */
    @Override public boolean isLimitStartCountAllowed() {
        return IMPL.isLimitStartCountAllowed();
    }

    /** {@inheritDoc} */
    @Override public boolean allowGeometry() {
        return IMPL.allowGeometry();
    }

    /** {@inheritDoc} */
    @Override public boolean shouldConvertRaggedUnionTypesToVarying() {
        return IMPL.shouldConvertRaggedUnionTypesToVarying();
    }

    /** {@inheritDoc} */
    @Override public boolean allowExtendedTrim() {
        return IMPL.allowExtendedTrim();
    }

    /** {@inheritDoc} */
    @Override public boolean allowPluralTimeUnits() {
        return IMPL.allowPluralTimeUnits();
    }

    /** {@inheritDoc} */
    @Override public boolean allowQualifyingCommonColumn() {
        return IMPL.allowQualifyingCommonColumn();
    }
}

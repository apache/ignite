/*
 * Copyright 2018-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.springdata20.repository.query;

import java.util.Collections;
import java.util.List;
import org.jetbrains.annotations.Nullable;
import org.springframework.util.Assert;

/**
 * NULL-Object pattern implementation for {@link DeclaredQuery}.
 *
 * @author Jens Schauder
 */
class EmptyDeclaredQuery implements DeclaredQuery {
    /**
     * An implementation implementing the NULL-Object pattern for situations where there is no query.
     */
    static final DeclaredQuery EMPTY_QUERY = new EmptyDeclaredQuery();

    /** {@inheritDoc} */
    @Override public boolean hasNamedParameter() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public String getQueryString() {
        return "";
    }

    /** {@inheritDoc} */
    @Override public String getAlias() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean hasConstructorExpression() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isDefaultProjection() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public List<StringQuery.ParameterBinding> getParameterBindings() {
        return Collections.emptyList();
    }

    /** {@inheritDoc} */
    @Override public DeclaredQuery deriveCountQuery(@Nullable String cntQry, @Nullable String cntQryProjection) {
        Assert.hasText(cntQry, "CountQuery must not be empty!");
        return DeclaredQuery.of(cntQry);
    }

    /** {@inheritDoc} */
    @Override public boolean usesJdbcStyleParameters() {
        return false;
    }
}

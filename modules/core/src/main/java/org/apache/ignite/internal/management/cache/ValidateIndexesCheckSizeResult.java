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

package org.apache.ignite.internal.management.cache;

import java.util.Collection;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;

import static java.util.Collections.emptyList;

/**
 * Result of checking size cache and index.
 */
public class ValidateIndexesCheckSizeResult extends IgniteDataTransferObject {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Cache size. */
    @Order(value = 0)
    long cacheSize;

    /** Issues. */
    @Order(value = 1)
    @GridToStringExclude
    Collection<ValidateIndexesCheckSizeIssue> issues;

    /**
     * Default constructor.
     */
    public ValidateIndexesCheckSizeResult() {
        //Default constructor required for Externalizable.
    }

    /**
     * Constructor.
     *
     * @param cacheSize Cache size.
     * @param issues Issues.
     */
    public ValidateIndexesCheckSizeResult(long cacheSize, Collection<ValidateIndexesCheckSizeIssue> issues) {
        this.cacheSize = cacheSize;
        this.issues = issues;
    }

    /**
     * Return issues when checking size of cache and index.
     *
     * @return Issues when checking size of cache and index.
     */
    public Collection<ValidateIndexesCheckSizeIssue> issues() {
        return issues == null ? emptyList() : issues;
    }

    /**
     * Return cache size.
     *
     * @return Cache size.
     */
    public long cacheSize() {
        return cacheSize;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ValidateIndexesCheckSizeResult.class, this);
    }
}

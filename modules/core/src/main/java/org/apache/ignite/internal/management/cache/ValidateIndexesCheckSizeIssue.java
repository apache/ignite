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

import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * Issue when checking size of cache and index.
 */
public class ValidateIndexesCheckSizeIssue extends IgniteDataTransferObject {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Index name. */
    String idxName;

    /** Index size. */
    long idxSize;

    /** Error. */
    @GridToStringExclude
    Throwable t;

    /**
     * Default constructor.
     */
    public ValidateIndexesCheckSizeIssue() {
        //Default constructor required for Externalizable.
    }

    /**
     * Constructor.
     *
     * @param idxName    Index name.
     * @param idxSize    Index size.
     * @param t          Error.
     */
    public ValidateIndexesCheckSizeIssue(@Nullable String idxName, long idxSize, @Nullable Throwable t) {
        this.idxName = idxName;
        this.idxSize = idxSize;
        this.t = t;
    }

    /**
     * Return index size.
     *
     * @return Index size.
     */
    public long indexSize() {
        return idxSize;
    }

    /**
     * Return error.
     *
     * @return Error.
     */
    public Throwable error() {
        return t;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ValidateIndexesCheckSizeIssue.class, this, "err", t);
    }
}

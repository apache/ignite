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

package org.apache.ignite.internal.processors.query.index;

import org.apache.ignite.internal.util.typedef.internal.S;

import java.util.UUID;

/**
 * Arguments for {@code CREATE INDEX}.
 */
public class IndexDropOperation extends IndexAbstractOperation {
    /** */
    private static final long serialVersionUID = 0L;

    /** Index name. */
    private final String idxName;

    /** Ignore operation if index doesn't exist. */
    private final boolean ifExists;

    /**
     * Constructor.
     *
     * @param cliNodeId Client node ID.
     * @param opId Operation id.
     * @param space Space.
     * @param idxName Index name.
     * @param ifExists Ignore operation if index doesn't exist.
     */
    public IndexDropOperation(UUID cliNodeId, UUID opId, String space, String idxName, boolean ifExists) {
        super(cliNodeId, opId, space);

        this.idxName = idxName;
        this.ifExists = ifExists;
    }

    /** {@inheritDoc} */
    @Override public String indexName() {
        return idxName;
    }

    /**
     * @return Ignore operation if index doesn't exist.
     */
    public boolean ifExists() {
        return ifExists;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IndexDropOperation.class, this);
    }
}

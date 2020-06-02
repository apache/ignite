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

package org.apache.ignite.internal.processors.cache.persistence;

import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 *
 */
class Metas {
    /** */
    @GridToStringInclude
    public final RootPage reuseListRoot;

    /** */
    @GridToStringInclude
    public final RootPage treeRoot;

    /** */
    @GridToStringInclude
    public final RootPage pendingTreeRoot;

    /** */
    @GridToStringInclude
    public final RootPage partMetastoreReuseListRoot;

    /**
     * @param treeRoot Metadata storage root.
     * @param reuseListRoot Reuse list root.
     */
    Metas(RootPage treeRoot, RootPage reuseListRoot, RootPage pendingTreeRoot, RootPage partMetastoreReuseListRoot) {
        this.treeRoot = treeRoot;
        this.reuseListRoot = reuseListRoot;
        this.pendingTreeRoot = pendingTreeRoot;
        this.partMetastoreReuseListRoot = partMetastoreReuseListRoot;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(Metas.class, this);
    }
}

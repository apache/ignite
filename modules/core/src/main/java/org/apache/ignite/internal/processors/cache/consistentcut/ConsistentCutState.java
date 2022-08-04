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

package org.apache.ignite.internal.processors.cache.consistentcut;

import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * Immutable snapshot of Consistent Cut state on local node.
 */
class ConsistentCutState {
    /**
     * Consistent Cut on local node. {@code null} if Consistent Cut isn't running.
     */
    @GridToStringInclude
    private final @Nullable ConsistentCut cut;

    /**
     * Version of the latest created {@link #cut}.
     */
    @GridToStringInclude
    private final ConsistentCutVersion cutVer;

    /** */
    ConsistentCutState(ConsistentCutVersion cutVer, @Nullable ConsistentCut cut) {
        this.cut = cut;
        this.cutVer = cutVer;
    }

    /** */
    ConsistentCut cut() {
        return cut;
    }

    /** */
    ConsistentCutVersion version() {
        return cutVer;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ConsistentCutState.class, this);
    }
}

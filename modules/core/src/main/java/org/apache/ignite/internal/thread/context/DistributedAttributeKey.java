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

package org.apache.ignite.internal.thread.context;

import org.apache.ignite.internal.processors.rollingupgrade.feature.IgniteFeature;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.thread.context.OperationContextDispatcher.MAX_ATTRS_CNT;

/**
 * Represents key that is used to consistently identify {@link OperationContext} attributes across
 * all nodes in the cluster.
 *
 * @see DistributedAttributeKeyRegistry
 */
public final class DistributedAttributeKey {
    /** */
    @GridToStringInclude
    private final byte id;

    /** */
    @GridToStringInclude
    @Nullable private final IgniteFeature introducedBy;

    /** */
    DistributedAttributeKey(int id) {
        this(id, null);
    }

    /** */
    DistributedAttributeKey(int id, @Nullable IgniteFeature introducedBy) {
        assert 0 <= id && id < MAX_ATTRS_CNT : "Invalid distributed attribute id [id=" + id + ']';

        this.id = (byte)id;
        this.introducedBy = introducedBy;
    }

    /** @return Cluster-wide id of the attribute. */
    public byte id() {
        return id;
    }

    /** @return Feature that introduced the attribute, or {@code null} if every peer reads it. */
    public @Nullable IgniteFeature introducedBy() {
        return introducedBy;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DistributedAttributeKey.class, this);
    }
}

/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.util.lang.gridfunc;

import java.util.UUID;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgnitePredicate;

/**
 * UUID equals predicate.
 */
public class EqualsUuidPredicate implements IgnitePredicate<UUID> {
    /** */
    private static final long serialVersionUID = -5664060422647374863L;

    /** */
    private final UUID nodeId;

    /**
     * @param nodeId Node ID for which returning predicate will evaluate to {@code true}.
     */
    public EqualsUuidPredicate(UUID nodeId) {
        this.nodeId = nodeId;
    }

    /** {@inheritDoc} */
    @Override public boolean apply(UUID id) {
        return id.equals(nodeId);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(EqualsUuidPredicate.class, this);
    }
}

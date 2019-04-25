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
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteClosure;

/**
 * Grid node to node ID transformer closure.
 */
public class ClusterNodeGetIdClosure implements IgniteClosure<ClusterNode, UUID> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override public UUID apply(ClusterNode n) {
        return n.id();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ClusterNodeGetIdClosure.class, this);
    }
}

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

package org.apache.ignite.internal.util.lang.gridfunc;

import java.util.UUID;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgnitePredicate;

/**
 * {@link ClusterNode} has equal id predicate.
 */
public class HasEqualIdPredicate<T extends ClusterNode> implements IgnitePredicate<T> {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final UUID locNodeId;

    /**
     * @param locNodeId Id for check.
     */
    public HasEqualIdPredicate(UUID locNodeId) {
        this.locNodeId = locNodeId;
    }

    /** {@inheritDoc} */
    @Override public boolean apply(T n) {
        return n.id().equals(locNodeId);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(HasEqualIdPredicate.class, this);
    }
}

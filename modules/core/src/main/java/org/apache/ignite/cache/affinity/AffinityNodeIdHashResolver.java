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

package org.apache.ignite.cache.affinity;

import java.io.Serializable;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Node hash resolver which uses generated node ID as node hash value. As new node ID is generated
 * on each node start, this resolver do not provide ability to map keys to the same nodes after restart.
 *
 * @deprecated Use {@link IgniteConfiguration#setConsistentId(Serializable)} instead.
 */
@Deprecated
public class AffinityNodeIdHashResolver implements AffinityNodeHashResolver {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override public Object resolve(ClusterNode node) {
        return node.id();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(AffinityNodeIdHashResolver.class, this);
    }
}
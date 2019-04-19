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

package org.apache.ignite.internal.processors.cache.distributed;

import java.util.Arrays;
import java.util.Collection;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.lang.IgnitePredicate;

import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_IGNITE_INSTANCE_NAME;

/**
 *
 */
public class TestCacheNodeExcludingFilter implements IgnitePredicate<ClusterNode> {
    /** */
    private Collection<String> excludeNodes;

    /**
     * @param excludeNodes Nodes names.
     */
    public TestCacheNodeExcludingFilter(Collection<String> excludeNodes) {
        this.excludeNodes = excludeNodes;
    }
    /**
     * @param excludeNodes Nodes names.
     */
    public TestCacheNodeExcludingFilter(String... excludeNodes) {
        this.excludeNodes = Arrays.asList(excludeNodes);
    }

    /** {@inheritDoc} */
    @Override public boolean apply(ClusterNode clusterNode) {
        String name = clusterNode.attribute(ATTR_IGNITE_INSTANCE_NAME).toString();

        return !excludeNodes.contains(name);
    }
}

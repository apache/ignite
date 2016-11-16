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

package org.apache.ignite.internal.processors.affinity;

import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.lang.IgnitePredicate;

/**
 *
 */
public class AffinityConfiguration {
    /** */
    private final AffinityFunction aff;

    /** */
    private final IgnitePredicate<ClusterNode> nodeFilter;

    /** */
    private final int backups;

    /**
     * @param aff
     * @param nodeFilter
     * @param backups
     */
    public AffinityConfiguration(AffinityFunction aff, IgnitePredicate<ClusterNode> nodeFilter, int backups) {
        this.aff = aff;
        this.nodeFilter = nodeFilter;
        this.backups = backups;
    }

    public AffinityFunction affinityFunction() {
        return aff;
    }

    public IgnitePredicate<ClusterNode> nodeFilter() {
        return nodeFilter;
    }

    public int backups() {
        return backups;
    }
}

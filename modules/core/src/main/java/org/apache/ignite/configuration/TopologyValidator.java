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

package org.apache.ignite.configuration;

import java.io.Serializable;
import java.util.Collection;
import javax.cache.CacheException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;

/**
 * Topology validator checks whether the new topology is valid for specific cache at each topology change.
 * <p>
 * Topology is always valid in case no topology validator used.
 * <p>
 * In case topology is valid for specific cache all operations on this cache are allowed.
 * <p>
 * In case topology is not valid for specific cache all update operations on this cache are restricted:
 * <ul>
 * <li>{@link CacheException} will be thrown at update operations (put, remove, etc) attempt.</li>
 * <li>{@link IgniteException} will be thrown at transaction commit attempt.</li>
 * </ul>
 * Usage example
 * <p>
 * Following validator allows to put data only in case topology contains exactly 2 nodes:
 * <pre>{@code
 * new TopologyValidator() {
 *    public boolean validate(Collection<ClusterNode> nodes) {
 *       return nodes.size() == 2;
 *    }
 * }
 * }</pre>
 */
public interface TopologyValidator extends Serializable {
    /**
     * Validates topology.
     * @param nodes Collection of nodes.
     * @return {@code true} in case topology is valid for specific cache, otherwise {@code false}
     */
    boolean validate(Collection<ClusterNode> nodes);
}
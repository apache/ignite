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
import org.apache.ignite.cluster.ClusterNode;

/**
 * Topology validator.
 */
public interface TopologyValidator extends Serializable {
    /**
     * Validates topology.
     * @param nodes Collection of nodes.
     * @return {@code true} in case topology is valid for specific cache, otherwise {@code false}
     */
    boolean validate(Collection<ClusterNode> nodes);
}
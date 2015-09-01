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

/**
 * Resolver which is used to provide node hash value for affinity function.
 * <p>
 * Node IDs constantly change when nodes get restarted, which causes affinity mapping to change between restarts,
 * and hence causing redundant repartitioning. Providing an alternate node hash value, which survives node restarts,
 * will help to map keys to the same nodes whenever possible.
 * <p>
 * Note that on case clients exist they will query this object from the server and use it for affinity calculation.
 * Therefore you must ensure that server and clients can marshal and unmarshal this object in portable format,
 * i.e. all parties have object class(es) configured as portable.
 *
 * @deprecated Use {@link IgniteConfiguration#setConsistentId(Serializable)} instead.
 */
@Deprecated
public interface AffinityNodeHashResolver extends Serializable {
    /**
     * Resolve alternate hash value for the given Grid node.
     *
     * @param node Grid node.
     * @return Resolved hash ID.
     */
    @Deprecated
    public Object resolve(ClusterNode node);
}
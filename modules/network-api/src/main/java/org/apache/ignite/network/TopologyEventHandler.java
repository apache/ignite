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
package org.apache.ignite.network;

/**
 * Interface for handling events related to topology changes.
 */
public interface TopologyEventHandler {
    /**
     * Called when a new member has been detected joining a cluster.
     *
     * @param member Appeared cluster member.
     */
    void onAppeared(ClusterNode member);

    /**
     * Indicates that a member has left a cluster. This method is only called when a member leaves permanently (i.e.
     * it is not possible to re-establish a connection to it).
     *
     * @param member Disappeared cluster member.
     */
    void onDisappeared(ClusterNode member);
}

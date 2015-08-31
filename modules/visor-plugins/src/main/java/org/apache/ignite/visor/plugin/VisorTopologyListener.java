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

package org.apache.ignite.visor.plugin;

import java.util.UUID;

/**
 * Listener for grid node topology changes.
 */
public interface VisorTopologyListener {
    /**
     * Action that should be done on node join.
     *
     * @param nid ID of node that join topology.
     */
    public void onNodeJoin(UUID nid);

    /**
     * Action that should be done on node left.
     *
     * @param nid ID of node that left topology.
     */
    public void onNodeLeft(UUID nid);

    /**
     * Action that should be done on node failed.
     *
     * @param nid ID of failed node.
     */
    public void onNodeFailed(UUID nid);

    /**
     * Action that should be done on node segmented.
     *
     * @param nid ID of segmented node.
     */
    public void onNodeSegmented(UUID nid);
}
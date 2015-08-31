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

package org.apache.ignite.internal.managers.communication;

import java.util.UUID;

/**
 * Node disconnect listener interface.
 */
public interface GridDisconnectListener {
    /**
     * Callback invoked when connection between nodes is closed. This callback invocation does not necessarily
     * mean that node failed or some messages got lost. However, there is such possibility, so listeners can
     * set up timeout objects to check for lost responses.
     *
     * @param nodeId Node ID for which connection was closed.
     */
    public void onNodeDisconnected(UUID nodeId);
}
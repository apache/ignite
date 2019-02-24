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

import java.util.EventListener;
import java.util.UUID;
import org.apache.ignite.internal.util.nio.channel.IgniteSocketChannel;

/**
 * Listener for connections established from remote nodes.
 */
public interface GridIoChannelListener extends EventListener {
    /**
     * The creation event of {@link IgniteSocketChannel} from remote connection.
     *
     * @param nodeId The remote node id.
     * @param channel Local created channel endpoint.
     */
    public void onChannelCreated(UUID nodeId, IgniteSocketChannel channel);
}

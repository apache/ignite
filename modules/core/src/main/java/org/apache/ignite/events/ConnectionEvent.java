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

package org.apache.ignite.events;

import java.net.SocketAddress;
import org.apache.ignite.cluster.ClusterNode;

/**
 * TODO add comment
 */
public class ConnectionEvent extends EventAdapter {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final SocketAddress locAddr;

    /** */
    private final SocketAddress rmtAddr;

    /**
     * @param node Node.
     * @param msg Message.
     * @param type Type.
     */
    public ConnectionEvent(ClusterNode node, String msg, int type) {
        super(node, msg, type);

        this.locAddr = null;
        this.rmtAddr = null;
    }

    /**
     * @param node Node.
     * @param msg Message.
     * @param type Type.
     * @param locAddr Local address.
     * @param rmtAddr Remote addres.
     */
    public ConnectionEvent(
        ClusterNode node,
        String msg,
        int type,
        SocketAddress locAddr,
        SocketAddress rmtAddr
    ) {
        super(node, msg, type);

        this.locAddr = locAddr;
        this.rmtAddr = rmtAddr;
    }
}

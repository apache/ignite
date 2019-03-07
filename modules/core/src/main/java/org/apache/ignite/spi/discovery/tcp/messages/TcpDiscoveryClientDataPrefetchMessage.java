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

package org.apache.ignite.spi.discovery.tcp.messages;

import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.util.typedef.internal.S;

/** */
public class TcpDiscoveryClientDataPrefetchMessage extends TcpDiscoveryAbstractMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private Map<Integer, byte[]> prefetchData;

    /**
     * Constructor.
     *  @param creatorNodeId Creator node ID.
     *
     */
    public TcpDiscoveryClientDataPrefetchMessage(UUID creatorNodeId, Map<Integer, byte[]> prefetchData) {
        super(creatorNodeId);

        this.prefetchData = prefetchData;
    }

    /** */
    public Map<Integer, byte[]> prefetchData() {
        return prefetchData;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TcpDiscoveryClientDataPrefetchMessage.class, this, "super", super.toString());
    }
}
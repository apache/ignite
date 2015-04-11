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

import org.apache.ignite.internal.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 *
 */
public class TcpDiscoveryClassResponse extends TcpDiscoveryAbstractMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private String errMsg;

    /** */
    private byte[] clsBytes;

    /**
     * @param creatorNodeId Creator node ID.
     * @param clsBytes Class bytes.
     */
    public TcpDiscoveryClassResponse(UUID creatorNodeId, byte[] clsBytes) {
        super(creatorNodeId);

        this.clsBytes = clsBytes;
    }

    /**
     * @param creatorNodeId Creator node ID.
     * @param errMsg Error message.
     */
    public TcpDiscoveryClassResponse(UUID creatorNodeId, String errMsg) {
        super(creatorNodeId);

        this.errMsg = errMsg;
    }

    /**
     * @return Error if class loading failed.
     */
    @Nullable public String error() {
        return errMsg;
    }

    /**
     * @return Loaded class bytes.
     */
    public byte[] classBytes() {
        return clsBytes;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TcpDiscoveryClassResponse.class, this, "super", super.toString());
    }
}

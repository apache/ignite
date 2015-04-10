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

import java.util.*;

/**
 *
 */
public class TcpDiscoveryClassRequest extends TcpDiscoveryAbstractMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private String clsName;

    /**
     * @param creatorNodeId Creator node ID.
     * @param clsName Class name.
     */
    public TcpDiscoveryClassRequest(UUID creatorNodeId, String clsName) {
        super(creatorNodeId);

        this.clsName = clsName;
    }

    /**
     * @return Class name.
     */
    public String className() {
        return clsName;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TcpDiscoveryClassRequest.class, this, "super", super.toString());
    }
}

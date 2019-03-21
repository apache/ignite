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

package org.apache.ignite.internal.managers.discovery;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridComponent;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.apache.ignite.spi.discovery.tcp.internal.DiscoveryTcpSpiDataExchange;

/** */
class DiscoveryTcpSpiDataExchangeImpl implements DiscoveryTcpSpiDataExchange {
    /** Kernal context. */
    private final GridKernalContext ctx;

    /** Logger. */
    private final IgniteLogger log;

    /** JDK marshaller. */
    private final JdkMarshaller marshaller;

    /** */
    public DiscoveryTcpSpiDataExchangeImpl(GridKernalContext ctx) {
        this.ctx = ctx;

        log = ctx.log(getClass());

        marshaller = ctx.marshallerContext().jdkMarshaller();
    }

    /** {@inheritDoc} */
    @Override public Map<Integer, byte[]> collectHandshakeResponseData() {
        Map<Integer, byte[]> componentsData = new HashMap<>();

        for (GridComponent c : ctx.components()) {
            GridComponent.DiscoveryDataExchangeType dataExchangeType = c.discoveryDataType();

            if (dataExchangeType == null)
                continue;

            Serializable componentData = c.collectTcpHandshakeResponseData();

            if (componentData != null) {
                try {
                    byte[] componentDataBytes = marshaller.marshal(componentData);

                    componentsData.put(dataExchangeType.ordinal(), componentDataBytes);
                }
                catch (IgniteCheckedException e) {
                    log.error("Cannot marshal handshake response data", e);
                }
            }
        }

        return componentsData;
    }

    /** {@inheritDoc} */
    @Override public void handshakeResponseDataReceived(Map<Integer, byte[]> componentsData) {
        if (componentsData == null)
            componentsData = Collections.emptyMap();

        for (GridComponent c : ctx.components()) {
            GridComponent.DiscoveryDataExchangeType dataExchangeType = c.discoveryDataType();

            if (dataExchangeType == null)
                continue;

            byte[] componentDataBytes = componentsData.get(dataExchangeType.ordinal());

            Serializable componentData = null;

            try {
                componentData = componentDataBytes == null
                    ? null
                    : marshaller.unmarshal(componentDataBytes, U.gridClassLoader());
            }
            catch (IgniteCheckedException e) {
                log.warning("Cannot unmarshal handshake response data", e);
            }

            c.onTcpHandshakeResponseDataReceived(componentData);
        }
    }
}

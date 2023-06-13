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

package org.apache.ignite.internal.management.api;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientCacheMode;
import org.apache.ignite.internal.client.GridClientException;
import org.apache.ignite.internal.client.GridClientNode;
import org.apache.ignite.internal.client.GridClientNodeMetrics;
import org.apache.ignite.internal.client.GridClientProtocol;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/** */
public class NodeCommandInvoker<A extends IgniteDataTransferObject> extends AbstractCommandInvoker<A> {
    /** */
    private final IgniteEx ignite;

    /**
     * @param cmd Command to execute.
     * @param arg Argument.
     * @param ignite Ignite node.
     */
    public NodeCommandInvoker(Command<A, ?> cmd, A arg, IgniteEx ignite) {
        super(cmd, arg);
        this.ignite = ignite;
    }

    /** {@inheritDoc} */
    @Override protected GridClient client() throws GridClientException {
        return null;
    }

    /** {@inheritDoc} */
    @Override protected Map<UUID, GridClientNode> nodes() throws GridClientException {
        return ignite.cluster().nodes().stream().map(n -> new GridClientNode() {
            @Override public UUID nodeId() {
                return n.id();
            }

            @Override public Object consistentId() {
                return n.consistentId();
            }

            @Override public boolean connectable() {
                return true;
            }

            @Override public long order() {
                return n.order();
            }

            @Override public boolean isClient() {
                return n.isClient();
            }

            @Override public List<String> tcpAddresses() {
                return U.arrayList(n.addresses());
            }

            @Override public List<String> tcpHostNames() {
                return U.arrayList(n.hostNames());
            }

            @Override public int tcpPort() {
                return -1;
            }

            @Override public Map<String, Object> attributes() {
                return n.attributes();
            }

            @Override public <T> @Nullable T attribute(String name) {
                return n.attribute(name);
            }

            @Override public GridClientNodeMetrics metrics() {
                throw new UnsupportedOperationException();
            }

            @Override public Map<String, GridClientCacheMode> caches() {
                throw new UnsupportedOperationException();
            }

            @Override public Collection<InetSocketAddress> availableAddresses(GridClientProtocol proto, boolean filterResolved) {
                throw new UnsupportedOperationException();
            }
        }).collect(Collectors.toMap(n -> n.nodeId(), Function.identity()));
    }

    /** {@inheritDoc} */
    @Override protected GridClientNode defaultNode() throws GridClientException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void close() throws Exception {
        // No-op.
    }
}

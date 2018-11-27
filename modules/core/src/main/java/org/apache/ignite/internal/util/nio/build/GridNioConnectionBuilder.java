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

package org.apache.ignite.internal.util.nio.build;

import java.net.InetSocketAddress;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.util.nio.GridNioRecoveryDescriptor;
import org.apache.ignite.spi.IgniteSpiOperationTimeoutHelper;
import org.apache.ignite.spi.communication.tcp.internal.ConnectionKey;
import org.apache.ignite.spi.communication.tcp.messages.HandshakeMessage;

/**
 * @param <T>
 */
public interface GridNioConnectionBuilder<T> {
    /** */
    public T build(
        GridNioConnectionBuilderContext ctx,
        InetSocketAddress addr,
        GridNioHandshakeCompletionHandler hndlr
    ) throws Exception;

    /** */
    public T build(GridNioConnectionBuilderContext ctx, InetSocketAddress addr) throws Exception;

    /** */
    public GridNioConnectionBuilder<T> setDirectBuf(boolean directBuf);

    /** */
    public GridNioConnectionBuilder<T> setTcpNoDelay(boolean tcpNoDelay);

    /** */
    public GridNioConnectionBuilder<T> setSockSndBufSize(int sockSndBufSize);

    /** */
    public GridNioConnectionBuilder<T> setSockRcvBufSize(int sockRcvBufSize);

    /** */
    public GridNioConnectionBuilder<T> setConnTimeout(long connTimeout);

    /** */
    public GridNioConnectionBuilder<T> setRemoteNode(ClusterNode remoteNode);

    /** */
    public GridNioConnectionBuilder<T> setTimeoutHelper(IgniteSpiOperationTimeoutHelper timeoutHelper);

    /** */
    public GridNioConnectionBuilder<T> setHandshakeMsg(HandshakeMessage handshakeMsg);

    /** */
    public GridNioConnectionBuilder<T> setConnKey(ConnectionKey connKey);

    /** */
    public GridNioConnectionBuilder<T> setRecoveryDesc(GridNioRecoveryDescriptor recoveryDesc);
}

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

package org.apache.ignite.internal.visor.node;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.configuration.SqlConnectorConfiguration;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;
import org.jetbrains.annotations.Nullable;

/**
 * Data transfer object for SQL connector configuration.
 *
 * Deprecated as of Apache Ignite 2.3
 */
@Deprecated
public class VisorSqlConnectorConfiguration extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Host. */
    private String host;

    /** Port. */
    private int port;

    /** Port range. */
    private int portRange;

    /** Max number of opened cursors per connection. */
    private int maxOpenCursorsPerConn;

    /** Socket send buffer size. */
    private int sockSndBufSize;

    /** Socket receive buffer size. */
    private int sockRcvBufSize;

    /** TCP no delay. */
    private boolean tcpNoDelay;

    /** Thread pool size. */
    private int threadPoolSize;

    /**
     * Default constructor.
     */
    public VisorSqlConnectorConfiguration() {
        // No-op.
    }

    /**
     * Create data transfer object for Sql connector configuration.
     *
     * @param cfg Sql connector configuration.
     */
    public VisorSqlConnectorConfiguration(SqlConnectorConfiguration cfg) {
        host = cfg.getHost();
        port = cfg.getPort();
        portRange = cfg.getPortRange();
        maxOpenCursorsPerConn = cfg.getMaxOpenCursorsPerConnection();
        sockSndBufSize = cfg.getSocketSendBufferSize();
        sockRcvBufSize = cfg.getSocketReceiveBufferSize();
        tcpNoDelay = cfg.isTcpNoDelay();
        threadPoolSize = cfg.getThreadPoolSize();
    }

    /**
     * @return Host.
     */
    @Nullable public String getHost() {
        return host;
    }

    /**
     * @return Port.
     */
    public int getPort() {
        return port;
    }

    /**
     * @return Port range.
     */
    public int getPortRange() {
        return portRange;
    }

    /**
     * @return Maximum number of opened cursors.
     */
    public int getMaxOpenCursorsPerConnection() {
        return maxOpenCursorsPerConn;
    }

    /**
     * @return Socket send buffer size in bytes.
     */
    public int getSocketSendBufferSize() {
        return sockSndBufSize;
    }

    /**
     * @return Socket receive buffer size in bytes.
     */
    public int getSocketReceiveBufferSize() {
        return sockRcvBufSize;
    }

    /**
     * @return TCP NO_DELAY flag.
     */
    public boolean isTcpNoDelay() {
        return tcpNoDelay;
    }

    /**
     * @return Thread pool that is in charge of processing SQL requests.
     */
    public int getThreadPoolSize() {
        return threadPoolSize;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeString(out, host);
        out.writeInt(port);
        out.writeInt(portRange);
        out.writeInt(maxOpenCursorsPerConn);
        out.writeInt(sockSndBufSize);
        out.writeInt(sockRcvBufSize );
        out.writeBoolean(tcpNoDelay);
        out.writeInt(threadPoolSize);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        host = U.readString(in);
        port = in.readInt();
        portRange = in.readInt();
        maxOpenCursorsPerConn = in.readInt();
        sockSndBufSize = in.readInt();
        sockRcvBufSize = in.readInt();
        tcpNoDelay = in.readBoolean();
        threadPoolSize = in.readInt();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorSqlConnectorConfiguration.class, this);
    }
}

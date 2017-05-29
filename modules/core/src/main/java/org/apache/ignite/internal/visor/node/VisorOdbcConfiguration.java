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
import org.apache.ignite.configuration.OdbcConfiguration;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;

/**
 * Data transfer object for configuration of ODBC data structures.
 */
public class VisorOdbcConfiguration extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Endpoint address. */
    private String endpointAddr;

    /** Socket send buffer size. */
    private int sockSndBufSize;

    /** Socket receive buffer size. */
    private int sockRcvBufSize;

    /** Max number of opened cursors per connection. */
    private int maxOpenCursors;

    /**
     * Default constructor.
     */
    public VisorOdbcConfiguration() {
        // No-op.
    }

    /**
     * Create data transfer object for ODBC configuration.
     *
     * @param src ODBC configuration.
     */
    public VisorOdbcConfiguration(OdbcConfiguration src) {
        endpointAddr = src.getEndpointAddress();
        sockSndBufSize = src.getSocketSendBufferSize();
        sockRcvBufSize = src.getSocketReceiveBufferSize();
        maxOpenCursors = src.getMaxOpenCursors();
    }

    /**
     * @return ODBC endpoint address.
     */
    public String getEndpointAddress() {
        return endpointAddr;
    }

    /**
     * @return Maximum number of opened cursors.
     */
    public int getMaxOpenCursors() {
        return maxOpenCursors;
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

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeString(out, endpointAddr);
        out.writeInt(sockSndBufSize);
        out.writeInt(sockRcvBufSize);
        out.writeInt(maxOpenCursors);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        endpointAddr = U.readString(in);
        sockSndBufSize = in.readInt();
        sockRcvBufSize = in.readInt();
        maxOpenCursors = in.readInt();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorOdbcConfiguration.class, this);
    }
}

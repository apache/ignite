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

package org.apache.ignite.internal.processors.odbc.jdbc;

import java.util.Map;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.internal.binary.BinaryReaderEx;
import org.apache.ignite.internal.binary.BinaryWriterEx;
import org.apache.ignite.internal.jdbc.thin.JdbcThinConnection;
import org.jetbrains.annotations.Nullable;

/** Request with client info set with {@link java.sql.Connection#setClientInfo}. */
public class JdbcClientInfoAwareRequest extends JdbcRequest {
    /** Client info set with {@link JdbcThinConnection#setClientInfo} methods. */
    private @Nullable Map<String, String> clientInfo;

    /**
     * @param type Command type.
     */
    public JdbcClientInfoAwareRequest(byte type) {
        super(type);
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(
        BinaryWriterEx writer,
        JdbcProtocolContext protoCtx
    ) throws BinaryObjectException {
        super.writeBinary(writer, protoCtx);

        if (protoCtx.isFeatureSupported(JdbcThinFeature.CLIENT_INFO))
            writer.writeMap(clientInfo);
    }

    /** {@inheritDoc} */
    @Override public void readBinary(
        BinaryReaderEx reader,
        JdbcProtocolContext protoCtx
    ) throws BinaryObjectException {
        super.readBinary(reader, protoCtx);

        if (protoCtx.isFeatureSupported(JdbcThinFeature.CLIENT_INFO))
            clientInfo = reader.readMap();
    }

    /** @return Client info. */
    public @Nullable Map<String, String> clientInfo() {
        return clientInfo;
    }

    /** @param clientInfo Client info. */
    public void clientInfo(@Nullable Map<String, String> clientInfo) {
        this.clientInfo = clientInfo;
    }
}

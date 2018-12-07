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

import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.internal.processors.odbc.ClientListenerProtocolVersion;
import org.apache.ignite.internal.processors.odbc.ClientListenerResponse;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * SQL listener response.
 */
public class JdbcResponse extends ClientListenerResponse implements JdbcRawBinarylizable {
    /** Response object. */
    @GridToStringInclude
    private JdbcResult res;

    /**
     * Default constructs is used for deserialization
     */
    public JdbcResponse() {
        super(-1, null);
    }

    /**
     * Constructs successful rest response.
     *
     * @param res Response result.
     */
    public JdbcResponse(JdbcResult res) {
        super(STATUS_SUCCESS, null);

        this.res = res;
    }

    /**
     * Constructs failed rest response.
     *
     * @param status Response status.
     * @param err Error, {@code null} if success is {@code true}.
     */
    public JdbcResponse(int status, @Nullable String err) {
        super(status, err);

        assert status != STATUS_SUCCESS;
    }

    /**
     * @return Response object.
     */
    public JdbcResult response() {
        return res;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(JdbcResponse.class, this, "status", status(),"err", error());
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(BinaryWriterExImpl writer,
        ClientListenerProtocolVersion ver) throws BinaryObjectException {
        writer.writeInt(status());

        if (status() == STATUS_SUCCESS) {
            writer.writeBoolean(res != null);

            if (res != null)
                res.writeBinary(writer, ver);
        }
        else
            writer.writeString(error());

    }

    /** {@inheritDoc} */
    @Override public void readBinary(BinaryReaderExImpl reader,
        ClientListenerProtocolVersion ver) throws BinaryObjectException {
        status(reader.readInt());

        if (status() == STATUS_SUCCESS) {
            if (reader.readBoolean())
                res = JdbcResult.readResult(reader, ver);
        }
        else
            error(reader.readString());
    }
}

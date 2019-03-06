/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.odbc.jdbc;

import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.internal.processors.bulkload.BulkLoadAckClientParameters;
import org.apache.ignite.internal.processors.odbc.ClientListenerProtocolVersion;
import org.apache.ignite.internal.sql.command.SqlBulkLoadCommand;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * A reply from server to SQL COPY command, which is essentially a request from server to client
 * to send files from client to server (see IGNITE-6917 for details).
 *
 * @see JdbcBulkLoadProcessor for the protocol.
 * @see SqlBulkLoadCommand
 */
public class JdbcBulkLoadAckResult extends JdbcResult {
    /** Cursor ID for matching this command on server in further {@link JdbcBulkLoadBatchRequest} commands. */
    private long cursorId;

    /**
     * Bulk load parameters, which are parsed on the server side and sent to client to specify
     * what files to send, batch size, etc.
     */
    private BulkLoadAckClientParameters params;

    /**Creates uninitialized bulk load batch request result. */
    public JdbcBulkLoadAckResult() {
        super(BULK_LOAD_ACK);

        cursorId = 0;
        params = null;
    }

    /**
     * Constructs a request from server (in form of reply) to send files from client to server.
     *
     * @param cursorId Cursor ID to send in further {@link JdbcBulkLoadBatchRequest}s.
     * @param params Various parameters for sending batches from client side.
     */
    public JdbcBulkLoadAckResult(long cursorId, BulkLoadAckClientParameters params) {
        super(BULK_LOAD_ACK);

        this.cursorId = cursorId;
        this.params = params;
    }

    /**
     * Returns the cursor ID.
     *
     * @return Cursor ID.
     */
    public long cursorId() {
        return cursorId;
    }

    /**
     * Returns the parameters for the client.
     *
     * @return The parameters for the client.
     */
    public BulkLoadAckClientParameters params() {
        return params;
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(BinaryWriterExImpl writer,
        ClientListenerProtocolVersion ver) throws BinaryObjectException {
        super.writeBinary(writer, ver);

        writer.writeLong(cursorId);
        writer.writeString(params.localFileName());
        writer.writeInt(params.packetSize());
    }

    /** {@inheritDoc} */
    @Override public void readBinary(BinaryReaderExImpl reader,
        ClientListenerProtocolVersion ver) throws BinaryObjectException {
        super.readBinary(reader, ver);

        cursorId = reader.readLong();

        String locFileName = reader.readString();
        int batchSize = reader.readInt();

        if (!BulkLoadAckClientParameters.isValidPacketSize(batchSize))
            throw new BinaryObjectException(BulkLoadAckClientParameters.packetSizeErrorMesssage(batchSize));

        params = new BulkLoadAckClientParameters(locFileName, batchSize);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(JdbcBulkLoadAckResult.class, this);
    }
}

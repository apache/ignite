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
import org.apache.ignite.internal.processors.odbc.ClientListenerProtocolVersion;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * JDBC get primary keys metadata request.
 */
public class JdbcMetaPrimaryKeysRequest extends JdbcRequest {
    /** Schema name pattern. */
    private String schemaName;

    /** Table name pattern. */
    private String tblName;

    /**
     * Default constructor is used for deserialization.
     */
    JdbcMetaPrimaryKeysRequest() {
        super(META_PRIMARY_KEYS);
    }

    /**
     * @param schemaName Cache name.
     * @param tblName Table name.
     */
    public JdbcMetaPrimaryKeysRequest(String schemaName, String tblName) {
        super(META_PRIMARY_KEYS);

        this.schemaName = schemaName;
        this.tblName = tblName;
    }

    /**
     * @return Schema name pattern.
     */
    @Nullable public String schemaName() {
        return schemaName;
    }

    /**
     * @return Table name pattern.
     */
    public String tableName() {
        return tblName;
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(BinaryWriterExImpl writer,
        ClientListenerProtocolVersion ver) throws BinaryObjectException {
        super.writeBinary(writer, ver);

        writer.writeString(schemaName);
        writer.writeString(tblName);
    }

    /** {@inheritDoc} */
    @Override public void readBinary(BinaryReaderExImpl reader,
        ClientListenerProtocolVersion ver) throws BinaryObjectException {
        super.readBinary(reader, ver);

        schemaName = reader.readString();
        tblName = reader.readString();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(JdbcMetaPrimaryKeysRequest.class, this);
    }
}
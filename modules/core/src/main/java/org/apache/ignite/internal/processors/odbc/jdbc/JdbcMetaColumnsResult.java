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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.internal.processors.odbc.ClientListenerProtocolVersion;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * JDBC columns metadata result.
 */
public class JdbcMetaColumnsResult extends JdbcResult {
    /** Columns metadata. */
    private List<JdbcColumnMeta> meta;

    /**
     * Default constructor is used for deserialization.
     */
    JdbcMetaColumnsResult() {
        super(META_COLUMNS);
    }

    /**
     * @param meta Columns metadata.
     */
    JdbcMetaColumnsResult(Collection<JdbcColumnMeta> meta) {
        super(META_COLUMNS);

        this.meta = new ArrayList<>(meta);
    }

    /**
     * Used by children classes.
     * @param type Type ID.
     */
    protected JdbcMetaColumnsResult(byte type) {
        super(type);
    }

    /**
     * Used by children classes.
     * @param type Type ID.
     * @param meta Columns metadata.
     */
    protected JdbcMetaColumnsResult(byte type, Collection<JdbcColumnMeta> meta) {
        super(type);

        this.meta = new ArrayList<>(meta);
    }

    /**
     * @return Columns metadata.
     */
    public List<JdbcColumnMeta> meta() {
        return meta;
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(BinaryWriterExImpl writer,
        ClientListenerProtocolVersion ver) throws BinaryObjectException {
        super.writeBinary(writer, ver);

        if (F.isEmpty(meta))
            writer.writeInt(0);
        else {
            writer.writeInt(meta.size());

            for(JdbcColumnMeta m : meta)
                m.writeBinary(writer, ver);
        }
    }

    /** {@inheritDoc} */
    @Override public void readBinary(BinaryReaderExImpl reader,
        ClientListenerProtocolVersion ver) throws BinaryObjectException {
        super.readBinary(reader, ver);

        int size = reader.readInt();

        if (size == 0)
            meta = Collections.emptyList();
        else {
            meta = new ArrayList<>(size);

            for (int i = 0; i < size; ++i) {
                JdbcColumnMeta m = createMetaColumn();

                m.readBinary(reader, ver);

                meta.add(m);
            }
        }
    }

    /**
     * @return Empty columns metadata to deserialization.
     */
    protected JdbcColumnMeta createMetaColumn() {
        return new JdbcColumnMeta();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(JdbcMetaColumnsResult.class, this);
    }
}

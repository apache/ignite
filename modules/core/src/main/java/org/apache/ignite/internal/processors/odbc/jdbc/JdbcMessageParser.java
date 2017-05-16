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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.BinaryUtils;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.internal.binary.GridBinaryMarshaller;
import org.apache.ignite.internal.binary.streams.BinaryHeapOutputStream;
import org.apache.ignite.internal.binary.streams.BinaryInputStream;
import org.apache.ignite.internal.processors.odbc.AbstractSqlBinaryReader;
import org.apache.ignite.internal.processors.odbc.AbstractSqlBinaryWriter;
import org.apache.ignite.internal.processors.odbc.SqlListenerMessageParser;
import org.apache.ignite.internal.processors.odbc.SqlListenerMessageParserImpl;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;

/**
 * JDBC message parser.
 */
public class JdbcMessageParser extends SqlListenerMessageParserImpl {
    /** Jdk marshaller. */
    private JdkMarshaller jdkMars = new JdkMarshaller();

    /**
     * @param ctx Context.
     */
    public JdbcMessageParser(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override protected BinaryWriterExImpl createBinaryWriter(int cap) {
        return new BinaryWriterExImpl(null, new BinaryHeapOutputStream(cap), null, null);
    }

    /** {@inheritDoc} */
    @Override protected BinaryReaderExImpl createBinaryReader(BinaryInputStream in) {
        return new BinaryReaderExImpl(null, in, null, false);
    }

    /** {@inheritDoc} */
    @Override protected void writeNotEmbeddedObject(BinaryWriterExImpl writer, Object obj) throws BinaryObjectException {
        writer.writeByte(SqlListenerMessageParser.JDK_MARSH);

        try {
            writer.writeByteArray(U.marshal(jdkMars, obj));
        }
        catch (IgniteCheckedException e) {
            throw new BinaryObjectException(e);
        }
    }

    /** {@inheritDoc} */
    @Override protected Object readJdkMarshalledObject(BinaryReaderExImpl reader) throws BinaryObjectException {
        try {
            byte type = reader.readByte();

            assert type == GridBinaryMarshaller.BYTE_ARR;

            return U.unmarshal(jdkMars, BinaryUtils.doReadByteArray(reader.in()), null);
        }
        catch (IgniteCheckedException e) {
            throw new BinaryObjectException(e);
        }

    }

}

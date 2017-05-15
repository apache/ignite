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
import org.apache.ignite.internal.binary.BinaryUtils;
import org.apache.ignite.internal.binary.GridBinaryMarshaller;
import org.apache.ignite.internal.binary.streams.BinaryInputStream;
import org.apache.ignite.internal.processors.odbc.AbstractSqlBinaryReader;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;

/**
 * Binary reader with marshaling non-primitive and non-embedded objects with JDK marshaller.
 */
@SuppressWarnings("unchecked")
public class JdbcBinaryReader extends AbstractSqlBinaryReader {
    /** Jdk marshaller. */
    private JdkMarshaller jdkMars = new JdkMarshaller();
    /**
     * @param in Binary reader.
     */
    public JdbcBinaryReader(BinaryInputStream in) {
        super(null, in, null, false);
    }

    /** {@inheritDoc} */
    @Override protected Object readNotEmbeddedObject() throws BinaryObjectException {
        try {
            byte type = readByte();

            assert type == GridBinaryMarshaller.BYTE_ARR;

            return U.unmarshal(jdkMars, BinaryUtils.doReadByteArray(in()), null);
        }
        catch (IgniteCheckedException e) {
            throw new BinaryObjectException(e);
        }
    }
}

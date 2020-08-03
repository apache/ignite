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
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * JDBC indexes metadata result.
 */
public class JdbcMetaIndexesResult extends JdbcResult {
    /** Indexes metadata. */
    private List<JdbcIndexMeta> meta;

    /**
     * Default constructor is used for deserialization.
     */
    JdbcMetaIndexesResult() {
        super(META_INDEXES);
    }

    /**
     * @param meta Indexes metadata.
     */
    JdbcMetaIndexesResult(Collection<JdbcIndexMeta> meta) {
        super(META_INDEXES);
        this.meta = new ArrayList<>(meta);
    }

    /**
     * @return Indexes metadata.
     */
    public List<JdbcIndexMeta> meta() {
        return meta;
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(
        BinaryWriterExImpl writer,
        JdbcProtocolContext protoCtx
    ) throws BinaryObjectException {
        super.writeBinary(writer, protoCtx);

        if (F.isEmpty(meta))
            writer.writeInt(0);
        else {
            writer.writeInt(meta.size());

            for (JdbcIndexMeta m : meta)
                m.writeBinary(writer, protoCtx);
        }
    }

    /** {@inheritDoc} */
    @Override public void readBinary(
        BinaryReaderExImpl reader,
        JdbcProtocolContext protoCtx
    ) throws BinaryObjectException {
        super.readBinary(reader, protoCtx);

        int size = reader.readInt();

        if (size == 0)
            meta = Collections.emptyList();
        else {
            meta = new ArrayList<>(size);

            for (int i = 0; i < size; ++i) {
                JdbcIndexMeta m = new JdbcIndexMeta();

                m.readBinary(reader, protoCtx);

                meta.add(m);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(JdbcMetaIndexesResult.class, this);
    }
}

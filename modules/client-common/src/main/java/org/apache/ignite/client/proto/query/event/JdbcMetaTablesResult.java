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

package org.apache.ignite.client.proto.query.event;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.ignite.internal.client.proto.ClientMessagePacker;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.tostring.S;

/**
 * JDBC tables metadata result.
 */
public class JdbcMetaTablesResult extends Response {
    /** Tables metadata. */
    private List<JdbcTableMeta> meta;

    /**
     * Default constructor is used for deserialization.
     */
    public JdbcMetaTablesResult() {
    }

    /**
     * Constructor.
     *
     * @param meta Tables metadata.
     */
    public JdbcMetaTablesResult(List<JdbcTableMeta> meta) {
        Objects.requireNonNull(meta);

        this.meta = meta;

        this.hasResults = true;
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(ClientMessagePacker packer) {
        super.writeBinary(packer);

        if (!hasResults)
            return;

        packer.packArrayHeader(meta.size());

        for (JdbcTableMeta m : meta)
            m.writeBinary(packer);
    }

    /** {@inheritDoc} */
    @Override public void readBinary(ClientMessageUnpacker unpacker) {
        super.readBinary(unpacker);

        if (!hasResults)
            return;

        int size = unpacker.unpackArrayHeader();

        meta = new ArrayList<>(size);

        for (int i = 0; i < size; ++i) {
            JdbcTableMeta m = new JdbcTableMeta();

            m.readBinary(unpacker);

            meta.add(m);
        }
    }

    /**
     * Gets table metadata.
     *
     * @return Tables metadata.
     */
    public List<JdbcTableMeta> meta() {
        return meta;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(JdbcMetaTablesResult.class, this);
    }
}

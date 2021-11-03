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
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.ignite.internal.client.proto.ClientMessagePacker;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.tostring.S;

/**
 * JDBC primary keys metadata result.
 */
public class JdbcMetaPrimaryKeysResult extends Response {
    /** Primary keys meta. */
    private List<JdbcPrimaryKeyMeta> meta;

    /**
     * Default constructor is used for deserialization.
     */
    public JdbcMetaPrimaryKeysResult() {
    }

    /**
     * Constructor.
     *
     * @param meta Column metadata.
     */
    public JdbcMetaPrimaryKeysResult(Collection<JdbcPrimaryKeyMeta> meta) {
        Objects.requireNonNull(meta);

        this.meta = new ArrayList<>(meta);

        this.hasResults = true;
    }

    /** {@inheritDoc} */
    @Override
    public void writeBinary(ClientMessagePacker packer) {
        super.writeBinary(packer);

        if (!hasResults) {
            return;
        }

        if (meta == null || meta.isEmpty()) {
            packer.packNil();

            return;
        }

        packer.packArrayHeader(meta.size());

        for (JdbcPrimaryKeyMeta keyMeta : meta) {
            keyMeta.writeBinary(packer);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void readBinary(ClientMessageUnpacker unpacker) {
        super.readBinary(unpacker);

        if (!hasResults) {
            return;
        }

        if (unpacker.tryUnpackNil()) {
            meta = Collections.emptyList();

            return;
        }

        int size = unpacker.unpackArrayHeader();

        meta = new ArrayList<>(size);

        for (int i = 0; i < size; ++i) {
            JdbcPrimaryKeyMeta m = new JdbcPrimaryKeyMeta();

            m.readBinary(unpacker);

            meta.add(m);
        }
    }

    /**
     * Gets primary key metadata.
     *
     * @return Primary keys metadata.
     */
    public List<JdbcPrimaryKeyMeta> meta() {
        return meta;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(JdbcMetaPrimaryKeysResult.class, this);
    }
}

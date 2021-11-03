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
 * JDBC columns metadata result.
 */
public class JdbcMetaColumnsResult extends Response {
    /** Columns metadata. */
    private List<JdbcColumnMeta> meta;

    /**
     * Default constructor is used for deserialization.
     */
    public JdbcMetaColumnsResult() {
    }

    /**
     * Constructor.
     *
     * @param status Status code.
     * @param err    Error message.
     */
    public JdbcMetaColumnsResult(int status, String err) {
        super(status, err);
    }

    /**
     * Constructor.
     *
     * @param meta Columns metadata.
     */
    public JdbcMetaColumnsResult(Collection<JdbcColumnMeta> meta) {
        Objects.requireNonNull(meta);

        this.meta = new ArrayList<>(meta);

        this.hasResults = true;
    }

    /**
     * Gets column metadata.
     *
     * @return Columns metadata.
     */
    public List<JdbcColumnMeta> meta() {
        return meta;
    }

    /** {@inheritDoc} */
    @Override
    public void writeBinary(ClientMessagePacker packer) {
        super.writeBinary(packer);

        if (!hasResults) {
            return;
        }

        packer.packArrayHeader(meta.size());

        for (JdbcColumnMeta m : meta) {
            m.writeBinary(packer);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void readBinary(ClientMessageUnpacker unpacker) {
        super.readBinary(unpacker);

        if (!hasResults) {
            return;
        }

        int size = unpacker.unpackArrayHeader();

        if (size == 0) {
            meta = Collections.emptyList();

            return;
        }

        meta = new ArrayList<>(size);

        for (int i = 0; i < size; ++i) {
            var m = new JdbcColumnMeta();

            m.readBinary(unpacker);

            meta.add(m);
        }
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(JdbcMetaColumnsResult.class, this);
    }
}

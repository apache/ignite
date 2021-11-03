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
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.ignite.client.proto.query.ClientMessage;
import org.apache.ignite.internal.client.proto.ClientMessagePacker;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.tostring.S;

/**
 * JDBC primary key metadata.
 */
public class JdbcPrimaryKeyMeta implements ClientMessage {
    /** Schema name. */
    private String schemaName;

    /** Table name. */
    private String tblName;

    /** Primary key name. */
    private String name;

    /** Primary key fields. */
    private List<String> fields;

    /**
     * Default constructor is used for binary serialization.
     */
    public JdbcPrimaryKeyMeta() {
    }

    /**
     * Constructor.
     *
     * @param schemaName Schema.
     * @param tblName    Table.
     * @param name       Primary key name.
     * @param fields     Primary key fields.
     */
    public JdbcPrimaryKeyMeta(String schemaName, String tblName, String name, List<String> fields) {
        this.schemaName = schemaName;
        this.tblName = tblName;
        this.name = name;
        this.fields = fields;
    }

    /**
     * Gets schema name.
     *
     * @return Schema name.
     */
    public String schemaName() {
        return schemaName;
    }

    /**
     * Gets table name.
     *
     * @return Table name.
     */
    public String tableName() {
        return tblName;
    }

    /**
     * Gets primary key name.
     *
     * @return Primary key name.
     */
    public String name() {
        return name;
    }

    /**
     * Gets key field names.
     *
     * @return Key fields.
     */
    public List<String> fields() {
        return fields;
    }


    /** {@inheritDoc} */
    @Override
    public void writeBinary(ClientMessagePacker packer) {
        packer.packString(schemaName);
        packer.packString(tblName);
        packer.packString(name);

        if (fields == null || fields.isEmpty()) {
            packer.packNil();
        }

        packer.packArrayHeader(fields.size());

        for (String field : fields) {
            packer.packString(field);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void readBinary(ClientMessageUnpacker unpacker) {
        schemaName = unpacker.unpackString();
        tblName = unpacker.unpackString();
        name = unpacker.unpackString();

        if (unpacker.tryUnpackNil()) {
            fields = Collections.emptyList();

            return;
        }

        int size = unpacker.unpackArrayHeader();

        fields = new ArrayList<>(size);

        for (int i = 0; i < size; i++) {
            fields.add(unpacker.unpackString());
        }
    }

    /** {@inheritDoc} */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        JdbcPrimaryKeyMeta meta = (JdbcPrimaryKeyMeta) o;

        return Objects.equals(schemaName, meta.schemaName)
                && Objects.equals(tblName, meta.tblName)
                && Objects.equals(name, meta.name)
                && Objects.equals(fields, meta.fields);
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
        int result = schemaName.hashCode();
        result = 31 * result + tblName.hashCode();
        result = 31 * result + name.hashCode();
        result = 31 * result + (fields != null ? fields.hashCode() : 0);
        return result;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(JdbcPrimaryKeyMeta.class, this);
    }
}

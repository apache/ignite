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
 * JDBC primary keys metadata result.
 */
public class JdbcMetaPrimaryKeysResult extends JdbcResult {
    /** Query result rows. */
    private List<JdbcPrimaryKeyMeta> meta;

    /**
     * Default constructor is used for deserialization.
     */
    JdbcMetaPrimaryKeysResult() {
        super(META_PRIMARY_KEYS);
    }

    /**
     * @param meta Column metadata.
     */
    JdbcMetaPrimaryKeysResult(Collection<JdbcPrimaryKeyMeta> meta) {
        super(META_PRIMARY_KEYS);

        this.meta = new ArrayList<>(meta);
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(BinaryWriterExImpl writer) throws BinaryObjectException {
        super.writeBinary(writer);

        if (F.isEmpty(meta))
            writer.writeInt(0);
        else {
            writer.writeInt(meta.size());

            for(JdbcPrimaryKeyMeta m : meta)
                m.writeBinary(writer);
        }
    }

    /** {@inheritDoc} */
    @Override public void readBinary(BinaryReaderExImpl reader) throws BinaryObjectException {
        super.readBinary(reader);

        int size = reader.readInt();

        if (size == 0)
            meta = Collections.emptyList();
        else {
            meta = new ArrayList<>(size);

            for (int i = 0; i < size; ++i) {
                JdbcPrimaryKeyMeta m = new JdbcPrimaryKeyMeta();

                m.readBinary(reader);

                meta.add(m);
            }
        }
    }

    /**
     * @return Primary keys metadata.
     */
    public List<JdbcPrimaryKeyMeta> meta() {
        return meta;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(JdbcMetaPrimaryKeysResult.class, this);
    }
}

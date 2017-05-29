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

package org.apache.ignite.internal.processors.odbc;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;

/**
 * Query get columns meta result.
 */
public class OdbcQueryGetTablesMetaResult implements RawBinarylizable {
    /** Query result rows. */
    private List<OdbcTableMeta> meta;

    /**
     * @param meta Column metadata.
     */
    public OdbcQueryGetTablesMetaResult(List<OdbcTableMeta> meta) {
        this.meta = meta;
    }

    /**
     * @return Query result rows.
     */
    public List<OdbcTableMeta> meta() {
        return meta;
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(BinaryWriterExImpl writer,
        SqlListenerAbstractObjectWriter objWriter) throws BinaryObjectException {
        if (meta == null) {
            writer.writeInt(0);

            return;
        }

        writer.writeInt(meta.size());

        for (OdbcTableMeta tblMeta : meta)
            tblMeta.writeBinary(writer, objWriter);
    }

    /** {@inheritDoc} */
    @Override public void readBinary(BinaryReaderExImpl reader,
        SqlListenerAbstractObjectReader objReader) throws BinaryObjectException {
        int metaSize = reader.readInt();

        if (metaSize > 0) {
            meta = new ArrayList<>(metaSize);

            for (int i = 0; i < metaSize; ++i) {
                OdbcTableMeta m = new OdbcTableMeta();

                m.readBinary(reader, objReader);

                meta.add(m);
            }
        }
        else
            meta = Collections.emptyList();
    }
}

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

import java.util.List;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;

/**
 * SQL listener query metadata result.
 */
public class SqlListenerQueryMetadataResult implements RawBinarylizable {
    /** Query ID. */
    private long queryId;

    /** Fields metadata. */
    private List<SqlListenerColumnMeta> meta;

    /**
     * Default constructor is used for deserialization.
     */
    public SqlListenerQueryMetadataResult() {
    }

    /**
     * @param queryId Query ID.
     * @param meta Query metadata.
     */
    public SqlListenerQueryMetadataResult(long queryId, List<SqlListenerColumnMeta> meta){
        this.queryId = queryId;
        this.meta = meta;
    }

    /**
     * @return Query ID.
     */
    public long queryId() {
        return queryId;
    }

    /**
     * @return Query result rows.
     */
    public List<SqlListenerColumnMeta> meta() {
        return meta;
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(BinaryWriterExImpl writer,
        SqlListenerAbstractObjectWriter objWriter) throws BinaryObjectException {

        writer.writeLong(queryId);

        SqlListenerAbstractMessageParser.writeColumnsMeta(writer, objWriter, meta);
    }

    /** {@inheritDoc} */
    @Override public void readBinary(BinaryReaderExImpl reader,
        SqlListenerAbstractObjectReader objReader) throws BinaryObjectException {

        queryId = reader.readLong();

        meta = SqlListenerAbstractMessageParser.readColumnsMeta(reader, objReader);
    }
}

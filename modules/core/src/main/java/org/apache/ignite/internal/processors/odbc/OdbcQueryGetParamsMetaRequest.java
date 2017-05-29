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

import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * ODBC query get params meta request.
 */
public class OdbcQueryGetParamsMetaRequest extends SqlListenerRequest {
    /** Cache. */
    private String cacheName;

    /** Query. */
    private String query;

    /**
     *
     */
    public OdbcQueryGetParamsMetaRequest() {
        super(META_PARAMS);
    }

    /**
     * @param cacheName Cache name.
     * @param query SQL Query.
     */
    public OdbcQueryGetParamsMetaRequest(String cacheName, String query) {
        super(META_PARAMS);

        this.cacheName = cacheName;
        this.query = query;
    }

    /**
     * @return SQL Query.
     */
    public String query() {
        return query;
    }

    /**
     * @return Cache name.
     */
    public String cacheName() {
        return cacheName;
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(BinaryWriterExImpl writer,
        SqlListenerAbstractObjectWriter objWriter) throws BinaryObjectException {
        super.writeBinary(writer, objWriter);

        writer.writeString(cacheName);
        writer.writeString(query);
    }

    /** {@inheritDoc} */
    @Override public void readBinary(BinaryReaderExImpl reader,
        SqlListenerAbstractObjectReader objReader) throws BinaryObjectException {
        super.readBinary(reader, objReader);

        cacheName = reader.readString();
        query = reader.readString();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(OdbcQueryGetParamsMetaRequest.class, this);
    }
}

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

import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.internal.processors.odbc.ClientListenerProtocolVersion;

public class JdbcCachePartitionsRequest extends JdbcRequest {

    private Set<String> cacheNames;

    public JdbcCachePartitionsRequest() {
        super(CACHE_PARTITIONS);
    }

    public JdbcCachePartitionsRequest(Set<String> cacheNames) {
        super(CACHE_PARTITIONS);

        this.cacheNames = cacheNames;
    }

    /**
     * @return Cache id.
     */
    public Set<String> cacheNames() {
        return cacheNames;
    }

    @Override
    public void writeBinary(BinaryWriterExImpl writer, ClientListenerProtocolVersion ver) throws BinaryObjectException {
        super.writeBinary(writer, ver);

        assert cacheNames != null;
        assert !cacheNames.isEmpty();

        writer.writeInt(cacheNames.size());
        for (String cacheName: cacheNames)
            writer.writeString(cacheName);
    }

    @Override
    public void readBinary(BinaryReaderExImpl reader, ClientListenerProtocolVersion ver) throws BinaryObjectException {
        super.readBinary(reader, ver);
        int cacheNamesSize = reader.readInt();

        cacheNames = new HashSet<>();
        for (int i = 0; i < cacheNamesSize; i++)
            cacheNames.add(reader.readString());
    }
}
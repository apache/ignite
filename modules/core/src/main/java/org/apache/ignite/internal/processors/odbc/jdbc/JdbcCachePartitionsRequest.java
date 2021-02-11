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

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Jdbc thin request for partiton distribution.
 */
public class JdbcCachePartitionsRequest extends JdbcRequest {
    /** Cache ids. */
    private Set<Integer> cacheIds;

    /**
     * Default constructor.
     */
    public JdbcCachePartitionsRequest() {
        super(CACHE_PARTITIONS);
    }

    /**
     * Constructor.
     *
     * @param cacheIds Cache ids.
     */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    public JdbcCachePartitionsRequest(Set<Integer> cacheIds) {
        super(CACHE_PARTITIONS);

        this.cacheIds = cacheIds;
    }

    /**
     * @return Cache id.
     */
    public Set<Integer> cacheIds() {
        return Collections.unmodifiableSet(cacheIds);
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(
        BinaryWriterExImpl writer,
        JdbcProtocolContext protoCtx
    ) throws BinaryObjectException {
        super.writeBinary(writer, protoCtx);

        assert cacheIds != null;
        assert !cacheIds.isEmpty();

        writer.writeInt(cacheIds.size());
        for (Integer cacheId : cacheIds)
            writer.writeInt(cacheId);
    }

    /** {@inheritDoc} */
    @Override public void readBinary(
        BinaryReaderExImpl reader,
        JdbcProtocolContext protoCtx
    ) throws BinaryObjectException {
        super.readBinary(reader, protoCtx);
        int cacheIdsSize = reader.readInt();

        cacheIds = new HashSet<>();
        for (int i = 0; i < cacheIdsSize; i++)
            cacheIds.add(reader.readInt());
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(JdbcCachePartitionsRequest.class, this);
    }
}

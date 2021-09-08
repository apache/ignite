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
import java.util.Collections;
import java.util.List;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.internal.jdbc.thin.JdbcThinPartitionAwarenessMappingGroup;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Jdbc thin partiton result that contains partition mappings.
 */
public class JdbcCachePartitionsResult extends JdbcResult {
    /** Partitions Mappings. */
    private List<JdbcThinPartitionAwarenessMappingGroup> mappings;

    /**
     * Default constructor.
     */
    public JdbcCachePartitionsResult() {
        super(CACHE_PARTITIONS);
    }

    /**
     * Constructor.
     *
     * @param mappings Partitions mappings.
     */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    public JdbcCachePartitionsResult(List<JdbcThinPartitionAwarenessMappingGroup> mappings) {
        super(CACHE_PARTITIONS);

        this.mappings = mappings;
    }

    /**
     * @return Partitons mappings.
     */
    public List<JdbcThinPartitionAwarenessMappingGroup> getMappings() {
        return Collections.unmodifiableList(mappings);
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(
        BinaryWriterExImpl writer,
        JdbcProtocolContext protoCtx
    ) throws BinaryObjectException {
        super.writeBinary(writer, protoCtx);

        assert mappings != null;

        writer.writeInt(mappings.size());

        for (JdbcThinPartitionAwarenessMappingGroup mappingGroup : mappings)
            mappingGroup.writeBinary(writer, protoCtx);
    }

    /** {@inheritDoc} */
    @Override public void readBinary(
        BinaryReaderExImpl reader,
        JdbcProtocolContext protoCtx
    ) throws BinaryObjectException {
        super.readBinary(reader, protoCtx);

        List<JdbcThinPartitionAwarenessMappingGroup> res = new ArrayList<>();

        int mappingsSize = reader.readInt();

        for (int i = 0; i < mappingsSize; i++)
            res.add(JdbcThinPartitionAwarenessMappingGroup.readGroup(reader, protoCtx));

        mappings = res;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(JdbcCachePartitionsResult.class, this);
    }
}

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
import java.util.List;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.internal.jdbc.thin.JdbcThinAffinityAwarenessMappingGroup;
import org.apache.ignite.internal.processors.odbc.ClientListenerProtocolVersion;

// TODO VO: Add aff topology version and use it to check whether it is valid to add collected distributions to current AffinityCache.
public class JdbcCachePartitionsResult extends JdbcResult {

    private List<JdbcThinAffinityAwarenessMappingGroup> mappings;

    /**
     * Default constructor.
     */
    public JdbcCachePartitionsResult() {
        super(CACHE_PARTITIONS);
    }

    public JdbcCachePartitionsResult(List<JdbcThinAffinityAwarenessMappingGroup> mappings) {
        super(CACHE_PARTITIONS);

        this.mappings = mappings;
    }

    public List<JdbcThinAffinityAwarenessMappingGroup> getMappings() {
        return mappings;
    }

    @Override
    public void writeBinary(BinaryWriterExImpl writer, ClientListenerProtocolVersion ver) throws BinaryObjectException {
        super.writeBinary(writer, ver);

        assert mappings != null;

        writer.writeInt(mappings.size());

        for (JdbcThinAffinityAwarenessMappingGroup mappingGroup : mappings)
            mappingGroup.writeBinary(writer, ver);
    }

    @Override
    public void readBinary(BinaryReaderExImpl reader, ClientListenerProtocolVersion ver) throws BinaryObjectException {
        super.readBinary(reader, ver);
        List<JdbcThinAffinityAwarenessMappingGroup> res = new ArrayList<>();

        int mappingsSize = reader.readInt();

        for (int i = 0; i < mappingsSize; i++)
            res.add(JdbcThinAffinityAwarenessMappingGroup.readGroup(reader, ver));

        mappings = res;
    }
}

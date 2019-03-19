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

package org.apache.ignite.internal.jdbc.thin;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.internal.processors.odbc.ClientListenerProtocolVersion;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcRawBinarylizable;

public class JdbcThinAffinityAwarenessMappings implements JdbcRawBinarylizable {

    private final List<JdbcThinAffinityAwarenessMappingGroup> mappings = new ArrayList<>();

    public void add(int cacheId, Map<UUID, Set<Integer>> partitionsMappings) {
        for (JdbcThinAffinityAwarenessMappingGroup mappingGroup : mappings) {
            if (mappingGroup.merge(cacheId, partitionsMappings))
                return;
        }

        mappings.add(new JdbcThinAffinityAwarenessMappingGroup(cacheId, partitionsMappings));
    }

    /**
     * @return Mappings.
     */
    public List<JdbcThinAffinityAwarenessMappingGroup> mappings() {
        return mappings;
    }

    @Override
    public void writeBinary(BinaryWriterExImpl writer, ClientListenerProtocolVersion ver) throws BinaryObjectException {
        writer.writeInt(mappings.size());

        for (JdbcThinAffinityAwarenessMappingGroup mappingGroup : mappings)
            mappingGroup.writeBinary(writer, ver);
    }

    @Override
    public void readBinary(BinaryReaderExImpl reader, ClientListenerProtocolVersion ver) throws BinaryObjectException {
        // No-op.
    }

    public static JdbcThinAffinityAwarenessMappings readMappings(BinaryReaderExImpl reader,
        ClientListenerProtocolVersion ver) throws BinaryObjectException {

        JdbcThinAffinityAwarenessMappings res = new JdbcThinAffinityAwarenessMappings();

        int mappingsSize = reader.readInt();

        for (int i = 0; i < mappingsSize; i++)
            res.mappings.add(JdbcThinAffinityAwarenessMappingGroup.readGroup(reader, ver));

        return res;
    }
}
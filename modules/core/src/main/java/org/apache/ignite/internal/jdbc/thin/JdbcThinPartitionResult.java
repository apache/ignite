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

import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.processors.cache.GridCacheUtils;
import org.apache.ignite.internal.processors.odbc.ClientListenerProtocolVersion;
import org.apache.ignite.internal.sql.optimizer.affinity.PartitionNode;
import org.apache.ignite.internal.sql.optimizer.affinity.PartitionResult;
import org.apache.ignite.internal.sql.optimizer.affinity.PartitionResultMarshaler;
import org.apache.ignite.internal.sql.optimizer.affinity.PartitionTableAffinityDescriptor;

public class JdbcThinPartitionResult extends PartitionResult {

    private final int cacheId;

    public JdbcThinPartitionResult(PartitionNode tree, PartitionTableAffinityDescriptor aff, int cacheId) {
        super(tree, aff);

        this.cacheId = cacheId;
    }

    /**
     * Returns debinarized partition result.
     *
     * @param reader Binary reader.
     * @param ver Protocol verssion.
     * @return Debinarized partition result.
     * @throws BinaryObjectException On error.
     */
    public static JdbcThinPartitionResult readResult(BinaryReaderExImpl reader, ClientListenerProtocolVersion ver)
        throws BinaryObjectException {

        PartitionResult partRes = PartitionResultMarshaler.unmarshal(reader, ver);

        // TODO: 27.03.19 tmp, try to remove JdbcThinPartitionResult.
        return new JdbcThinPartitionResult(
            partRes.tree(),
            partRes.affinity(),
            partRes.tree() != null ? GridCacheUtils.cacheId(partRes.tree().cacheName()) : null);
    }

    /**
     * @return Cache Id.
     */
    public int cacheId() {
        return cacheId;
    }
}

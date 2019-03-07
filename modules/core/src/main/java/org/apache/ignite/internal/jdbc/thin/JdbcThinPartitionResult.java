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
import org.apache.ignite.internal.processors.odbc.ClientListenerProtocolVersion;
import org.apache.ignite.internal.sql.optimizer.affinity.PartitionNode;
import org.apache.ignite.internal.sql.optimizer.affinity.PartitionResult;
import org.apache.ignite.internal.sql.optimizer.affinity.PartitionTableAffinityDescriptor;

public class JdbcThinPartitionResult extends PartitionResult {

    private final String cacheName;

    public JdbcThinPartitionResult(PartitionNode tree, PartitionTableAffinityDescriptor aff, String cacheName) {
        super(tree, aff);

        this.cacheName = cacheName;
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

        PartitionNode tree = null;

        if (reader.readBoolean())
            tree = PartitionNode.readNode(reader, ver);

        return new JdbcThinPartitionResult(tree, null, tree != null ? tree.cacheName() : null);
    }

    /**
     * @return Cache name.
     */
    public String cacheName() {
        return cacheName;
    }
}

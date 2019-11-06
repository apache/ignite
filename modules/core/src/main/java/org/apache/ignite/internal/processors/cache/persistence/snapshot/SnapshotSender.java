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

package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import java.io.Closeable;
import java.io.File;
import java.util.List;
import java.util.Map;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.internal.processors.cache.persistence.partstate.GroupPartitionId;
import org.apache.ignite.internal.processors.marshaller.MappedName;

/**
 *
 */
interface SnapshotSender extends Closeable {
    /**
     * @param mappings Local node marshaller mappings.
     */
    public void sendMarshallerMeta(List<Map<Integer, MappedName>> mappings);

    /**
     * @param types Collection of known binary types.
     */
    public void sendBinaryMeta(Map<Integer, BinaryType> types);

    /**
     * @param ccfg Cache configuration file.
     * @param cacheDirName Cache group directory name.
     * @param pair Group id with partition id pair.
     */
    public void sendCacheConfig(File ccfg, String cacheDirName, GroupPartitionId pair);

    /**
     * @param part Partition file to send.
     * @param cacheDirName Cache group directory name.
     * @param pair Group id with partition id pair.
     * @param length Partition length.
     */
    public void sendPart(File part, String cacheDirName, GroupPartitionId pair, Long length);

    /**
     * @param delta Delta pages file.
     * @param cacheDirName Cache group directory name.
     * @param pair Group id with partition id pair.
     */
    public void sendDelta(File delta, String cacheDirName, GroupPartitionId pair);
}

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
import java.util.Map;
import java.util.Set;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.internal.processors.cache.persistence.partstate.GroupPartitionId;

/**
 *
 */
interface SnapshotReceiver extends Closeable {
    /**
     * @param marshallerMeta The set of marshalled objects.
     * @param ccfg Cache configuration file.
     */
    public void receiveMeta(Set<File> marshallerMeta, File ccfg);

    /**
     * @param types Collection of known binary types.
     */
    public void receiveBinaryMeta(Map<Integer, BinaryType> types);

    /**
     * @param part Partition file to receive.
     * @param cacheDirName Cache group directory name.
     * @param pair Group id with partition id pair.
     * @param length Partition length.
     */
    public void receivePart(File part, String cacheDirName, GroupPartitionId pair, Long length);

    /**
     * @param delta Delta pages file.
     * @param cacheDirName Cache group directory name.
     * @param pair Group id with partition id pair.
     */
    public void receiveDelta(File delta, String cacheDirName, GroupPartitionId pair);
}

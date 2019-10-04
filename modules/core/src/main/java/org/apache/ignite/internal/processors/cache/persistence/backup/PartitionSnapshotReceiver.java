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

package org.apache.ignite.internal.processors.cache.persistence.backup;

import java.io.Closeable;
import java.io.File;
import org.apache.ignite.internal.processors.cache.persistence.partstate.GroupPartitionId;

/**
 *
 */
interface PartitionSnapshotReceiver extends Closeable {
    /**
     * @param part Partition file to receive.
     * @param cacheDirName Cache group directory.
     * @param pair Group id with partition id pair.
     * @param length Partition length.
     */
    public void receivePart(File part, String cacheDirName, GroupPartitionId pair, Long length);

    /**
     * @param delta Delta pages file.
     * @param pair Group id with partition id pair.
     */
    public void receiveDelta(File delta, GroupPartitionId pair);
}

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

package org.apache.ignite.dump;

import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.StoredCacheData;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotMetadata;
import org.apache.ignite.lang.IgniteExperimental;

/**
 *
 */
@IgniteExperimental
public interface Dump {
    /** @return List of node directories. */
    List<String> nodesDirectories();

    /** @return List of snapshot metadata saved in dump directory. */
    List<SnapshotMetadata> metadata() throws IOException, IgniteCheckedException;

    /**
     * @param node Node directory name.
     * @param group Group id.
     * @return List of cache configs saved in dump for group.
     */
    List<StoredCacheData> configs(String node, int group);

    /**
     * @param node Node directory name.
     * @param group Group id.
     * @return Dump iterator.
     */
    List<Integer> partitions(String node, int group);

    /**
     * @param node Node directory name.
     * @param group Group id.
     * @param part Partition id.
     * @return Dump iterator.
     */
    DumpedPartitionIterator iterator(String node, int group, int part);

    /** @return Root dump directory. */
    File dumpDirectory();
}

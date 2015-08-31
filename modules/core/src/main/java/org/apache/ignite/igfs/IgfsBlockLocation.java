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

package org.apache.ignite.igfs;

import java.util.Collection;
import java.util.UUID;

/**
 * {@code IGFS} file's data block location in the grid. It is used to determine
 * node affinity of a certain file block within the Grid by calling
 * {@link org.apache.ignite.IgniteFileSystem#affinity(IgfsPath, long, long)} method.
 */
public interface IgfsBlockLocation {
    /**
     * Start position in the file this block relates to.
     *
     * @return Start position in the file this block relates to.
     */
    public long start();

    /**
     * Length of the data block in the file.
     *
     * @return Length of the data block in the file.
     */
    public long length();

    /**
     * Nodes this block belongs to. First node id in collection is
     * primary node id.
     *
     * @return Nodes this block belongs to.
     */
    public Collection<UUID> nodeIds();

    /**
     * Compliant with Hadoop interface.
     *
     * @return Collection of host:port addresses.
     */
    public Collection<String> names();

    /**
     * Compliant with Hadoop interface.
     *
     * @return Collection of host names.
     */
    public Collection<String> hosts();
}
/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.igfs;

import org.apache.ignite.configuration.DataRegionConfiguration;

/**
 * {@code IGFS} metrics snapshot for the file system. Note, that some metrics are global and
 * some are local (i.e. per each node).
 */
public interface IgfsMetrics {
    /**
     * Gets local used space in bytes. This is the sum of all file chunks stored on local node.
     * <p>
     * This is a local metric.
     *
     * @return Node used space in bytes.
     */
    public long localSpaceSize();

    /**
     * Gets maximum amount of data that can be stored on local node. This metrics is related to
     * to the {@link DataRegionConfiguration#getMaxSize()} of the IGFS data cache.
     *
     * @return Maximum IGFS local space size.
     */
    public long maxSpaceSize();

    /**
    * Get used space in bytes used in the secondary file system.
    * <p>
    * This is a global metric.
    *
    * @return Used space in the secondary file system or {@code 0} in case no secondary file system is configured.
    */
    public long secondarySpaceSize();

    /**
     * Gets number of directories created in file system.
     * <p>
     * This is a global metric.
     *
     * @return Number of directories.
     */
    public int directoriesCount();

    /**
     * Gets number of files stored in file system.
     * <p>
     * This is a global metric.
     *
     * @return Number of files.
     */
    public int filesCount();

    /**
     * Gets number of files that are currently opened for reading.
     * <p>
     * This is a local metric.
     *
     * @return Number of opened files.
     */
    public int filesOpenedForRead();

    /**
     * Gets number of files that are currently opened for writing.
     * <p>
     * This is a local metric.
     *
     * @return Number of opened files.
     */
    public int filesOpenedForWrite();

    /**
     * Gets total blocks read, local and remote.
     * <p>
     * This is a local metric.
     *
     * @return Total blocks read.
     */
    public long blocksReadTotal();

    /**
     * Gets total remote blocks read.
     * <p>
     * This is a local metric.
     *
     * @return Total blocks remote read.
     */
    public long blocksReadRemote();

    /**
     * Gets total blocks written, local and remote.
     * <p>
     * This is a local metric.
     *
     * @return Total blocks written.
     */
    public long blocksWrittenTotal();

    /**
     * Gets total remote blocks written.
     * <p>
     * This is a local metric.
     *
     * @return Total blocks written.
     */
    public long blocksWrittenRemote();

    /**
     * Gets total bytes read.
     * <p>
     * This is a local metric.
     *
     * @return Total bytes read.
     */
    public long bytesRead();

    /**
     * Gets total bytes read time.
     * <p>
     * This is a local metric.
     *
     * @return Total bytes read time.
     */
    public long bytesReadTime();

    /**
     * Gets total bytes written.
     * <p>
     * This is a local metric.
     *
     * @return Total bytes written.
     */
    public long bytesWritten();

    /**
     * Gets total bytes write time.
     * <p>
     * This is a local metric.
     *
     * @return Total bytes write time.
     */
    public long bytesWriteTime();
}
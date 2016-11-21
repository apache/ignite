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

package org.apache.ignite.internal.processors.igfs;

import java.util.Collection;
import org.apache.ignite.IgniteException;
import org.apache.ignite.igfs.IgfsBlockLocation;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.igfs.IgfsPathNotFoundException;
import org.apache.ignite.igfs.secondary.IgfsSecondaryFileSystem;

/**
 * Extended version of secondary file system with missing methods.
 *
 * @deprecated Will be removed in Apache Ignite 2.0. Methods will be merged to {@link IgfsSecondaryFileSystem}.
 */
@Deprecated
public interface IgfsSecondaryFileSystemV2 extends IgfsSecondaryFileSystem {
    /**
     * Set times for the given path.
     *
     * @param path Path.
     * @param accessTime Access time.
     * @param modificationTime Modification time.
     * @throws IgniteException If failed.
     */
    public void setTimes(IgfsPath path, long accessTime, long modificationTime) throws IgniteException;

    /**
     * Get affinity block locations for data blocks of the file. In case {@code maxLen} parameter is set and
     * particular block location length is greater than this value, block locations will be split into smaller
     * chunks.
     *
     * @param path File path to get affinity for.
     * @param start Position in the file to start affinity resolution from.
     * @param len Size of data in the file to resolve affinity for.
     * @param maxLen Maximum length of a single returned block location length.
     *
     * @return Affinity block locations.
     * @throws IgniteException In case of error.
     * @throws IgfsPathNotFoundException If path doesn't exist.
     */
    public Collection<IgfsBlockLocation> affinity(IgfsPath path, long start, long len, long maxLen)
        throws IgniteException;
}

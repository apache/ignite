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

import java.util.Map;
import org.jetbrains.annotations.Nullable;

/**
 * {@code IGFS} file or directory descriptor. For example, to get information about
 * a file you would use the following code:
 * <pre name="code" class="java">
 *     IgfsPath filePath = new IgfsPath("my/working/dir", "file.txt");
 *
 *     // Get metadata about file.
 *     IgfsFile file = igfs.info(filePath);
 * </pre>
 */
public interface IgfsFile {
    /**
     * Gets path to file.
     *
     * @return Path to file.
     */
    public IgfsPath path();

    /**
     * Check this file is a data file.
     *
     * @return {@code True} if this is a data file.
     */
    public boolean isFile();

    /**
     * Check this file is a directory.
     *
     * @return {@code True} if this is a directory.
     */
    public boolean isDirectory();

    /**
     * Gets file's length.
     *
     * @return File's length or {@code zero} for directories.
     */
    public long length();

    /**
     * Gets file's data block size.
     *
     * @return File's data block size or {@code zero} for directories.
     */
    public int blockSize();

    /**
     * Gets file group block size (i.e. block size * group size).
     *
     * @return File group block size.
     */
    public long groupBlockSize();

    /**
     * Gets file last access time. File last access time is not updated automatically due to
     * performance considerations and can be updated on demand with
     * {@link org.apache.ignite.IgniteFileSystem#setTimes(IgfsPath, long, long)} method.
     * <p>
     * By default last access time equals file creation time.
     *
     * @return Last access time.
     */
    public long accessTime();

    /**
     * Gets file last modification time. File modification time is updated automatically on each file write and
     * append.
     *
     * @return Last modification time.
     */
    public long modificationTime();

    /**
     * Get file's property for specified name.
     *
     * @param name Name of the property.
     * @return File's property for specified name.
     * @throws IllegalArgumentException If requested property was not found.
     */
    public String property(String name) throws IllegalArgumentException;

    /**
     * Get file's property for specified name.
     *
     * @param name Name of the property.
     * @param dfltVal Default value if requested property was not found.
     * @return File's property for specified name.
     */
    @Nullable public String property(String name, @Nullable String dfltVal);

    /**
     * Get properties of the file.
     *
     * @return Properties of the file.
     */
    public Map<String, String> properties();
}
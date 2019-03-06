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
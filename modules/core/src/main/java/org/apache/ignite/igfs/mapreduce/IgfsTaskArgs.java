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

package org.apache.ignite.igfs.mapreduce;

import java.util.Collection;
import org.apache.ignite.igfs.IgfsPath;

/**
 * IGFS task arguments. When you initiate new IGFS task execution using one of {@code IgniteFs.execute(...)} methods,
 * all passed parameters are encapsulated in a single {@code IgfsTaskArgs} object. Later on this object is
 * passed to {@link IgfsTask#createJob(org.apache.ignite.igfs.IgfsPath, IgfsFileRange, IgfsTaskArgs)} method.
 * <p>
 * Task arguments encapsulates the following data:
 * <ul>
 *     <li>IGFS name</li>
 *     <li>File paths passed to {@code IgniteFs.execute()} method</li>
 *     <li>{@link IgfsRecordResolver} for that task</li>
 *     <li>Flag indicating whether to skip non-existent file paths or throw an exception</li>
 *     <li>User-defined task argument</li>
 *     <li>Maximum file range length for that task (see {@link org.apache.ignite.configuration.FileSystemConfiguration#getMaximumTaskRangeLength()})</li>
 * </ul>
 */
public interface IgfsTaskArgs<T> {
    /**
     * Gets IGFS name.
     *
     * @return IGFS name.
     */
    public String igfsName();

    /**
     * Gets file paths to process.
     *
     * @return File paths to process.
     */
    public Collection<IgfsPath> paths();

    /**
     * Gets record resolver for the task.
     *
     * @return Record resolver.
     */
    public IgfsRecordResolver recordResolver();

    /**
     * Flag indicating whether to fail or simply skip non-existent files.
     *
     * @return {@code True} if non-existent files should be skipped.
     */
    public boolean skipNonExistentFiles();

    /**
     * User argument provided for task execution.
     *
     * @return User argument.
     */
    public T userArgument();

    /**
     * Optional maximum allowed range length, {@code 0} by default. If not specified, full range including
     * all consecutive blocks will be used without any limitations.
     *
     * @return Maximum range length.
     */
    public long maxRangeLength();
}
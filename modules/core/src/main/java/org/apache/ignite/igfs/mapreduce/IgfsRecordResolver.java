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

import java.io.IOException;
import java.io.Serializable;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteFileSystem;
import org.apache.ignite.igfs.IgfsInputStream;
import org.jetbrains.annotations.Nullable;

/**
 * IGFS record resolver. When {@link IgfsTask} is split into {@link IgfsJob}s each produced job will obtain
 * {@link IgfsFileRange} based on file data location. Record resolver is invoked in each job before actual
 * execution in order to adjust record boundaries in a way consistent with user data.
 * <p>
 * E.g., you may want to split your task into jobs so that each job process zero, one or several lines from that file.
 * But file is split into ranges based on block locations, not new line boundaries. Using convenient record resolver
 * you can adjust job range so that it covers the whole line(s).
 * <p>
 * The following record resolvers are available out of the box:
 * <ul>
 *     <li>{@link org.apache.ignite.igfs.mapreduce.records.IgfsFixedLengthRecordResolver}</li>
 *     <li>{@link org.apache.ignite.igfs.mapreduce.records.IgfsByteDelimiterRecordResolver}</li>
 *     <li>{@link org.apache.ignite.igfs.mapreduce.records.IgfsStringDelimiterRecordResolver}</li>
 *     <li>{@link org.apache.ignite.igfs.mapreduce.records.IgfsNewLineRecordResolver}</li>
 * </ul>
 */
public interface IgfsRecordResolver extends Serializable {
    /**
     * Adjusts record start offset and length.
     *
     * @param fs IGFS instance to use.
     * @param stream Input stream for split file.
     * @param suggestedRecord Suggested file system record.
     * @return New adjusted record. If this method returns {@code null}, original record is ignored.
     * @throws IgniteException If resolve failed.
     * @throws IOException If resolve failed.
     */
    @Nullable public IgfsFileRange resolveRecords(IgniteFileSystem fs, IgfsInputStream stream,
        IgfsFileRange suggestedRecord) throws IgniteException, IOException;
}
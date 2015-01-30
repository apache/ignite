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

package org.apache.ignite.fs.mapreduce;

import org.apache.ignite.*;
import org.apache.ignite.fs.*;
import org.jetbrains.annotations.*;

import java.io.*;

/**
 * GGFS record resolver. When {@link IgniteFsTask} is split into {@link IgniteFsJob}s each produced job will obtain
 * {@link IgniteFsFileRange} based on file data location. Record resolver is invoked in each job before actual
 * execution in order to adjust record boundaries in a way consistent with user data.
 * <p>
 * E.g., you may want to split your task into jobs so that each job process zero, one or several lines from that file.
 * But file is split into ranges based on block locations, not new line boundaries. Using convenient record resolver
 * you can adjust job range so that it covers the whole line(s).
 * <p>
 * The following record resolvers are available out of the box:
 * <ul>
 *     <li>{@link org.apache.ignite.fs.mapreduce.records.IgniteFsFixedLengthRecordResolver}</li>
 *     <li>{@link org.apache.ignite.fs.mapreduce.records.IgniteFsByteDelimiterRecordResolver}</li>
 *     <li>{@link org.apache.ignite.fs.mapreduce.records.IgniteFsStringDelimiterRecordResolver}</li>
 *     <li>{@link org.apache.ignite.fs.mapreduce.records.IgniteFsNewLineRecordResolver}</li>
 * </ul>
 */
public interface IgniteFsRecordResolver extends Serializable {
    /**
     * Adjusts record start offset and length.
     *
     * @param ggfs GGFS instance to use.
     * @param stream Input stream for split file.
     * @param suggestedRecord Suggested file system record.
     * @return New adjusted record. If this method returns {@code null}, original record is ignored.
     * @throws IgniteException If resolve failed.
     * @throws IOException If resolve failed.
     */
    @Nullable public IgniteFsFileRange resolveRecords(IgniteFs ggfs, IgniteFsInputStream stream,
        IgniteFsFileRange suggestedRecord) throws IgniteException, IOException;
}

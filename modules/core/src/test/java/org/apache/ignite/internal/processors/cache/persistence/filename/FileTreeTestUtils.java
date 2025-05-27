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

package org.apache.ignite.internal.processors.cache.persistence.filename;

import java.io.File;
import java.util.regex.Pattern;

import static org.apache.ignite.internal.processors.cache.persistence.filename.NodeFileTree.PART_FILE_PREFIX;
import static org.apache.ignite.internal.processors.cache.persistence.filename.NodeFileTree.WAL_SEGMENT_COMPACTED_OR_RAW_FILE_FILTER;
import static org.apache.ignite.internal.processors.cache.persistence.filename.NodeFileTree.WAL_SEGMENT_FILE_COMPACTED_FILTER;

/** */
public class FileTreeTestUtils {
    /**
     * @return Pattern to find partition files.
     */
    public static Pattern partitionFilePattern() {
        return Pattern.compile('^' + Pattern.quote(PART_FILE_PREFIX) + ".*");
    }

    /**
     * @param f Directory to scan for WAL compacted or raw files.
     * @return WAL compacted or raw files.
     */
    public static File[] walCompactedOrRawFiles(File f) {
        return f.listFiles(WAL_SEGMENT_COMPACTED_OR_RAW_FILE_FILTER);
    }

    /**
     * @param ft Node file tree.
     * @return WAL archive compacted files.
     */
    public static File[] walArchiveCompactedFiles(NodeFileTree ft) {
        return ft.walArchive().listFiles(WAL_SEGMENT_FILE_COMPACTED_FILTER);
    }
}

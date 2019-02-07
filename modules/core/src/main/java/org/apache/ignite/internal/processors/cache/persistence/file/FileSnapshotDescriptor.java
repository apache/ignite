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

package org.apache.ignite.internal.processors.cache.persistence.file;

import java.io.File;
import org.apache.ignite.IgniteException;

/** */
public class FileSnapshotDescriptor {
    /** */
    private File file;

    /** */
    private SnapshotFileType type;

    /** */
    private long offset;

    /** */
    private long count;

    /**
     * @param file A representation of partiton or delta file.
     * @param type The type of corresponding file.
     * @param offset Position to start with.
     * @param count The count of processing bits.
     */
    public FileSnapshotDescriptor(File file, SnapshotFileType type, long offset, long count) {
        this.file = file;
        this.type = type;
        this.offset = offset;
        this.count = count;
    }

    /**
     * @param file A representation of partiton or delta file.
     * @param type The type of corresponding file.
     * @param offset Position to start with.
     * @param count The count of processing bits.
     */
    public FileSnapshotDescriptor(File file, int type, long offset, long count) {
        this(file, SnapshotFileType.get(type), offset, count);
    }

    /**
     * @return Underlying file.
     */
    public File getFile() {
        return file;
    }

    /**
     * @return Corresponding file type.
     */
    public SnapshotFileType getType() {
        return type;
    }

    /**
     * @return The position of in file to start with.
     */
    public long getOffset() {
        return offset;
    }

    /**
     * @return The count of processing bits.
     */
    public long getCount() {
        return count;
    }

    /** */
    public enum SnapshotFileType {
        /** */
        ORIG,
        /** */
        DELTA;

        /** */
        public static SnapshotFileType get(int type) {
            if (SnapshotFileType.values().length > type)
                throw new IgniteException("Unknown file type");

            return SnapshotFileType.values()[type];
        }
    }
}

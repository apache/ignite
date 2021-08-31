/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.commandline.indexreader;

import java.io.File;

import org.apache.ignite.internal.util.typedef.internal.U;

import static java.lang.String.format;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.INDEX_PARTITION;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.INDEX_FILE_NAME;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.PART_FILE_TEMPLATE;

/**
 * Collection of utility methods used throughout index reading.
 */
public class IndexReaderUtils {
    /**
     * Private constructor.
     */
    private IndexReaderUtils() {
        throw new UnsupportedOperationException();
    }

    /**
     * Align value to base.
     *
     * @param val  Value.
     * @param base Base.
     * @return Base-aligned value.
     */
    public static long align(long val, int base) {
        if ((val % base) == 0)
            return val;
        else
            return ((val / base) + 1) * base;
    }

    /**
     * Returns approximate link size in bytes.
     *
     * @return Approximate link size in bytes.
     */
    public static long linkSize() {
        return U.jvm32Bit() ? 4 : 8;
    }

    /**
     * Approximate calculation of object size in bytes.
     *
     * @param fieldsSize Approximate fields size in bytes.
     * @return Approximate object size in bytes.
     */
    public static long objectSize(long fieldsSize) {
        return align(/*Object header*/(2 * linkSize()) + fieldsSize, 8);
    }

    /**
     * Approximate calculation of array objects size in bytes.
     *
     * @param len Length of array.
     * @return Approximate array objects size in bytes.
     */
    public static long objectArraySize(int len) {
        return align(/*Object header*/(2 * linkSize()) + (linkSize() * len), 8);
    }

    /**
     * Returns the absolute pathname string of partition.
     *
     * @param partId Partition number.
     * @param parent Partition directory.
     * @return Partition absolute pathname string.
     */
    public static String partitionFileAbsolutePath(File parent, int partId) {
        String fileName = partId == INDEX_PARTITION ? INDEX_FILE_NAME : format(PART_FILE_TEMPLATE, partId);

        return new File(parent, fileName).getAbsolutePath();
    }
}

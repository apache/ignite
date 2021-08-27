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
import java.nio.ByteBuffer;
import java.util.Collection;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStore;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.jetbrains.annotations.Nullable;

import static java.lang.String.format;
import static java.util.Objects.nonNull;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.INDEX_PARTITION;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.FILE_SUFFIX;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.INDEX_FILE_NAME;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.PART_FILE_TEMPLATE;

/**
 * Factory {@link FilePageStore} for analyzing partition and index files.
 */
public interface IgniteIndexReaderFilePageStoreFactory {
    /**
     * Creating new {@link FilePageStore}. It can return {@code null} if partition file were not found,
     * for example: node should not contain it by affinity.
     *
     * @param partId Partition ID.
     * @param type Data type, can be {@link PageIdAllocator#FLAG_IDX} or {@link PageIdAllocator#FLAG_DATA}.
     * @param errors Errors while reading partition.
     * @return New instance of {@link FilePageStore} or {@code null}.
     * @throws IgniteCheckedException If there are errors when creating {@link FilePageStore}.
     */
    @Nullable FilePageStore createFilePageStore(int partId, byte type, Collection<Throwable> errors) throws IgniteCheckedException;

    /**
     * Creating new {@link FilePageStore} and initializing it.
     * It can return {@code null} if partition file were not found, for example: node should not contain it by affinity.
     *
     * @param partId Partition ID.
     * @param type Data type, can be {@link PageIdAllocator#FLAG_IDX} or {@link PageIdAllocator#FLAG_DATA}.
     * @param errors Errors while reading partition.
     * @return New instance of {@link FilePageStore} or {@code null}.
     * @throws IgniteCheckedException If there are errors when creating or initializing {@link FilePageStore}.
     */
    @Nullable default FilePageStore createFilePageStoreWithEnsure(
        int partId,
        byte type,
        Collection<Throwable> errors
    ) throws IgniteCheckedException {
        FilePageStore filePageStore = createFilePageStore(partId, type, errors);

        if (nonNull(filePageStore))
            filePageStore.ensure();

        return filePageStore;
    }

    /**
     * Create buffer with header.
     *
     * @param type Data type, can be {@link PageIdAllocator#FLAG_IDX} or {@link PageIdAllocator#FLAG_DATA}.
     * @return New buffer with header.
     */
    default ByteBuffer headerBuffer(byte type) throws IgniteCheckedException {
        throw new UnsupportedOperationException();
    }

    /**
     * Getting a partition or index file that may not exist.
     *
     * @param dir Directory to get partition or index file.
     * @param partId ID of partition or index.
     * @param fileExt File extension if it differs from {@link FilePageStoreManager#FILE_SUFFIX}.
     * @return Partition or index file that may not exist.
     */
    default File getFile(File dir, int partId, @Nullable String fileExt) {
        String fileName = partId == INDEX_PARTITION ? INDEX_FILE_NAME : format(PART_FILE_TEMPLATE, partId);

        if (nonNull(fileExt) && !FILE_SUFFIX.equals(fileExt))
            fileName = fileName.replace(FILE_SUFFIX, fileExt);

        return new File(dir, fileName);
    }

    /**
     * Return page size.
     *
     * @return Page size.
     */
    int pageSize();

    /**
     * Return partition count.
     *
     * @return Partition count.
     */
    int partitionCount();
}

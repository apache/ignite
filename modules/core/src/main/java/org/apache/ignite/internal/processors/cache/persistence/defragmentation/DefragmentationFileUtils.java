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

package org.apache.ignite.internal.processors.cache.persistence.defragmentation;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.filename.CacheFileTree;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.INDEX_PARTITION;
import static org.apache.ignite.internal.processors.cache.persistence.filename.CacheFileTree.extractPartId;

/**
 * Everything related to file management during defragmentation process.
 */
public class DefragmentationFileUtils {
    /**
     * Performs cleanup of work dir before initializing file page stores.
     * Will finish batch renaming if defragmentation was completed or delete garbage if it wasn't.
     *
     * @param cft Cache file tree.
     * @throws IgniteCheckedException If {@link IOException} occurred.
     */
    public static void beforeInitPageStores(CacheFileTree cft) throws IgniteCheckedException {
        try {
            batchRenameDefragmentedCacheGroupPartitions(cft);

            U.delete(cft.defragmentationCompletionMarkerFile());

            deleteLeftovers(cft);
        }
        catch (IgniteException e) {
            throw new IgniteCheckedException(e);
        }
    }

    /**
     * Deletes all defragmentation related file from work directory, except for completion marker.
     *
     * @param cft Cache file tree.
     */
    public static void deleteLeftovers(CacheFileTree cft) {
        for (File storage : cft.storages()) {
            for (File file : storage.listFiles()) {
                String fileName = file.getName();

                if (
                    CacheFileTree.isDefragmentPartition(fileName)
                        || CacheFileTree.isDefragmentIndex(fileName)
                        || CacheFileTree.isDefragmentLinkMapping(fileName)
                )
                    U.delete(file);
            }
        }
    }

    /**
     * Checks whether cache group defragmentation completed or not. Completes it if all that's left is renaming.
     *
     * @param cft Cache file tree.
     * @param log Logger to write messages.
     * @return {@code true} if given cache group is already defragmented.
     * @throws IgniteException If {@link IOException} occurred.
     *
     * @see CacheFileTree#defragmentationCompletionMarkerFile()
     */
    public static boolean skipAlreadyDefragmentedCacheGroup(CacheFileTree cft, IgniteLogger log) throws IgniteException {
        File completionMarkerFile = cft.defragmentationCompletionMarkerFile();

        if (completionMarkerFile.exists()) {
            if (log.isInfoEnabled()) {
                log.info(S.toString(
                    "Skipping already defragmented page group",
                    "grpId", cft.groupId(), false,
                    "markerFileName", completionMarkerFile.getName(), false,
                    "workDir", Arrays.toString(cft.storages()), false
                ));
            }

            batchRenameDefragmentedCacheGroupPartitions(cft);

            return true;
        }

        return false;
    }

    /**
     * Checks whether partition has already been defragmented or not. Cleans corrupted data if previous failed
     * defragmentation attempt was found.
     *
     * @param cft Cache file tree.
     * @param partId Partition index to check.
     * @param log Logger to write messages.
     * @return {@code true} if given partition is already defragmented.
     * @throws IgniteException If {@link IOException} occurred.
     *
     * @see CacheFileTree#defragmentedPartTmpFile(int)
     * @see CacheFileTree#defragmentedPartFile(int)
     * @see CacheFileTree#defragmentedPartMappingFile(int)
     */
    public static boolean skipAlreadyDefragmentedPartition(CacheFileTree cft, int partId, IgniteLogger log) throws IgniteException {
        File defragmentedPartFile = cft.defragmentedPartFile(partId);
        File defragmentedPartMappingFile = cft.defragmentedPartMappingFile(partId);

        if (defragmentedPartFile.exists() && defragmentedPartMappingFile.exists()) {
            if (log.isInfoEnabled()) {
                log.info(S.toString(
                    "Skipping already defragmented partition",
                    "grpId", cft.groupId(), false,
                    "partId", partId, false,
                    "partFileName", defragmentedPartFile.getName(), false,
                    "mappingFileName", defragmentedPartMappingFile.getName(), false,
                    "workDir", Arrays.toString(cft.storages()), false
                ));
            }

            return true;
        }

        File defragmentedPartTmpFile = cft.defragmentedPartTmpFile(partId);

        try {
            Files.deleteIfExists(defragmentedPartTmpFile.toPath());

            Files.deleteIfExists(defragmentedPartFile.toPath());

            Files.deleteIfExists(defragmentedPartMappingFile.toPath());
        }
        catch (IOException e) {
            throw new IgniteException(e);
        }

        return false;
    }

    /**
     * Failure-tolerant batch rename of defragmented partition files.
     *
     * Deletes all link mapping files old partition and index files, renaming defragmentated files in the process. Can
     * be run on the same folder multiple times if failed for some reason.
     *
     * Does something only if completion marker is present in the folder. This marker won't be deleted in the end.
     * Deletion of the marker must be done outside of defragmentation mode to prevent cache groups to be defragmentated
     * several times in case of failures.
     *
     * @param cft Cache file tree.
     * @throws IgniteException If {@link IOException} occurred.
     *
     * @see DefragmentationFileUtils#writeDefragmentationCompletionMarker(FileIOFactory, CacheFileTree, IgniteLogger)
     */
    public static void batchRenameDefragmentedCacheGroupPartitions(CacheFileTree cft) throws IgniteException {
        File completionMarkerFile = cft.defragmentationCompletionMarkerFile();

        if (!completionMarkerFile.exists())
            return;

        try {
            for (File storage : cft.storages()) {
                for (File mappingFile : storage.listFiles((dir, name) -> CacheFileTree.isDefragmentLinkMapping(name)))
                    Files.delete(mappingFile.toPath());

                for (File partFile : storage.listFiles((dir, name) -> CacheFileTree.isDefragmentPartition(name))) {
                    int partId = extractPartId(partFile.getName());

                    File oldPartFile = cft.partitionFile(partId);

                    Files.move(partFile.toPath(), oldPartFile.toPath(), ATOMIC_MOVE, REPLACE_EXISTING);
                }
            }

            File idxFile = cft.defragmentedIndexFile();

            if (idxFile.exists()) {
                File oldIdxFile = cft.partitionFile(INDEX_PARTITION);

                Files.move(idxFile.toPath(), oldIdxFile.toPath(), ATOMIC_MOVE, REPLACE_EXISTING);
            }
        }
        catch (IOException e) {
            throw new IgniteException(e);
        }
    }

    /**
     * Rename temporary index defragmentation file to a finalized one.
     *
     * @param cft Cache file tree.
     * @throws IgniteException If {@link IOException} occurred.
     *
     * @see CacheFileTree#defragmentedIndexTmpFile()
     * @see CacheFileTree#defragmentedIndexFile()
     */
    public static void renameTempIndexFile(CacheFileTree cft) throws IgniteException {
        File defragmentedIdxTmpFile = cft.defragmentedIndexTmpFile();
        File defragmentedIdxFile = cft.defragmentedIndexFile();

        try {
            Files.deleteIfExists(defragmentedIdxFile.toPath());

            Files.move(defragmentedIdxTmpFile.toPath(), defragmentedIdxFile.toPath(), ATOMIC_MOVE);
        }
        catch (IOException e) {
            throw new IgniteException(e);
        }
    }

    /**
     * Rename temporary partition defragmentation file to a finalized one.
     *
     * @param cft Cache file tree.
     * @param partId Partition index.
     * @throws IgniteException If {@link IOException} occurred.
     *
     * @see CacheFileTree#defragmentedPartTmpFile(int)
     * @see CacheFileTree#defragmentedPartFile(int)
     */
    public static void renameTempPartitionFile(CacheFileTree cft, int partId) throws IgniteException {
        File defragmentedPartTmpFile = cft.defragmentedPartTmpFile(partId);
        File defragmentedPartFile = cft.defragmentedPartFile(partId);

        assert !defragmentedPartFile.exists() : defragmentedPartFile;

        try {
            Files.move(defragmentedPartTmpFile.toPath(), defragmentedPartFile.toPath(), ATOMIC_MOVE);
        }
        catch (IOException e) {
            throw new IgniteException(e);
        }
    }

    /**
     * Creates empty completion marker file in given directory.
     *
     * @param ioFactory File IO factory.
     * @param cft Cache file tree.
     * @param log Logger to write messages.
     * @throws IgniteException If {@link IOException} occurred.
     *
     * @see CacheFileTree#defragmentationCompletionMarkerFile()
     */
    public static void writeDefragmentationCompletionMarker(
        FileIOFactory ioFactory,
        CacheFileTree cft,
        IgniteLogger log
    ) throws IgniteException {
        File completionMarker = cft.defragmentationCompletionMarkerFile();

        try (FileIO io = ioFactory.create(completionMarker, CREATE_NEW, WRITE)) {
            io.force(true);
        }
        catch (IOException e) {
            throw new IgniteException(e);
        }
    }
}

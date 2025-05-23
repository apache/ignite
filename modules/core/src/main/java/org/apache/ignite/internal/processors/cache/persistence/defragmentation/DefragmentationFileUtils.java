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
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.filename.DefragmentationFileTreeUtils;
import org.apache.ignite.internal.processors.cache.persistence.filename.NodeFileTree;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.INDEX_PARTITION;
import static org.apache.ignite.internal.processors.cache.persistence.filename.DefragmentationFileTreeUtils.defragmentationCompletionMarkerFile;
import static org.apache.ignite.internal.processors.cache.persistence.filename.DefragmentationFileTreeUtils.defragmentedIndexFile;
import static org.apache.ignite.internal.processors.cache.persistence.filename.DefragmentationFileTreeUtils.defragmentedIndexTmpFile;
import static org.apache.ignite.internal.processors.cache.persistence.filename.DefragmentationFileTreeUtils.defragmentedPartFile;
import static org.apache.ignite.internal.processors.cache.persistence.filename.DefragmentationFileTreeUtils.defragmentedPartMappingFile;
import static org.apache.ignite.internal.processors.cache.persistence.filename.DefragmentationFileTreeUtils.defragmentedPartTmpFile;
import static org.apache.ignite.internal.processors.cache.persistence.filename.NodeFileTree.partitionFileName;

/**
 * Everything related to file management during defragmentation process.
 */
public class DefragmentationFileUtils {

    /**
     * Performs cleanup of work dir before initializing file page stores.
     * Will finish batch renaming if defragmentation was completed or delete garbage if it wasn't.
     *
     * @param workDir Cache group working directory.
     * @param log Logger to write messages.
     * @throws IgniteCheckedException If {@link IOException} occurred.
     */
    public static void beforeInitPageStores(File workDir, IgniteLogger log) throws IgniteCheckedException {
        try {
            batchRenameDefragmentedCacheGroupPartitions(workDir, log);

            U.delete(defragmentationCompletionMarkerFile(workDir));

            deleteLeftovers(workDir);
        }
        catch (IgniteException e) {
            throw new IgniteCheckedException(e);
        }
    }

    /**
     * Deletes all defragmentation related file from work directory, except for completion marker.
     *
     * @param workDir Cache group working directory.
     */
    public static void deleteLeftovers(File workDir) {
        for (File file : workDir.listFiles()) {
            if (
                DefragmentationFileTreeUtils.defragmentationPartitionFile(file)
                    || DefragmentationFileTreeUtils.defragmentationIndexFile(file)
                    || DefragmentationFileTreeUtils.defragmentationLinkMappingFile(file)
            )
                U.delete(file);
        }
    }

    /**
     * Checks whether cache group defragmentation completed or not. Completes it if all that's left is renaming.
     *
     * @param ft Node file tree.
     * @param gctx Group context.
     * @param log Logger to write messages.
     * @return {@code true} if given cache group is already defragmented.
     * @throws IgniteException If {@link IOException} occurred.
     *
     * @see DefragmentationFileTreeUtils#defragmentationCompletionMarkerFile(File)
     */
    public static boolean skipAlreadyDefragmentedCacheGroup(
        NodeFileTree ft,
        CacheGroupContext gctx,
        IgniteLogger log
    ) throws IgniteException {
        File workDir = ft.cacheStorage(gctx.config());
        File completionMarkerFile = defragmentationCompletionMarkerFile(workDir);

        if (completionMarkerFile.exists()) {
            if (log.isInfoEnabled()) {
                log.info(S.toString(
                    "Skipping already defragmented page group",
                    "grpId", gctx.groupId(), false,
                    "markerFileName", completionMarkerFile.getName(), false,
                    "workDir", workDir.getAbsolutePath(), false
                ));
            }

            batchRenameDefragmentedCacheGroupPartitions(workDir, log);

            return true;
        }

        return false;
    }

    /**
     * Checks whether partition has already been defragmented or not. Cleans corrupted data if previous failed
     * defragmentation attempt was found.
     *
     * @param workDir Cache group working directory.
     * @param grpId Cache group Id of cache group belonging to the given working directory.
     * @param partId Partition index to check.
     * @param log Logger to write messages.
     * @return {@code true} if given partition is already defragmented.
     * @throws IgniteException If {@link IOException} occurred.
     *
     * @see DefragmentationFileTreeUtils#defragmentedPartTmpFile(File, int)
     * @see DefragmentationFileTreeUtils#defragmentedPartFile(File, int)
     * @see DefragmentationFileTreeUtils#defragmentedPartMappingFile(File, int)
     */
    public static boolean skipAlreadyDefragmentedPartition(File workDir, int grpId, int partId, IgniteLogger log) throws IgniteException {
        File defragmentedPartFile = defragmentedPartFile(workDir, partId);
        File defragmentedPartMappingFile = defragmentedPartMappingFile(workDir, partId);

        if (defragmentedPartFile.exists() && defragmentedPartMappingFile.exists()) {
            if (log.isInfoEnabled()) {
                log.info(S.toString(
                    "Skipping already defragmented partition",
                    "grpId", grpId, false,
                    "partId", partId, false,
                    "partFileName", defragmentedPartFile.getName(), false,
                    "mappingFileName", defragmentedPartMappingFile.getName(), false,
                    "workDir", workDir.getAbsolutePath(), false
                ));
            }

            return true;
        }

        File defragmentedPartTmpFile = defragmentedPartTmpFile(workDir, partId);

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
     * @param workDir Cache group working directory.
     * @param log Logger to write messages.
     * @throws IgniteException If {@link IOException} occurred.
     *
     * @see DefragmentationFileUtils#writeDefragmentationCompletionMarker(FileIOFactory, File, IgniteLogger)
     */
    public static void batchRenameDefragmentedCacheGroupPartitions(File workDir, IgniteLogger log) throws IgniteException {
        File completionMarkerFile = defragmentationCompletionMarkerFile(workDir);

        if (!completionMarkerFile.exists())
            return;

        try {
            for (File mappingFile : workDir.listFiles(DefragmentationFileTreeUtils::defragmentationLinkMappingFile))
                Files.delete(mappingFile.toPath());

            for (File partFile : workDir.listFiles(DefragmentationFileTreeUtils::defragmentationPartitionFile)) {
                int partId = DefragmentationFileTreeUtils.partId(partFile);

                File oldPartFile = new File(workDir, partitionFileName(partId));

                Files.move(partFile.toPath(), oldPartFile.toPath(), ATOMIC_MOVE, REPLACE_EXISTING);
            }

            File idxFile = DefragmentationFileTreeUtils.defragmentedIndexFile(workDir);

            if (idxFile.exists()) {
                File oldIdxFile = new File(workDir, partitionFileName(INDEX_PARTITION));

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
     * @param workDir Cache group working directory.
     * @throws IgniteException If {@link IOException} occurred.
     *
     * @see DefragmentationFileTreeUtils#defragmentedIndexTmpFile(File)
     * @see DefragmentationFileTreeUtils#defragmentedIndexFile(File)
     */
    public static void renameTempIndexFile(File workDir) throws IgniteException {
        File defragmentedIdxTmpFile = defragmentedIndexTmpFile(workDir);
        File defragmentedIdxFile = defragmentedIndexFile(workDir);

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
     * @param workDir Cache group working directory.
     * @param partId Partition index.
     * @throws IgniteException If {@link IOException} occurred.
     *
     * @see DefragmentationFileTreeUtils#defragmentedPartTmpFile(File, int)
     * @see DefragmentationFileTreeUtils#defragmentedPartFile(File, int)
     */
    public static void renameTempPartitionFile(File workDir, int partId) throws IgniteException {
        File defragmentedPartTmpFile = defragmentedPartTmpFile(workDir, partId);
        File defragmentedPartFile = defragmentedPartFile(workDir, partId);

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
     * @param workDir Cache group working directory.
     * @param log Logger to write messages.
     * @throws IgniteException If {@link IOException} occurred.
     *
     * @see DefragmentationFileTreeUtils#defragmentationCompletionMarkerFile(File)
     */
    public static void writeDefragmentationCompletionMarker(
        FileIOFactory ioFactory,
        File workDir,
        IgniteLogger log
    ) throws IgniteException {
        File completionMarker = defragmentationCompletionMarkerFile(workDir);

        try (FileIO io = ioFactory.create(completionMarker, CREATE_NEW, WRITE)) {
            io.force(true);
        }
        catch (IOException e) {
            throw new IgniteException(e);
        }
    }
}

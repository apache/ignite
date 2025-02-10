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
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.FILE_SUFFIX;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.INDEX_FILE_NAME;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.INDEX_FILE_PREFIX;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.PART_FILE_TEMPLATE;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.TMP_SUFFIX;
import static org.apache.ignite.internal.processors.cache.persistence.filename.NodeFileTree.PART_FILE_PREFIX;

/**
 * Everything related to file management during defragmentation process.
 */
public class DefragmentationFileUtils {
    /** Prefix for link mapping files. */
    private static final String DFRG_LINK_MAPPING_FILE_PREFIX = PART_FILE_PREFIX + "map-";

    /** Link mapping file template. */
    private static final String DFRG_LINK_MAPPING_FILE_TEMPLATE = DFRG_LINK_MAPPING_FILE_PREFIX + "%d" + FILE_SUFFIX;

    /** Defragmentation complation marker file name. */
    private static final String DFRG_COMPLETION_MARKER_FILE_NAME = "dfrg-completion-marker";

    /** Name of defragmentated index partition file. */
    private static final String DFRG_INDEX_FILE_NAME = INDEX_FILE_PREFIX + "-dfrg" + FILE_SUFFIX;

    /** Name of defragmentated index partition temporary file. */
    private static final String DFRG_INDEX_TMP_FILE_NAME = DFRG_INDEX_FILE_NAME + TMP_SUFFIX;

    /** Prefix for defragmented partition files. */
    private static final String DFRG_PARTITION_FILE_PREFIX = PART_FILE_PREFIX + "dfrg-";

    /** Defragmented partition file template. */
    private static final String DFRG_PARTITION_FILE_TEMPLATE = DFRG_PARTITION_FILE_PREFIX + "%d" + FILE_SUFFIX;

    /** Defragmented partition temp file template. */
    private static final String DFRG_PARTITION_TMP_FILE_TEMPLATE = DFRG_PARTITION_FILE_TEMPLATE + TMP_SUFFIX;

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
            String fileName = file.getName();

            if (
                fileName.startsWith(DFRG_PARTITION_FILE_PREFIX)
                    || fileName.startsWith(DFRG_INDEX_FILE_NAME)
                    || fileName.startsWith(DFRG_LINK_MAPPING_FILE_PREFIX)
            )
                U.delete(file);
        }
    }

    /**
     * Checks whether cache group defragmentation completed or not. Completes it if all that's left is renaming.
     *
     * @param workDir Cache group working directory.
     * @param grpId Cache group Id of cache group belonging to the given working directory.
     * @param log Logger to write messages.
     * @return {@code true} if given cache group is already defragmented.
     * @throws IgniteException If {@link IOException} occurred.
     *
     * @see DefragmentationFileUtils#defragmentationCompletionMarkerFile(File)
     */
    public static boolean skipAlreadyDefragmentedCacheGroup(File workDir, int grpId, IgniteLogger log) throws IgniteException {
        File completionMarkerFile = defragmentationCompletionMarkerFile(workDir);

        if (completionMarkerFile.exists()) {
            if (log.isInfoEnabled()) {
                log.info(S.toString(
                    "Skipping already defragmented page group",
                    "grpId", grpId, false,
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
     * @see DefragmentationFileUtils#defragmentedPartTmpFile(File, int)
     * @see DefragmentationFileUtils#defragmentedPartFile(File, int)
     * @see DefragmentationFileUtils#defragmentedPartMappingFile(File, int)
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
            for (File mappingFile : workDir.listFiles((dir, name) -> name.startsWith(DFRG_LINK_MAPPING_FILE_PREFIX)))
                Files.delete(mappingFile.toPath());

            for (File partFile : workDir.listFiles((dir, name) -> name.startsWith(DFRG_PARTITION_FILE_PREFIX))) {
                int partId = extractPartId(partFile.getName());

                File oldPartFile = new File(workDir, String.format(PART_FILE_TEMPLATE, partId));

                Files.move(partFile.toPath(), oldPartFile.toPath(), ATOMIC_MOVE, REPLACE_EXISTING);
            }

            File idxFile = new File(workDir, DFRG_INDEX_FILE_NAME);

            if (idxFile.exists()) {
                File oldIdxFile = new File(workDir, INDEX_FILE_NAME);

                Files.move(idxFile.toPath(), oldIdxFile.toPath(), ATOMIC_MOVE, REPLACE_EXISTING);
            }
        }
        catch (IOException e) {
            throw new IgniteException(e);
        }
    }

    /**
     * Extracts partition number from file names like {@code part-dfrg-%d.bin}.
     *
     * @param dfrgPartFileName Defragmented partition file name.
     * @return Partition index.
     *
     * @see DefragmentationFileUtils#defragmentedPartFile(File, int)
     */
    private static int extractPartId(String dfrgPartFileName) {
        assert dfrgPartFileName.startsWith(DFRG_PARTITION_FILE_PREFIX) : dfrgPartFileName;
        assert dfrgPartFileName.endsWith(FILE_SUFFIX) : dfrgPartFileName;

        String partIdStr = dfrgPartFileName.substring(
            DFRG_PARTITION_FILE_PREFIX.length(),
            dfrgPartFileName.length() - FILE_SUFFIX.length()
        );

        return Integer.parseInt(partIdStr);
    }

    /**
     * Return file named {@code index-dfrg.bin.tmp} in given folder. It will be used for storing defragmented index
     * partition during the process.
     *
     * @param workDir Cache group working directory.
     * @return File.
     *
     * @see DefragmentationFileUtils#defragmentedIndexFile(File)
     */
    public static File defragmentedIndexTmpFile(File workDir) {
        return new File(workDir, DFRG_INDEX_TMP_FILE_NAME);
    }

    /**
     * Return file named {@code index-dfrg.bin} in given folder. It will be used for storing defragmented index
     * partition when the process is over.
     *
     * @param workDir Cache group working directory.
     * @return File.
     *
     * @see DefragmentationFileUtils#defragmentedIndexTmpFile(File)
     */
    public static File defragmentedIndexFile(File workDir) {
        return new File(workDir, DFRG_INDEX_FILE_NAME);
    }

    /**
     * Rename temporary index defragmentation file to a finalized one.
     *
     * @param workDir Cache group working directory.
     * @throws IgniteException If {@link IOException} occurred.
     *
     * @see DefragmentationFileUtils#defragmentedIndexTmpFile(File)
     * @see DefragmentationFileUtils#defragmentedIndexFile(File)
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
     * Return file named {@code part-dfrg-%d.bin.tmp} in given folder. It will be used for storing defragmented data
     * partition during the process.
     *
     * @param workDir Cache group working directory.
     * @param partId Partition index, will be substituted into file name.
     * @return File.
     *
     * @see DefragmentationFileUtils#defragmentedPartFile(File, int)
     */
    public static File defragmentedPartTmpFile(File workDir, int partId) {
        return new File(workDir, String.format(DFRG_PARTITION_TMP_FILE_TEMPLATE, partId));
    }

    /**
     * Return file named {@code part-dfrg-%d.bin} in given folder. It will be used for storing defragmented data
     * partition when the process is over.
     *
     * @param workDir Cache group working directory.
     * @param partId Partition index, will be substituted into file name.
     * @return File.
     *
     * @see DefragmentationFileUtils#defragmentedPartTmpFile(File, int)
     */
    public static File defragmentedPartFile(File workDir, int partId) {
        return new File(workDir, String.format(DFRG_PARTITION_FILE_TEMPLATE, partId));
    }

    /**
     * Rename temporary partition defragmentation file to a finalized one.
     *
     * @param workDir Cache group working directory.
     * @param partId Partition index.
     * @throws IgniteException If {@link IOException} occurred.
     *
     * @see DefragmentationFileUtils#defragmentedPartTmpFile(File, int)
     * @see DefragmentationFileUtils#defragmentedPartFile(File, int)
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
     * Return file named {@code part-map-%d.bin} in given folder. It will be used for storing defragmention links
     * mapping for given partition during and after defragmentation process. No temporary counterpart is required here.
     *
     * @param workDir Cache group working directory.
     * @param partId Partition index, will be substituted into file name.
     * @return File.
     *
     * @see LinkMap
     */
    public static File defragmentedPartMappingFile(File workDir, int partId) {
        return new File(workDir, String.format(DFRG_LINK_MAPPING_FILE_TEMPLATE, partId));
    }

    /**
     * Return defragmentation completion marker file. This file can only be created when all partitions and index are
     * defragmented and renamed from their original {@code *.tmp} versions. Presence of this file signals that no data
     * will be lost if original partitions are deleted and batch rename process can be safely initiated.
     *
     * @param workDir Cache group working directory.
     * @return File.
     *
     * @see DefragmentationFileUtils#writeDefragmentationCompletionMarker(FileIOFactory, File, IgniteLogger)
     * @see DefragmentationFileUtils#batchRenameDefragmentedCacheGroupPartitions(File, IgniteLogger)
     */
    public static File defragmentationCompletionMarkerFile(File workDir) {
        return new File(workDir, DFRG_COMPLETION_MARKER_FILE_NAME);
    }

    /**
     * Creates empty completion marker file in given directory.
     *
     * @param ioFactory File IO factory.
     * @param workDir Cache group working directory.
     * @param log Logger to write messages.
     * @throws IgniteException If {@link IOException} occurred.
     *
     * @see DefragmentationFileUtils#defragmentationCompletionMarkerFile(File)
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

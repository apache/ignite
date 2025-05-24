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
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.cache.persistence.defragmentation.LinkMap;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.persistence.filename.NodeFileTree.FILE_SUFFIX;
import static org.apache.ignite.internal.processors.cache.persistence.filename.NodeFileTree.INDEX_FILE_PREFIX;
import static org.apache.ignite.internal.processors.cache.persistence.filename.NodeFileTree.PART_FILE_PREFIX;
import static org.apache.ignite.internal.processors.cache.persistence.filename.NodeFileTree.TMP_SUFFIX;

public class DefragmentationFileTree {
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

    private final NodeFileTree ft;

    public DefragmentationFileTree(NodeFileTree ft) {
        this.ft = ft;
    }

    /**
     * @param f File to check.
     * @return {@code True} if file is defragmentation patition.
     */
    public static boolean defragmentationPartitionFile(File f) {
        return f.getName().startsWith(DFRG_PARTITION_FILE_PREFIX);
    }

    /**
     * @param f File to check.
     * @return {@code True} if file is defragmentation index.
     */
    public static boolean defragmentationIndexFile(File f) {
        return f.getName().startsWith(DFRG_INDEX_FILE_NAME);
    }

    /**
     * @param f File to check.
     * @return {@code True} if file is defragmentation link mapping file.
     */
    public static boolean defragmentationLinkMappingFile(File f) {
        return f.getName().startsWith(DFRG_LINK_MAPPING_FILE_PREFIX);
    }

    /**
     * Extracts partition number from file names like {@code part-dfrg-%d.bin}.
     *
     * @param part Defragmented partition file name.
     * @return Partition index.
     *
     * @see #defragmentedPartFile(File, int)
     */
    public static int partId(File part) {
        String dfrgPartFileName = part.getName();
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
     * @param ft Node file tree.
     * @param ccfg Cache configuration.
     * @return File.
     *
     * @see #defragmentedIndexFile(File)
     */
    public static File defragmentedIndexTmpFile(NodeFileTree ft, CacheConfiguration<?, ?> ccfg) {
        return new File(ft.cacheStorage(ccfg), DFRG_INDEX_TMP_FILE_NAME);
    }

    /**
     * Return file named {@code index-dfrg.bin} in given folder. It will be used for storing defragmented index
     * partition when the process is over.
     *
     * @param workDir Cache group working directory.
     * @return File.
     *
     * @see #defragmentedIndexTmpFile(NodeFileTree, CacheConfiguration)
     */
    public static File defragmentedIndexFile(File workDir) {
        return new File(workDir, DFRG_INDEX_FILE_NAME);
    }

    /**
     * Return file named {@code part-dfrg-%d.bin.tmp} in given folder. It will be used for storing defragmented data
     * partition during the process.
     *
     * @param workDir Cache group working directory.
     * @param partId Partition index, will be substituted into file name.
     * @return File.
     *
     * @see #defragmentedPartFile(File, int)
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
     * @see #defragmentedPartTmpFile(File, int)
     */
    public static File defragmentedPartFile(File workDir, int partId) {
        return new File(workDir, String.format(DFRG_PARTITION_FILE_TEMPLATE, partId));
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
     * @return File.
     */
    public File defragmentationCompletionMarkerFile(boolean metaStore, @Nullable CacheConfiguration<?, ?> ccfg) {
        return new File(cacheStorage(metaStore, ccfg), DFRG_COMPLETION_MARKER_FILE_NAME);
    }

    /**
     * @param ft
     * @param metaStore
     * @param ccfg
     * @return
     */
    public File cacheStorage(boolean metaStore, @Nullable CacheConfiguration<?, ?> ccfg) {
        return metaStore ? ft.metaStorage() : ft.cacheStorage(ccfg);
    }
}

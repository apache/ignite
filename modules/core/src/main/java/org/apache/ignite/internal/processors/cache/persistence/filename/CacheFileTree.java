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
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.cache.persistence.defragmentation.DefragmentationFileUtils;
import org.apache.ignite.internal.processors.cache.persistence.defragmentation.LinkMap;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.MetaStorage;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.persistence.filename.NodeFileTree.FILE_SUFFIX;
import static org.apache.ignite.internal.processors.cache.persistence.filename.NodeFileTree.INDEX_FILE_PREFIX;
import static org.apache.ignite.internal.processors.cache.persistence.filename.NodeFileTree.PART_FILE_PREFIX;
import static org.apache.ignite.internal.processors.cache.persistence.filename.NodeFileTree.TMP_SUFFIX;

/**
 * Class to be used in common code: for metastorage or regular cache.
 */
public class CacheFileTree {
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

    /** Node file tree. */
    private final NodeFileTree ft;

    /** Cache storage. */
    private final File storage;

    /** {@code True} if tree for metastore, {@code false} otherwise. */
    private final boolean metastore;

    /** Cache configuration. {@code Null} only if {@code metastore == true}. */
    private final @Nullable CacheConfiguration<?, ?> ccfg;

    /** Cache configuration. {@code Null} only if {@code metastore == true}. */
    private final int grpId;

    /**
     * @param ft Node file tree.
     * @param metastore {@code True} if tree for metastore, {@code false} otherwise.
     * @param ccfg Cache configuration. {@code Null} only if {@code metastore == true}.
     */
    CacheFileTree(NodeFileTree ft, boolean metastore, @Nullable CacheConfiguration<?, ?> ccfg) {
        assert ccfg != null || metastore;

        this.ft = ft;
        this.metastore = metastore;
        this.storage = metastore ? ft.metaStorage() : ft.cacheStorage(ccfg);
        this.ccfg = ccfg;
        this.grpId = metastore ? MetaStorage.METASTORAGE_CACHE_ID : CU.cacheGroupId(ccfg);
    }

    /**
     * @return Storage for cache.
     */
    public File storage() {
        return storage;
    }

    /**
     * @return Cache configuration.
     */
    @Nullable public CacheConfiguration<?, ?> config() {
        return ccfg;
    }

    /**
     * @return {@code True} if tree for metastore, {@code false} otherwise.
     */
    public boolean metastore() {
        return metastore;
    }

    /**
     * @param part Partition id.
     * @return Partition file.
     */
    public File partitionFile(int part) {
        return metastore
            ? ft.metaStoragePartition(part)
            : ft.partitionFile(ccfg, part);
    }

    /**
     * @return Cache name.
     */
    public String name() {
        return metastore ? MetaStorage.METASTORAGE_CACHE_NAME : ccfg.getName();
    }

    /**
     * @return Gropu id.
     */
    public int groupId() {
        return grpId;
    }

    /**
     * Return file named {@code index-dfrg.bin.tmp} in given folder. It will be used for storing defragmented index
     * partition during the process.
     *
     * @return File.
     *
     * @see CacheFileTree#defragmentedIndexFile()
     */
    public File defragmentedIndexTmpFile() {
        return new File(storage(), DFRG_INDEX_TMP_FILE_NAME);
    }

    /**
     * Return file named {@code index-dfrg.bin} in given folder. It will be used for storing defragmented index
     * partition when the process is over.
     *
     * @return File.
     *
     * @see #defragmentedIndexTmpFile()
     */
    public File defragmentedIndexFile() {
        return new File(storage(), DFRG_INDEX_FILE_NAME);
    }

    /**
     * Return file named {@code part-dfrg-%d.bin.tmp} in given folder. It will be used for storing defragmented data
     * partition during the process.
     *
     * @param partId Partition index, will be substituted into file name.
     * @return File.
     *
     * @see #defragmentedPartFile(int)
     */
    public File defragmentedPartTmpFile(int partId) {
        return new File(storage(), String.format(DFRG_PARTITION_TMP_FILE_TEMPLATE, partId));
    }

    /**
     * Return file named {@code part-dfrg-%d.bin} in given folder. It will be used for storing defragmented data
     * partition when the process is over.
     *
     * @param partId Partition index, will be substituted into file name.
     * @return File.
     *
     * @see #defragmentedPartTmpFile(int)
     */
    public File defragmentedPartFile(int partId) {
        return new File(storage(), String.format(DFRG_PARTITION_FILE_TEMPLATE, partId));
    }

    /**
     * Return file named {@code part-map-%d.bin} in given folder. It will be used for storing defragmention links
     * mapping for given partition during and after defragmentation process. No temporary counterpart is required here.
     *
     * @param partId Partition index, will be substituted into file name.
     * @return File.
     *
     * @see LinkMap
     */
    public File defragmentedPartMappingFile(int partId) {
        return new File(storage(), String.format(DFRG_LINK_MAPPING_FILE_TEMPLATE, partId));
    }

    /**
     * Return defragmentation completion marker file. This file can only be created when all partitions and index are
     * defragmented and renamed from their original {@code *.tmp} versions. Presence of this file signals that no data
     * will be lost if original partitions are deleted and batch rename process can be safely initiated.
     *
     * @return File.
     *
     * @see DefragmentationFileUtils#writeDefragmentationCompletionMarker(FileIOFactory, CacheFileTree, IgniteLogger)
     * @see DefragmentationFileUtils#batchRenameDefragmentedCacheGroupPartitions(CacheFileTree)
     */
    public File defragmentationCompletionMarkerFile() {
        return new File(storage(), DFRG_COMPLETION_MARKER_FILE_NAME);
    }

    /**
     * @param fileName File name.
     * @return {@code True} if name is defragment partition file.
     */
    public static boolean isDefragmentPartition(String fileName) {
        return fileName.startsWith(DFRG_PARTITION_FILE_PREFIX);
    }

    /**
     * @param fileName File name.
     * @return {@code True} if name is defragment index file.
     */
    public static boolean isDefragmentIndex(String fileName) {
        return fileName.startsWith(DFRG_INDEX_FILE_NAME);
    }

    /**
     * @param fileName File name.
     * @return {@code True} if name is defragment link mapping file.
     */
    public static boolean isDefragmentLinkMapping(String fileName) {
        return fileName.startsWith(DFRG_LINK_MAPPING_FILE_PREFIX);
    }

    /**
     * Extracts partition number from file names like {@code part-dfrg-%d.bin}.
     *
     * @param dfrgPartFileName Defragmented partition file name.
     * @return Partition index.
     *
     * @see #defragmentedPartFile(int)
     */
    public static int extractPartId(String dfrgPartFileName) {
        assert dfrgPartFileName.startsWith(DFRG_PARTITION_FILE_PREFIX) : dfrgPartFileName;
        assert dfrgPartFileName.endsWith(FILE_SUFFIX) : dfrgPartFileName;

        String partIdStr = dfrgPartFileName.substring(
            DFRG_PARTITION_FILE_PREFIX.length(),
            dfrgPartFileName.length() - FILE_SUFFIX.length()
        );

        return Integer.parseInt(partIdStr);
    }
}

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
package org.apache.ignite.development.utils.indexreader;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.development.utils.ProgressPrinter;
import org.apache.ignite.development.utils.StringBuilderOutputStream;
import org.apache.ignite.development.utils.arguments.CLIArgument;
import org.apache.ignite.development.utils.arguments.CLIArgumentParser;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.processors.cache.persistence.AllocatedPageTracker;
import org.apache.ignite.internal.processors.cache.persistence.IndexStorageImpl;
import org.apache.ignite.internal.processors.cache.persistence.StorageException;
import org.apache.ignite.internal.processors.cache.persistence.file.AsyncFileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStore;
import org.apache.ignite.internal.processors.cache.persistence.file.FileVersionCheckingFactory;
import org.apache.ignite.internal.processors.cache.persistence.freelist.io.PagesListMetaIO;
import org.apache.ignite.internal.processors.cache.persistence.freelist.io.PagesListNodeIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.AbstractDataPageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusInnerIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusLeafIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusMetaIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.DataPagePayload;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageMetaIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PagePartitionMetaIOV2;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.TrackingPageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.util.PageHandler;
import org.apache.ignite.internal.processors.cache.persistence.wal.crc.FastCrc;
import org.apache.ignite.internal.processors.cache.persistence.wal.crc.IgniteDataIntegrityViolationException;
import org.apache.ignite.internal.processors.cache.tree.AbstractDataLeafIO;
import org.apache.ignite.internal.processors.cache.tree.PendingRowIO;
import org.apache.ignite.internal.processors.cache.tree.RowLinkIO;
import org.apache.ignite.internal.processors.query.h2.database.io.H2ExtrasInnerIO;
import org.apache.ignite.internal.processors.query.h2.database.io.H2ExtrasLeafIO;
import org.apache.ignite.internal.processors.query.h2.database.io.H2InnerIO;
import org.apache.ignite.internal.processors.query.h2.database.io.H2LeafIO;
import org.apache.ignite.internal.processors.query.h2.database.io.H2RowLinkIO;
import org.apache.ignite.internal.util.GridLongList;
import org.apache.ignite.internal.util.GridStringBuilder;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.lang.GridClosure3;
import org.apache.ignite.internal.util.lang.IgnitePair;
import org.apache.ignite.lang.IgniteBiTuple;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.WRITE;
import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.development.utils.arguments.CLIArgument.mandatoryArg;
import static org.apache.ignite.development.utils.arguments.CLIArgument.optionalArg;
import static org.apache.ignite.development.utils.indexreader.IgniteIndexReader.Args.CHECK_PARTS;
import static org.apache.ignite.development.utils.indexreader.IgniteIndexReader.Args.DEST;
import static org.apache.ignite.development.utils.indexreader.IgniteIndexReader.Args.DEST_FILE;
import static org.apache.ignite.development.utils.indexreader.IgniteIndexReader.Args.DIR;
import static org.apache.ignite.development.utils.indexreader.IgniteIndexReader.Args.FILE_MASK;
import static org.apache.ignite.development.utils.indexreader.IgniteIndexReader.Args.INDEXES;
import static org.apache.ignite.development.utils.indexreader.IgniteIndexReader.Args.PAGE_SIZE;
import static org.apache.ignite.development.utils.indexreader.IgniteIndexReader.Args.PAGE_STORE_VER;
import static org.apache.ignite.development.utils.indexreader.IgniteIndexReader.Args.PART_CNT;
import static org.apache.ignite.development.utils.indexreader.IgniteIndexReader.Args.TRANSFORM;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.FLAG_DATA;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.FLAG_IDX;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.INDEX_PARTITION;
import static org.apache.ignite.internal.pagemem.PageIdUtils.flag;
import static org.apache.ignite.internal.pagemem.PageIdUtils.itemId;
import static org.apache.ignite.internal.pagemem.PageIdUtils.pageId;
import static org.apache.ignite.internal.pagemem.PageIdUtils.pageIndex;
import static org.apache.ignite.internal.pagemem.PageIdUtils.partId;
import static org.apache.ignite.internal.processors.cache.persistence.AllocatedPageTracker.NO_OP;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.INDEX_FILE_NAME;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.PART_FILE_TEMPLATE;
import static org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO.getType;
import static org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO.getVersion;
import static org.apache.ignite.internal.util.GridUnsafe.allocateBuffer;
import static org.apache.ignite.internal.util.GridUnsafe.bufferAddress;
import static org.apache.ignite.internal.util.GridUnsafe.freeBuffer;

/**
 * Offline reader for index files.
 */
public class IgniteIndexReader implements AutoCloseable {
    /** */
    private static final String META_TREE_NAME = "MetaTree";

    /** */
    public static final String RECURSIVE_TRAVERSE_NAME = "<RECURSIVE> ";

    /** */
    public static final String HORIZONTAL_SCAN_NAME = "<HORIZONTAL> ";

    /** */
    private static final String PAGE_LISTS_PREFIX = "<PAGE_LIST> ";

    /** */
    public static final String ERROR_PREFIX = "<ERROR> ";

    /** */
    private static final Pattern CACHE_TYPE_ID_SEACH_PATTERN =
        Pattern.compile("(?<id>[-0-9]{1,15})_(?<typeId>[-0-9]{1,15})_.*");

    /** */
    private static final Pattern CACHE_ID_SEACH_PATTERN =
        Pattern.compile("(?<id>[-0-9]{1,15})_.*");

    /** */
    private static final int CHECK_PARTS_MAX_ERRORS_PER_PARTITION = 10;

    /** */
    private static final Map<String, IgnitePair<Integer>> CACHE_TYPE_IDS = new HashMap<>();

    /** */
    static {
        PageIO.registerH2(H2InnerIO.VERSIONS, H2LeafIO.VERSIONS);

        H2ExtrasInnerIO.register();
        H2ExtrasLeafIO.register();
    }

    /** */
    private final int pageSize;

    /** */
    private final int partCnt;

    /** */
    private final File cacheWorkDir;

    /** */
    private final DataStorageConfiguration dsCfg;

    /** */
    private final FileVersionCheckingFactory storeFactory;

    /** */
    private final AllocatedPageTracker allocationTracker = NO_OP;

    /** */
    private final Set<String> indexes;

    /** */
    private final PrintStream outStream;

    /** */
    private final PrintStream outErrStream;

    /** */
    private final File idxFile;

    /** */
    private final FilePageStore idxStore;

    /** */
    private final FilePageStore[] partStores;

    /** */
    private final boolean checkParts;

    /** */
    private final Set<Integer> missingPartitions = new HashSet<>();

    /** */
    private PageIOProcessor innerPageIOProcessor = new InnerPageIOProcessor();

    /** */
    private PageIOProcessor leafPageIOProcessor = new LeafPageIOProcessor();

    /** */
    private PageIOProcessor metaPageIOProcessor = new MetaPageIOProcessor();

    /** */
    public IgniteIndexReader(
        String cacheWorkDirPath,
        int pageSize,
        int partCnt,
        int filePageStoreVer,
        String[] indexes,
        boolean checkParts,
        OutputStream outputStream
    ) throws IgniteCheckedException {
        this.pageSize = pageSize;
        this.partCnt = partCnt;
        this.dsCfg = new DataStorageConfiguration().setPageSize(pageSize);
        this.cacheWorkDir = new File(cacheWorkDirPath);
        this.checkParts = checkParts;
        this.indexes = indexes == null ? null : new HashSet<>(asList(indexes));
        this.storeFactory = storeFactory(filePageStoreVer);
        this.outStream = outputStream == null ? System.out : new PrintStream(outputStream);
        this.outErrStream = outputStream == null ? System.out : outStream;

        idxFile = getFile(INDEX_PARTITION);

        if (idxFile == null)
            throw new IgniteException(INDEX_FILE_NAME + " file not found");

        idxStore = storeFactory.createPageStore(FLAG_IDX, idxFile, allocationTracker);

        partStores = new FilePageStore[partCnt];

        for (int i = 0; i < partCnt; i++) {
            final File file = getFile(i);

            // Some of array members will be null if node doesn't have all partition files locally.
            if (file != null)
                partStores[i] = storeFactory.createPageStore(FLAG_DATA, file, allocationTracker);
        }
    }

    /** */
    public IgniteIndexReader(String cacheWorkDirPath, int pageSize, int filePageStoreVer, OutputStream outputStream) {
        this.pageSize = pageSize;
        this.partCnt = 0;
        this.dsCfg = new DataStorageConfiguration().setPageSize(pageSize);
        this.cacheWorkDir = new File(cacheWorkDirPath);
        this.checkParts = false;
        this.indexes = null;
        this.storeFactory = storeFactory(filePageStoreVer);
        this.outStream = outputStream == null ? System.out : new PrintStream(outputStream);
        this.outErrStream = outputStream == null ? System.out : outStream;
        this.idxFile = null;
        this.idxStore = null;
        this.partStores = null;
    }

    /** */
    private void print(String s) {
        outStream.println(s);
    }

    /** */
    private void printErr(String s) {
        outErrStream.println(ERROR_PREFIX + s);
    }

    /** */
    private void printErrors(
        String prefix,
        String caption,
        String alternativeCaption,
        String elementFormatPtrn,
        boolean printTrace,
        Map<?, ? extends List<? extends Throwable>> errors
    ) {
        if (errors.isEmpty()) {
            print(prefix + alternativeCaption);

            return;
        }

        if (caption != null)
            outErrStream.println(prefix + ERROR_PREFIX + caption);

        errors.forEach((k, v) -> {
            outErrStream.println(prefix + ERROR_PREFIX + String.format(elementFormatPtrn, k.toString()));

            v.forEach(e -> {
                if (printTrace)
                    printStackTrace(e);
                else
                    printErr(e.getMessage());
            });
        });
    }

    /** */
    private void printPageStat(String prefix, String caption, Map<Class, Long> stat) {
        if (caption != null)
            print(prefix + caption + (stat.isEmpty() ? " empty" : ""));

        stat.forEach((cls, cnt) -> print(prefix + cls.getSimpleName() + ": " + cnt));
    }

    /** */
    private void printStackTrace(Throwable e) {
        OutputStream os = new StringBuilderOutputStream();

        e.printStackTrace(new PrintStream(os));

        outErrStream.println(os.toString());
    }

    /** */
    private static long normalizePageId(long pageId) {
        return pageId(partId(pageId), flag(pageId), pageIndex(pageId));
    }

    /** */
    private File getFile(int partId) {
        File file = new File(
                cacheWorkDir,
                partId == INDEX_PARTITION ? INDEX_FILE_NAME : String.format(PART_FILE_TEMPLATE, partId)
        );

        if (!file.exists())
            return null;
        else if (partId == INDEX_PARTITION)
            print("Analyzing file: " + file.getPath());

        return file;
    }

    /** */
    private FileVersionCheckingFactory storeFactory(int filePageStoreVer) {
        return new FileVersionCheckingFactory(new AsyncFileIOFactory(), new AsyncFileIOFactory(), dsCfg) {
            /** {@inheritDoc} */
            @Override public int latestVersion() {
                return filePageStoreVer;
            }
        };
    }

    /** */
    private void readPage(FilePageStore store, long pageId, ByteBuffer buf) throws IgniteCheckedException {
        try {
            store.read(pageId, buf, false);
        }
        catch (IgniteDataIntegrityViolationException e) {
            // Replacing exception due to security reasons, as IgniteDataIntegrityViolationException prints page content.
            throw new IgniteException("Failed to read page, id=" + pageId + ", file=" + store.getFileAbsolutePath());
        }
    }

    /**
     * Read index file.
     */
    public void readIdx() {
        long partPageStoresNum = Arrays.stream(partStores)
            .filter(Objects::nonNull)
            .count();

        print("Partitions files num: " + partPageStoresNum);

        Map<Class, Long> pageClasses = new HashMap<>();

        long pagesNum = idxStore == null ? 0 : (idxFile.length() - idxStore.headerSize()) / pageSize;

        print("Going to check " + pagesNum + " pages.");

        Set<Long> pageIds = new HashSet<>();

        AtomicReference<Map<String, TreeTraversalInfo>> treeInfo = new AtomicReference<>();

        AtomicReference<Map<String, TreeTraversalInfo>> horizontalScans = new AtomicReference<>();

        AtomicReference<PageListsInfo> pageListsInfo = new AtomicReference<>();

        List<Throwable> errors;

        try {
            Set<Class> metaPageClasses = new HashSet<>(asList(PageMetaIO.class, PagesListMetaIO.class));

            Map<Class, Long> idxMetaPages = findPages(INDEX_PARTITION, FLAG_IDX, idxStore, metaPageClasses);

            Long pageMetaPageId = idxMetaPages.get(PageMetaIO.class);

            // Traversing trees.
            if (pageMetaPageId != null) {
                doWithBuffer((buf, addr) -> {
                    readPage(idxStore, pageMetaPageId, buf);

                    PageMetaIO pageMetaIO = PageIO.getPageIO(addr);

                    long metaTreeRootId = normalizePageId(pageMetaIO.getTreeRoot(addr));

                    treeInfo.set(traverseAllTrees("Index trees traversal", metaTreeRootId, CountOnlyStorage::new, this::traverseTree));

                    treeInfo.get().forEach((name, info) -> {
                        pageIds.addAll(info.innerPageIds);

                        pageIds.add(info.rootPageId);
                    });

                    Supplier<ItemStorage> itemStorageFactory = checkParts ? LinkStorage::new : CountOnlyStorage::new;

                    horizontalScans.set(traverseAllTrees("Scan index trees horizontally", metaTreeRootId, itemStorageFactory, this::horizontalTreeScan));

                    return null;
                });
            }

            Long pageListMetaPageId = idxMetaPages.get(PagesListMetaIO.class);

            // Scanning page reuse lists.
            if (pageListMetaPageId != null)
                pageListsInfo.set(getPageListsInfo(pageListMetaPageId));

            ProgressPrinter progressPrinter = new ProgressPrinter(System.out, "Reading pages sequentially", pagesNum);

            // Scan all pages in file.
            errors = scanFileStore(INDEX_PARTITION, FLAG_IDX, idxStore, (pageId, addr, io) -> {
                progressPrinter.printProgress();

                pageClasses.compute(io.getClass(), (k, v) -> v == null ? 1 : v + 1);

                if (!(io instanceof PageMetaIO || io instanceof PagesListMetaIO)) {
                    if (indexes == null) {
                        if ((io instanceof BPlusMetaIO || io instanceof BPlusInnerIO)
                                && !pageIds.contains(pageId)
                                && pageListsInfo.get() != null
                                && !pageListsInfo.get().allPages.contains(pageId)) {
                            throw new IgniteException(
                                    "Possibly orphan " + io.getClass().getSimpleName() + " page, pageId=" + pageId
                            );
                        }
                    }
                }

                return true;
            });
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(INDEX_FILE_NAME + " scan problem", e);
        }

        if (treeInfo.get() == null)
            printErr("No tree meta info found.");
        else {
            printTraversalResults(RECURSIVE_TRAVERSE_NAME, treeInfo.get());

            printTraversalResults(HORIZONTAL_SCAN_NAME, horizontalScans.get());
        }

        compareTraversals(treeInfo.get(), horizontalScans.get());

        if (pageListsInfo.get() == null)
            printErr("No page lists meta info found.");
        else
            printPagesListsInfo(pageListsInfo.get());

        printPageStat("", "\n---These pages types were encountered during sequential scan:", pageClasses);

        if (!errors.isEmpty()) {
            printErr("---");
            printErr("Errors:");

            errors.forEach(this::printStackTrace);
        }

        print("---");
        print("Total pages encountered during sequential scan: " + pageClasses.values().stream().mapToLong(a -> a).sum());
        print("Total errors occurred during sequential scan: " + errors.size());

        if (indexes != null)
            print("Orphan pages were not reported due to --indexes filter.");

        print("Note that some pages can be occupied by meta info, tracking info, etc., so total page count can differ " +
            "from count of pages found in index trees and page lists.");

        if (checkParts) {
            Map<Integer, List<Throwable>> checkPartsErrors = checkParts(horizontalScans.get());

            print("");

            printErrors("",
                "Partitions check:",
                "Partitions check detected no errors.",
                "Errors detected in partition, partId=%s",
                false,
                checkPartsErrors
            );

            print("\nPartition check finished, total errors: " +
                checkPartsErrors.values().stream().mapToInt(List::size).sum() + ", total problem partitions: " +
                checkPartsErrors.size()
            );
        }
    }

    /**
     * Allocates buffer and does some work in closure, then frees the buffer.
     *
     * @param c Closure.
     * @param <T> Result type.
     * @return Result of closure.
     * @throws IgniteCheckedException If failed.
     */
    private <T> T doWithBuffer(BufferClosure<T> c) throws IgniteCheckedException {
        ByteBuffer buf = allocateBuffer(pageSize);

        try {
            long addr = bufferAddress(buf);

            return c.apply(buf, addr);
        }
        finally {
            freeBuffer(buf);
        }
    }

    /**
     * Scans given file page store and executes closure for each page.
     *
     * @param partId Partition id.
     * @param flag Flag.
     * @param store Page store.
     * @param c Closure that accepts page id, page address, page IO. If it returns false, scan stops.
     * @return List of errors that occured while scanning.
     * @throws IgniteCheckedException If failed.
     */
    private List<Throwable> scanFileStore(int partId, byte flag, FilePageStore store, GridClosure3<Long, Long, PageIO, Boolean> c)
        throws IgniteCheckedException {
        return doWithBuffer((buf, addr) -> {
            List<Throwable> errors = new LinkedList<>();

            long size = new File(store.getFileAbsolutePath()).length();

            long pagesNum = store == null ? 0 : (size - store.headerSize()) / pageSize;

            for (int i = 0; i < pagesNum; i++) {
                buf.rewind();

                try {
                    long pageId = PageIdUtils.pageId(partId, flag, i);

                    readPage(store, pageId, buf);

                    PageIO io = PageIO.getPageIO(addr);

                    if (!c.apply(pageId, addr, io))
                        break;
                }
                catch (Throwable e) {
                    String err = "Exception occurred on step " + i + ": " + e.getMessage();

                    errors.add(new IgniteException(err, e));
                }
            }

            return errors;
        });
    }

    /**
     * Checks partitions, comparing partition indexes (cache data tree) to indexes given in {@code aTreesInfo}.
     *
     * @param aTreesInfo Index trees info to compare cache data tree with.
     * @return Map of errors, bound to partition id.
     */
    private Map<Integer, List<Throwable>> checkParts(Map<String, TreeTraversalInfo> aTreesInfo) {
        System.out.println();

        // Map partId -> errors.
        Map<Integer, List<Throwable>> res = new HashMap<>();

        Map<String, TreeTraversalInfo> treesInfo = new HashMap<>(aTreesInfo);

        treesInfo.remove(META_TREE_NAME);

        ProgressPrinter progressPrinter = new ProgressPrinter(System.out, "Checking partitions", partCnt);

        for (int i = 0; i < partCnt; i++) {
            progressPrinter.printProgress();

            FilePageStore partStore = partStores[i];

            if (partStore == null)
                continue;

            List<Throwable> errors = new LinkedList<>();

            final int partId = i;

            try {
                Map<Class, Long> metaPages = findPages(i, FLAG_DATA, partStore, singleton(PagePartitionMetaIOV2.class));

                long partMetaId = metaPages.get(PagePartitionMetaIOV2.class);

                doWithBuffer((buf, addr) -> {
                    readPage(partStore, partMetaId, buf);

                    PagePartitionMetaIOV2 partMetaIO = PageIO.getPageIO(addr);

                    long cacheDataTreeRoot = partMetaIO.getTreeRoot(addr);

                    TreeTraversalInfo cacheDataTreeInfo =
                        horizontalTreeScan(partStore, cacheDataTreeRoot, "dataTree-" + partId, new ItemsListStorage());

                    for (Object dataTreeItem : cacheDataTreeInfo.itemStorage) {
                        CacheAwareLink cacheAwareLink = (CacheAwareLink)dataTreeItem;

                        for (Map.Entry<String, TreeTraversalInfo> e : treesInfo.entrySet()) {
                            String name = e.getKey();

                            TreeTraversalInfo tree = e.getValue();

                            int cacheId = getCacheId(name);

                            if (cacheId != cacheAwareLink.cacheId)
                                continue; // It's index for other cache, don't check.

                            if (!tree.itemStorage.contains(cacheAwareLink))
                                errors.add(new IgniteException(cacheDataTreeEntryMissingError(name, cacheAwareLink)));
                        }

                        if (errors.size() >= CHECK_PARTS_MAX_ERRORS_PER_PARTITION) {
                            errors.add(new IgniteException("Too many errors (" + CHECK_PARTS_MAX_ERRORS_PER_PARTITION +
                                ") found for partId=" + partId + ", stopping analysis for this partition."));

                            break;
                        }
                    }

                    return null;
                });
            }
            catch (IgniteCheckedException e) {
                errors.add(new IgniteException("Partition check failed, partId=" + i, e));
            }

            if (!errors.isEmpty())
                res.put(partId, errors);
        }

        return res;
    }

    /** */
    private String cacheDataTreeEntryMissingError(String treeName, CacheAwareLink cacheAwareLink) {
        long link = cacheAwareLink.link;

        long pageId = pageId(link);

        int itemId = itemId(link);

        int partId = partId(pageId);

        int pageIdx = pageIndex(pageId);

        return "Entry is missing in index: " + treeName +
            ", cacheId=" + cacheAwareLink.cacheId + ", partId=" + partId +
            ", pageIndex=" + pageIdx + ", itemId=" + itemId + ", link=" + link;
    }

    /**
     * Finds certain pages in file page store. When all pages corresponding given types is found, at least one page
     * for each type, we return the result.
     *
     * @param partId Partition id.
     * @param flag Page store flag.
     * @param store File page store.
     * @param pageTypes Page types to find.
     * @return Map of found pages. First page of this class that was found, is put to this map.
     * @throws IgniteCheckedException If failed.
     */
    private Map<Class, Long> findPages(int partId, byte flag, FilePageStore store, Set<Class> pageTypes)
        throws IgniteCheckedException {
        Map<Class, Long> res = new HashMap<>();

        scanFileStore(partId, flag, store, (pageId, addr, io) -> {
            if (pageTypes.contains(io.getClass())) {
                res.put(io.getClass(), pageId);

                pageTypes.remove(io.getClass());
            }

            return !pageTypes.isEmpty();
        });

        return res;
    }

    /**
     * Compares result of traversals.
     *
     * @param treeInfos Traversal from root to leafs.
     * @param treeScans Traversal using horizontal scan.
     */
    private void compareTraversals(Map<String, TreeTraversalInfo> treeInfos, Map<String, TreeTraversalInfo> treeScans) {
        List<String> errors = new LinkedList<>();

        Set<String> treeIdxNames = new HashSet<>();

        treeInfos.forEach((name, tree) -> {
            treeIdxNames.add(name);

            TreeTraversalInfo scan = treeScans.get(name);

            if (scan == null) {
                errors.add("Tree was detected in " + RECURSIVE_TRAVERSE_NAME + " but absent in  "
                    + HORIZONTAL_SCAN_NAME + ": " + name);

                return;
            }

            if (tree.itemStorage.size() != scan.itemStorage.size())
                errors.add(compareError("items", name, tree.itemStorage.size(), scan.itemStorage.size(), null));

            Set<Class> classesInStat = new HashSet<>();

            tree.ioStat.forEach((cls, cnt) -> {
                classesInStat.add(cls);

                long scanCnt = scan.ioStat.getOrDefault(cls, 0L);

                if (scanCnt != cnt)
                    errors.add(compareError("pages", name, cnt, scanCnt, cls));
            });

            scan.ioStat.forEach((cls, cnt) -> {
                if (classesInStat.contains(cls))
                    // Already checked.
                    return;

                errors.add(compareError("pages", name, 0, cnt, cls));
            });
        });

        treeScans.forEach((name, tree) -> {
            if (!treeIdxNames.contains(name))
                errors.add("Tree was detected in " + HORIZONTAL_SCAN_NAME + " but absent in  "
                    + RECURSIVE_TRAVERSE_NAME + ": " + name);
        });

        errors.forEach(this::printErr);

        print("Comparing traversals detected " + errors.size() + " errors.");
        print("------------------");

    }

    /** */
    private String compareError(String itemName, String idxName, long fromRoot, long scan, Class pageType) {
        return String.format(
            "Different count of %s; index: %s, %s:%s, %s:%s" + (pageType == null ? "" : ", pageType: " + pageType.getName()),
            itemName,
            idxName,
            RECURSIVE_TRAVERSE_NAME,
            fromRoot,
            HORIZONTAL_SCAN_NAME,
            scan
        );
    }

    /**
     * Gets info about page lists.
     *
     * @param metaPageListId Page list meta id.
     * @return Page list info.
     */
    private PageListsInfo getPageListsInfo(long metaPageListId) {
        Map<IgniteBiTuple<Long, Integer>, List<Long>> bucketsData = new HashMap<>();

        Set<Long> allPages = new HashSet<>();

        Map<Class, Long> pageListStat = new HashMap<>();

        Map<Long, List<Throwable>> errors = new HashMap<>();

        try {
            doWithBuffer((buf, addr) -> {
                long nextMetaId = metaPageListId;

                while (nextMetaId != 0) {
                    try {
                        buf.rewind();

                        readPage(idxStore, nextMetaId, buf);

                        PagesListMetaIO io = PageIO.getPageIO(addr);

                        Map<Integer, GridLongList> data = new HashMap<>();

                        io.getBucketsData(addr, data);

                        final long fNextMetaId = nextMetaId;

                        data.forEach((k, v) -> {
                            List<Long> listIds = LongStream.of(v.array()).map(IgniteIndexReader::normalizePageId).boxed().collect(toList());

                            listIds.forEach(listId -> allPages.addAll(getPageList(listId, pageListStat)));

                            bucketsData.put(new IgniteBiTuple<>(fNextMetaId, k), listIds);
                        });

                        nextMetaId = io.getNextMetaPageId(addr);
                    }
                    catch (Exception e) {
                        errors.put(nextMetaId, singletonList(e));

                        nextMetaId = 0;
                    }
                }

                return null;
            });
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }

        return new PageListsInfo(bucketsData, allPages, pageListStat, errors);
    }

    /**
     * Get single page list.
     *
     * @param pageListStartId Id of the start page of the page list.
     * @param pageStat Page types statistics.
     * @return List of page ids.
     */
    private List<Long> getPageList(long pageListStartId, Map<Class, Long> pageStat) {
        List<Long> res = new LinkedList<>();

        long nextNodeId = pageListStartId;

        ByteBuffer nodeBuf = allocateBuffer(pageSize);
        ByteBuffer pageBuf = allocateBuffer(pageSize);

        long nodeAddr = bufferAddress(nodeBuf);
        long pageAddr = bufferAddress(pageBuf);

        try {
            while (nextNodeId != 0) {
                try {
                    nodeBuf.rewind();

                    readPage(idxStore, nextNodeId, nodeBuf);

                    PagesListNodeIO io = PageIO.getPageIO(nodeAddr);

                    for (int i = 0; i < io.getCount(nodeAddr); i++) {
                        pageBuf.rewind();

                        long pageId = normalizePageId(io.getAt(nodeAddr, i));

                        res.add(pageId);

                        readPage(idxStore, pageId, pageBuf);

                        PageIO pageIO = PageIO.getPageIO(pageAddr);

                        pageStat.compute(pageIO.getClass(), (k, v) -> v == null ? 1 : v + 1);
                    }

                    nextNodeId = io.getNextId(nodeAddr);
                }
                catch (IgniteCheckedException e) {
                    throw new IgniteException(e.getMessage(), e);
                }
            }
        }
        finally {
            freeBuffer(nodeBuf);
            freeBuffer(pageBuf);
        }

        return res;
    }

    /**
     * Traverse all trees in file and return their info.
     *
     * @param metaTreeRoot Meta tree root page id.
     * @return Index trees info.
     */
    private Map<String, TreeTraversalInfo> traverseAllTrees(
        String traverseProcCaption,
        long metaTreeRoot,
        Supplier<ItemStorage> itemStorageFactory,
        TraverseProc traverseProc
    ) {
        Map<String, TreeTraversalInfo> treeInfos = new LinkedHashMap<>();

        TreeTraversalInfo metaTreeTraversalInfo =
            traverseProc.traverse(idxStore, metaTreeRoot, META_TREE_NAME, new ItemsListStorage<IndexStorageImpl.IndexItem>());

        treeInfos.put(META_TREE_NAME, metaTreeTraversalInfo);

        ProgressPrinter progressPrinter =
            new ProgressPrinter(System.out, traverseProcCaption, metaTreeTraversalInfo.itemStorage.size());

        metaTreeTraversalInfo.itemStorage.forEach(item -> {
            progressPrinter.printProgress();

            IndexStorageImpl.IndexItem idxItem = (IndexStorageImpl.IndexItem)item;

            if (indexes != null && !indexes.contains(idxItem.nameString()))
                return;

            TreeTraversalInfo treeTraversalInfo =
                traverseProc.traverse(idxStore, normalizePageId(idxItem.pageId()), idxItem.nameString(), itemStorageFactory.get());

            treeInfos.put(idxItem.toString(), treeTraversalInfo);
        });

        return treeInfos;
    }

    /**
     * Prints traversal info.
     *
     * @param treeInfos Tree traversal info.
     */
    private void printTraversalResults(String prefix, Map<String, TreeTraversalInfo> treeInfos) {
        print("\n" + prefix + "Tree traversal results");

        Map<Class, Long> totalStat = new HashMap<>();

        AtomicInteger totalErr = new AtomicInteger(0);

        // Map (cacheId, typeId) -> (map idxName -> size))
        Map<IgnitePair<Integer>, Map<String, Long>> cacheIdxSizes = new HashMap<>();

        treeInfos.forEach((idxName, validationInfo) -> {
            print(prefix + "-----");
            print(prefix + "Index tree: " + idxName);
            print(prefix + "-- Page stat:");

            validationInfo.ioStat.forEach((cls, cnt) -> {
                print(prefix + cls.getSimpleName() + ": " + cnt);

                totalStat.compute(cls, (k, v) -> v == null ? 1 : v + 1);
            });

            print(prefix + "-- Count of items found in leaf pages: " + validationInfo.itemStorage.size());

            printErrors(
                prefix,
                "Errors:",
                "No errors occurred while traversing.",
                "Page id=%s, exceptions:",
                true,
                validationInfo.errors
            );

            totalErr.addAndGet(validationInfo.errors.size());

            cacheIdxSizes.computeIfAbsent(getCacheAndTypeId(idxName), k -> new HashMap<>())
                .put(idxName, validationInfo.itemStorage.size());
        });

        print(prefix + "---");

        printPageStat(prefix, "Total page stat collected during trees traversal:", totalStat);

        print("");

        AtomicBoolean sizeConsistencyErrorsFound = new AtomicBoolean(false);

        cacheIdxSizes.forEach((cacheTypeId, idxSizes) -> {
            if (idxSizes.values().stream().distinct().count() > 1) {
                sizeConsistencyErrorsFound.set(true);

                totalErr.incrementAndGet();

                printErr("Index size inconsistency: cacheId=" +cacheTypeId.get1() + ", typeId=" + cacheTypeId.get2());

                idxSizes.forEach((name, size) -> printErr("     Index name: " + name + ", size=" + size));
            }
        });

        if (!sizeConsistencyErrorsFound.get())
            print(prefix + "No index size consistency errors found.");

        print("");
        print(prefix + "Total trees: " + treeInfos.keySet().size());
        print(prefix + "Total pages found in trees: " + totalStat.values().stream().mapToLong(a -> a).sum());
        print(prefix + "Total errors during trees traversal: " + totalErr.get());
        print("");

        print("------------------");
    }

    /**
     * Tries to get cache id from index tree name.
     *
     * @param name Index name.
     * @return Cache id.
     */
    public static int getCacheId(String name) {
        return getCacheAndTypeId(name).get1();
    }

    /**
     * Tries to get cache id and type id from index tree name.
     *
     * @param name Index name.
     * @return Pair of cache id and type id.
     */
    public static IgnitePair<Integer> getCacheAndTypeId(String name) {
        return CACHE_TYPE_IDS.computeIfAbsent(name, k -> {
            if (META_TREE_NAME.equals(name))
                return new IgnitePair<>(-1, -1);

            Matcher mId = CACHE_TYPE_ID_SEACH_PATTERN.matcher(k);

            if (mId.find()) {
                String id = mId.group("id");

                String typeId = mId.group("typeId");

                return new IgnitePair<>(Integer.parseInt(id), Integer.parseInt(typeId));
            }
            else {
                Matcher cId = CACHE_ID_SEACH_PATTERN.matcher(k);

                if (cId.find()) {
                    String id = cId.group("id");

                    return new IgnitePair<>(Integer.parseInt(id), 0);
                }
            }

            return new IgnitePair<>(0, 0);
        });
    }

    /**
     * Prints page lists info.
     *
     * @param pageListsInfo Page lists info.
     */
    private void printPagesListsInfo(PageListsInfo pageListsInfo) {
        String prefix = PAGE_LISTS_PREFIX;

        print("\n" + prefix + "Page lists info.");

        if (!pageListsInfo.bucketsData.isEmpty())
            print(prefix + "---Printing buckets data:");

        pageListsInfo.bucketsData.forEach((bucket, bucketData) -> {
            GridStringBuilder sb = new GridStringBuilder(prefix)
                .a("List meta id=")
                .a(bucket.get1())
                .a(", bucket number=")
                .a(bucket.get2())
                .a(", lists=[")
                .a(bucketData.stream().map(String::valueOf).collect(joining(", ")))
                .a("]");

            print(sb.toString());
        });

        printPageStat(prefix, "-- Page stat:", pageListsInfo.pageListStat);

        printErrors(prefix, "---Errors:", "---No errors.", "Page id: %s, exception: ", true, pageListsInfo.errors);

        print("");
        print(prefix + "Total index pages found in lists: " + pageListsInfo.allPages.size());
        print(prefix + "Total errors during lists scan: " + pageListsInfo.errors.size());
        print("------------------");
    }

    /**
     * Traverse single index tree from root to leafs.
     *
     * @param store File page store.
     * @param rootPageId Root page id.
     * @param treeName Tree name.
     * @param itemStorage Items storage.
     * @return Tree traversal info.
     */
    private TreeTraversalInfo traverseTree(FilePageStore store, long rootPageId, String treeName, ItemStorage itemStorage) {
        Map<Class, Long> ioStat = new HashMap<>();

        Map<Long, List<Throwable>> errors = new HashMap<>();

        Set<Long> innerPageIds = new HashSet<>();

        PageCallback innerCb = (content, pageId) -> innerPageIds.add(normalizePageId(pageId));

        ItemCallback itemCb = (currPageId, item, link) -> itemStorage.add(item);

        getTreeNode(rootPageId, new TreeTraverseContext(treeName, store, ioStat, errors, innerCb, null, itemCb));

        return new TreeTraversalInfo(ioStat, errors, innerPageIds, rootPageId, itemStorage);
    }

    /**
     * Traverse single index tree by each level horizontally.
     *
     * @param store File page store.
     * @param rootPageId Root page id.
     * @param treeName Tree name.
     * @param itemStorage Items storage.
     * @return Tree traversal info.
     */
    private TreeTraversalInfo horizontalTreeScan(
        FilePageStore store,
        long rootPageId,
        String treeName,
        ItemStorage itemStorage
    ) {
        Map<Long, List<Throwable>> errors = new HashMap<>();

        Map<Class, Long> ioStat = new HashMap<>();

        TreeTraverseContext treeCtx = new TreeTraverseContext(treeName, store, ioStat, errors, null, null, null);

        ByteBuffer buf = allocateBuffer(pageSize);

        try {
            long addr = bufferAddress(buf);

            readPage(store, rootPageId, buf);

            PageIO pageIO = PageIO.getPageIO(addr);

            if (!(pageIO instanceof BPlusMetaIO))
                throw new IgniteException("Root page is not meta, pageId=" + rootPageId);

            BPlusMetaIO metaIO = (BPlusMetaIO)pageIO;

            ioStat.compute(metaIO.getClass(), (k, v) -> v == null ? 1 : v + 1);

            int lvlsCnt = metaIO.getLevelsCount(addr);

            long[] firstPageIds = IntStream.range(0, lvlsCnt).mapToLong(i -> metaIO.getFirstPageId(addr, i)).toArray();

            for (int i = 0; i < lvlsCnt; i++) {
                long pageId = firstPageIds[i];

                while (pageId > 0) {
                    try {
                        buf.rewind();

                        readPage(store, pageId, buf);

                        pageIO = PageIO.getPageIO(addr);

                        if (i == 0 && !(pageIO instanceof BPlusLeafIO))
                            throw new IgniteException("Not-leaf page found on leaf level, pageId=" + pageId + ", level=" + i);

                        if (!(pageIO instanceof BPlusIO))
                            throw new IgniteException("Not-BPlus page found, pageId=" + pageId + ", level=" + i);

                        ioStat.compute(pageIO.getClass(), (k, v) -> v == null ? 1 : v + 1);

                        if (pageIO instanceof BPlusLeafIO) {
                            PageIOProcessor ioProcessor = getIOProcessor(pageIO);

                            PageContent pageContent = ioProcessor.getContent(pageIO, addr, pageId, treeCtx);

                            pageContent.items.forEach(itemStorage::add);
                        }

                        pageId = ((BPlusIO)pageIO).getForward(addr);
                    }
                    catch (Throwable e) {
                        errors.computeIfAbsent(pageId, k -> new LinkedList<>()).add(e);

                        pageId = 0;
                    }
                }
            }
        }
        catch (Throwable e) {
            errors.computeIfAbsent(rootPageId, k -> new LinkedList<>()).add(e);
        }
        finally {
            freeBuffer(buf);
        }

        return new TreeTraversalInfo(ioStat, errors, null, rootPageId, itemStorage);
    }

    /**
     * Gets tree node and all its children.
     *
     * @param pageId Page id, where tree node is located.
     * @param nodeCtx Tree traverse context.
     * @return Tree node.
     */
    private TreeNode getTreeNode(long pageId, TreeTraverseContext nodeCtx) {
        PageContent pageContent;

        PageIOProcessor ioProcessor;

        try {
            final ByteBuffer buf = allocateBuffer(pageSize);

            try {
                readPage(nodeCtx.store, pageId, buf);

                final long addr = bufferAddress(buf);

                final PageIO io = PageIO.getPageIO(addr);

                nodeCtx.ioStat.compute(io.getClass(), (k, v) -> v == null ? 1 : v + 1);

                ioProcessor = getIOProcessor(io);

                pageContent = ioProcessor.getContent(io, addr, pageId, nodeCtx);
            }
            finally {
                freeBuffer(buf);
            }

            return ioProcessor.getNode(pageContent, pageId, nodeCtx);
        }
        catch (Throwable e) {
            nodeCtx.errors.computeIfAbsent(pageId, k -> new LinkedList<>()).add(e);

            return new TreeNode(pageId, null, "exception: " + e.getMessage(), Collections.emptyList());
        }
    }

    /** */
    private PageIOProcessor getIOProcessor(PageIO io) {
        if (io instanceof BPlusLeafIO)
            return leafPageIOProcessor;
        else if (io instanceof BPlusInnerIO)
            return innerPageIOProcessor;
        else if (io instanceof BPlusMetaIO)
            return metaPageIOProcessor;
        else
            return null;
    }


    /** {@inheritDoc} */
    @Override public void close() throws StorageException {
        if (idxStore != null)
            idxStore.stop(false);

        if (partStores != null) {
            for (FilePageStore store : partStores) {
                if (store != null)
                    store.stop(false);
            }
        }
    }

    /**
     * Transforms snapshot files to regular PDS files.
     *
     * @param dest Destination directory.
     * @param fileMask File mask.
     */
    public void transform(String dest, String fileMask) {
        File destDir = new File(dest);

        if (!destDir.exists())
            destDir.mkdirs();

        try (DirectoryStream<Path> files = Files.newDirectoryStream(cacheWorkDir.toPath(), "*" + fileMask)) {
            List<Path> filesList = new LinkedList<>();

            for (Path f : files) {
                if (f.toString().toLowerCase().endsWith(fileMask))
                    filesList.add(f);
            }

            ProgressPrinter progressPrinter = new ProgressPrinter(System.out, "Transforming files", filesList.size());

            for (Path f : filesList) {
                progressPrinter.printProgress();

                try {
                    copyFromStreamToFile(
                        f.toFile(),
                        new File(destDir.getPath(), f.getFileName().toString()),
                        f.getFileName().toString().equals(INDEX_FILE_NAME) ? FLAG_IDX : FLAG_DATA
                    );
                }
                catch (Exception e) {
                    File destF = new File(destDir.getPath(), f.getFileName().toString());

                    if (destF.exists())
                        destF.delete();

                    printErr("Could not transform file: " + destF.getPath() + ", error: " + e.getMessage());

                    printStackTrace(e);
                }
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** */
    private int copyFromStreamToFile(File fileInPath, File fileOutPath, byte flag) throws IOException, IgniteCheckedException {
        ByteBuffer readBuf = GridUnsafe.allocateBuffer(pageSize);

        try {
            readBuf.order(ByteOrder.nativeOrder());

            long readAddr = GridUnsafe.bufferAddress(readBuf);

            ByteBuffer hdrBuf = headerBuffer(flag, pageSize);

            try (FileChannel ch = FileChannel.open(fileOutPath.toPath(), WRITE, CREATE)) {
                int hdrSize = hdrBuf.limit();

                ch.write(hdrBuf, 0);

                int pageCnt = 0;

                FileChannel stream = new RandomAccessFile(fileInPath, "r").getChannel();

                while (readNextPage(readBuf, stream, pageSize)) {
                    pageCnt++;

                    readBuf.rewind();

                    long pageId = PageIO.getPageId(readAddr);

                    assert pageId != 0;

                    int pageIdx = PageIdUtils.pageIndex(pageId);

                    int crcSaved = PageIO.getCrc(readAddr);

                    PageIO.setCrc(readAddr, 0);

                    int calced = FastCrc.calcCrc(readBuf, pageSize);

                    if (calced != crcSaved)
                        throw new IgniteCheckedException("Snapshot corrupted");

                    PageIO.setCrc(readAddr, crcSaved);

                    readBuf.rewind();

                    boolean changed = false;

                    int pageType = PageIO.getType(readAddr);

                    switch (pageType) {
                        case PageIO.T_PAGE_UPDATE_TRACKING:
                            PageHandler.zeroMemory(readAddr, TrackingPageIO.COMMON_HEADER_END,
                                readBuf.capacity() - TrackingPageIO.COMMON_HEADER_END);

                            changed = true;

                            break;
                        case PageIO.T_META:
                        case PageIO.T_PART_META:
                            PageMetaIO io = PageIO.getPageIO(pageType, PageIO.getVersion(readAddr));

                            io.setLastAllocatedPageCount(readAddr, 0);
                            io.setLastSuccessfulFullSnapshotId(readAddr, 0);
                            io.setLastSuccessfulSnapshotId(readAddr, 0);
                            io.setLastSuccessfulSnapshotTag(readAddr, 0);
                            io.setNextSnapshotTag(readAddr, 1);
                            io.setCandidatePageCount(readAddr, 0);

                            changed = true;

                            break;
                    }
                    if (changed) {
                        PageIO.setCrc(readAddr, 0);

                        int crc32 = FastCrc.calcCrc(readBuf, pageSize);

                        PageIO.setCrc(readAddr, crc32);

                        readBuf.rewind();
                    }
                    ch.write(readBuf, hdrSize + ((long)pageIdx) * pageSize);

                    readBuf.rewind();
                }

                ch.force(true);

                return pageCnt;
            }
        }
        finally {
            freeBuffer(readBuf);
        }
    }

    /** */
    private static boolean readNextPage(ByteBuffer buf, FileChannel ch, int pageSize) throws IOException {
        assert buf.remaining() == pageSize;

        do {
            if (ch.read(buf) == -1)
                break;
        }
        while (buf.hasRemaining());

        if (!buf.hasRemaining() && PageIO.getPageId(buf) != 0)
            return true; //pageSize bytes read && pageId != 0
        else if (buf.remaining() == pageSize)
            return false; //0 bytes read
        else
            // 1 <= readBytes < pageSize || readBytes == pagesIze && pageId != 0
            throw new IgniteException("Corrupted page in partitionId " +
                ", readByte=" + buf.position() + ", pageSize=" + pageSize);
    }

    /** */
    private ByteBuffer headerBuffer(byte type, int pageSize) {
        FilePageStore store = storeFactory.createPageStore(type, null, storeFactory.latestVersion(), allocationTracker);

        return store.header(type, pageSize);
    }

    /**
     * Entry point.
     *
     * @param args Arguments.
     * @throws Exception If failed.
     */
    public static void main(String[] args) throws Exception {
        System.out.println("THIS UTILITY MUST BE LAUNCHED ON PERSISTENT STORE WHICH IS NOT UNDER RUNNING GRID!");

        AtomicReference<CLIArgumentParser> parserRef = new AtomicReference<>();

        List<CLIArgument> argsConfiguration = asList(
            mandatoryArg(
                    DIR.arg(),
                    "partition directory, where " + INDEX_FILE_NAME + " and (optionally) partition files are located.",
                    String.class
            ),
            optionalArg(PART_CNT.arg(), "full partitions count in cache group.", Integer.class, () -> 0),
            optionalArg(PAGE_SIZE.arg(), "page size.", Integer.class, () -> 4096),
            optionalArg(PAGE_STORE_VER.arg(), "page store version.", Integer.class, () -> 2),
            optionalArg(INDEXES.arg(), "you can specify index tree names that will be processed, separated by comma " +
                "without spaces, other index trees will be skipped.", String[].class, () -> null),
            optionalArg(DEST_FILE.arg(),
                    "file to print the report to (by default report is printed to console).", String.class, () -> null),
            optionalArg(TRANSFORM.arg(), "if specified, this utility assumes that all *.bin files " +
                "in --dir directory are snapshot files, and transforms them to normal format and puts to --dest" +
                " directory.", Boolean.class, () -> false),
            optionalArg(DEST.arg(),
                "directory where to put files transformed from snapshot (needed if you use --transform).",
                String.class,
                () -> {
                    if (parserRef.get().get(TRANSFORM.arg()))
                        throw new IgniteException("Destination path for transformed files is not specified (use --dest)");
                    else
                        return null;
                }
            ),
            optionalArg(FILE_MASK.arg(),
                    "mask for files to transform (optional if you use --transform).", String.class, () -> ".bin"),
            optionalArg(CHECK_PARTS.arg(),
                    "check cache data tree in partition files and it's consistency with indexes.", Boolean.class, () -> false)
        );

        CLIArgumentParser p = new CLIArgumentParser(argsConfiguration);

        parserRef.set(p);

        if (args.length == 0) {
            System.out.println(p.usage());

            return;
        }

        p.parse(asList(args).iterator());

        String destFile = p.get(DEST_FILE.arg());

        OutputStream destStream = destFile == null ? null : new FileOutputStream(destFile);

        if (p.get(TRANSFORM.arg())) {
            try (IgniteIndexReader reader = new IgniteIndexReader(
                    p.get(DIR.arg()),
                    p.get(PAGE_SIZE.arg()),
                    p.get(PAGE_STORE_VER.arg()),
                    destStream
            )) {
                reader.transform(p.get(DEST.arg()), p.get(FILE_MASK.arg()));
            }
        }
        else {
            try (IgniteIndexReader reader = new IgniteIndexReader(
                    p.get(DIR.arg()),
                    p.get(PAGE_SIZE.arg()),
                    p.get(PART_CNT.arg()),
                    p.get(PAGE_STORE_VER.arg()),
                    p.get(INDEXES.arg()),
                    p.get(CHECK_PARTS.arg()),
                    destStream
            )) {
                reader.readIdx();
            }
        }
    }

    /**
     * Enum of possible utility arguments.
     */
    public enum Args {
        /** */
        DIR("--dir"),
        /** */
        PART_CNT("--partCnt"),
        /** */
        PAGE_SIZE("--pageSize"),
        /** */
        PAGE_STORE_VER("--pageStoreVer"),
        /** */
        INDEXES("--indexes"),
        /** */
        DEST_FILE("--destFile"),
        /** */
        TRANSFORM("--transform"),
        /** */
        DEST("--dest"),
        /** */
        FILE_MASK("--fileMask"),
        /** */
        CHECK_PARTS("--checkParts");

        /** */
        private String arg;

        /** */
        Args(String arg) {
            this.arg = arg;
        }

        /** */
        public String arg() {
            return arg;
        }
    }

    /**
     *
     */
    private interface BufferClosure<T> {
        /** */
        T apply(ByteBuffer buf, Long addr) throws IgniteCheckedException;
    }

    /**
     *
     */
    private interface TraverseProc {
        /** */
        TreeTraversalInfo traverse(FilePageStore store, long rootId, String treeName, ItemStorage itemStorage);
    }

    /**
     * Processor for page IOs.
     */
    private interface PageIOProcessor {
        /**
         * Gets deserialized content.
         * @param io Page IO.
         * @param addr Page address.
         * @param pageId Page id.
         * @param nodeCtx Tree traversal context.
         * @return Page content.
         */
        PageContent getContent(PageIO io, long addr, long pageId, TreeTraverseContext nodeCtx);

        /**
         * Gets node info from page contents.
         * @param content Page content.
         * @param pageId Page id.
         * @param nodeCtx Tree traversal context.
         * @return Tree node info.
         */
        TreeNode getNode(PageContent content, long pageId, TreeTraverseContext nodeCtx);
    }

    /**
     *
     */
    private class MetaPageIOProcessor implements PageIOProcessor {
        /** {@inheritDoc} */
        @Override public PageContent getContent(PageIO io, long addr, long pageId, TreeTraverseContext nodeCtx) {
            BPlusMetaIO bPlusMetaIO = (BPlusMetaIO)io;

            int rootLvl = bPlusMetaIO.getRootLevel(addr);
            long rootId = bPlusMetaIO.getFirstPageId(addr, rootLvl);

            return new PageContent(io, singletonList(rootId), null, null);
        }

        /** {@inheritDoc} */
        @Override public TreeNode getNode(PageContent content, long pageId, TreeTraverseContext nodeCtx) {
            return new TreeNode(pageId, content.io, null, singletonList(getTreeNode(content.linkedPageIds.get(0), nodeCtx)));
        }
    }

    /**
     *
     */
    private class InnerPageIOProcessor implements PageIOProcessor {
        /** {@inheritDoc} */
        @Override public PageContent getContent(PageIO io, long addr, long pageId, TreeTraverseContext nodeCtx) {
            BPlusInnerIO innerIo = (BPlusInnerIO)io;

            int cnt = innerIo.getCount(addr);

            List<Long> childrenIds;

            if (cnt > 0) {
                childrenIds = new ArrayList<>(cnt + 1);

                for (int i = 0; i < cnt; i++)
                    childrenIds.add(innerIo.getLeft(addr, i));

                childrenIds.add(innerIo.getRight(addr, cnt - 1));
            }
            else {
                long left = innerIo.getLeft(addr, 0);

                childrenIds = left == 0 ? Collections.<Long>emptyList() : singletonList(left);
            }

            return new PageContent(io, childrenIds, null, null);
        }

        /** {@inheritDoc} */
        @Override public TreeNode getNode(PageContent content, long pageId, TreeTraverseContext nodeCtx) {
            List<TreeNode> children = new ArrayList<>(content.linkedPageIds.size());

            for (Long id : content.linkedPageIds)
                children.add(getTreeNode(id, nodeCtx));

            if (nodeCtx.innerCb != null)
                nodeCtx.innerCb.cb(content, pageId);

            return new TreeNode(pageId, content.io, null, children);
        }
    }

    /**
     *
     */
    private class LeafPageIOProcessor implements PageIOProcessor {
        /** {@inheritDoc} */
        @Override public PageContent getContent(PageIO io, long addr, long pageId, TreeTraverseContext nodeCtx) {
            GridStringBuilder sb = new GridStringBuilder();

            List<Object> items = new LinkedList<>();

            BPlusLeafIO leafIO = (BPlusLeafIO)io;

            for (int j = 0; j < leafIO.getCount(addr); j++) {
                Object idxItem = null;

                try {
                    if (io instanceof IndexStorageImpl.MetaStoreLeafIO) {
                        idxItem = ((IndexStorageImpl.MetaStoreLeafIO)io).getLookupRow(null, addr, j);

                        sb.a(idxItem.toString() + " ");
                    }
                    else
                        idxItem = getLeafItem(leafIO, pageId, addr, j, nodeCtx);
                }
                catch (Exception e) {
                    nodeCtx.errors.computeIfAbsent(pageId, k -> new LinkedList<>()).add(e);
                }

                if (idxItem != null)
                    items.add(idxItem);
            }

            return new PageContent(io, null, items, sb.toString());
        }

        /** */
        private Object getLeafItem(BPlusLeafIO io, long pageId, long addr, int idx, TreeTraverseContext nodeCtx) {
            if (isLinkIo(io)) {
                final long link = getLink(io, addr, idx);

                int cacheId;

                if (io instanceof AbstractDataLeafIO && ((AbstractDataLeafIO)io).storeCacheId())
                    cacheId = ((AbstractDataLeafIO)io).getCacheId(addr, idx);
                else
                    cacheId = nodeCtx.cacheId;

                final CacheAwareLink res = new CacheAwareLink(cacheId, link);

                if (partCnt > 0) {
                    try {
                        long linkedPageId = pageId(link);

                        int linkedPagePartId = partId(linkedPageId);

                        if (missingPartitions.contains(linkedPagePartId))
                            return res; // just skip

                        int linkedItemId = itemId(link);

                        if (linkedPagePartId > partStores.length - 1) {
                            missingPartitions.add(linkedPagePartId);

                            throw new IgniteException("Calculated data page partition id exceeds given partitions " +
                                "count: " + linkedPagePartId + ", partCnt=" + partCnt);
                        }

                        final FilePageStore store = partStores[linkedPagePartId];

                        if (store == null) {
                            missingPartitions.add(linkedPagePartId);

                            throw new IgniteException("Corresponding store wasn't found for partId=" +
                                linkedPagePartId + ". Does partition file exist?");
                        }

                        doWithBuffer((dataBuf, dataBufAddr) -> {
                            readPage(store, linkedPageId, dataBuf);

                            PageIO dataIo = PageIO.getPageIO(getType(dataBuf), getVersion(dataBuf));

                            if (dataIo instanceof AbstractDataPageIO) {
                                AbstractDataPageIO dataPageIO = (AbstractDataPageIO)dataIo;

                                DataPagePayload payload = dataPageIO.readPayload(dataBufAddr, linkedItemId, pageSize);

                                if (payload.offset() <= 0 || payload.payloadSize() <= 0) {
                                    GridStringBuilder payloadInfo = new GridStringBuilder("Invalid data page payload: ")
                                        .a("off=").a(payload.offset())
                                        .a(", size=").a(payload.payloadSize())
                                        .a(", nextLink=").a(payload.nextLink());

                                    throw new IgniteException(payloadInfo.toString());
                                }
                            }

                            return null;
                        });
                    }
                    catch (Exception e) {
                        nodeCtx.errors.computeIfAbsent(pageId, k -> new LinkedList<>()).add(e);
                    }
                }

                return res;
            }
            else
                throw new IgniteException("Unexpected page io: " + io.getClass().getSimpleName());
        }

        /** */
        private boolean isLinkIo(PageIO io) {
            return io instanceof H2RowLinkIO || io instanceof PendingRowIO || io instanceof RowLinkIO;
        }

        /** */
        private long getLink(BPlusLeafIO io, long addr, int idx) {
            if (io instanceof RowLinkIO)
                return ((RowLinkIO)io).getLink(addr, idx);
            if (io instanceof H2RowLinkIO)
                return ((H2RowLinkIO)io).getLink(addr, idx);
            else if (io instanceof PendingRowIO)
                return ((PendingRowIO)io).getLink(addr, idx);
            else
                throw new IgniteException("No link to data page on idx=" + idx);
        }

        /** {@inheritDoc} */
        @Override public TreeNode getNode(PageContent content, long pageId, TreeTraverseContext nodeCtx) {
            if (nodeCtx.leafCb != null)
                nodeCtx.leafCb.cb(content, pageId);

            if (nodeCtx.itemCb != null) {
                for (Object item : content.items)
                    nodeCtx.itemCb.cb(pageId, item, 0);
            }

            return new TreeNode(pageId, content.io, content.info, Collections.emptyList());
        }
    }
}

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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.cache.query.index.sorted.inline.io.AbstractInlineInnerIO;
import org.apache.ignite.internal.cache.query.index.sorted.inline.io.AbstractInlineLeafIO;
import org.apache.ignite.internal.cache.query.index.sorted.inline.io.InlineIO;
import org.apache.ignite.internal.cache.query.index.sorted.inline.io.InnerIO;
import org.apache.ignite.internal.cache.query.index.sorted.inline.io.LeafIO;
import org.apache.ignite.internal.cache.query.index.sorted.inline.io.MvccInnerIO;
import org.apache.ignite.internal.cache.query.index.sorted.inline.io.MvccLeafIO;
import org.apache.ignite.internal.commandline.ProgressPrinter;
import org.apache.ignite.internal.commandline.StringBuilderOutputStream;
import org.apache.ignite.internal.commandline.argument.parser.CLIArgument;
import org.apache.ignite.internal.commandline.argument.parser.CLIArgumentParser;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.persistence.IndexStorageImpl;
import org.apache.ignite.internal.processors.cache.persistence.StorageException;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStore;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
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
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PagePartitionMetaIO;
import org.apache.ignite.internal.processors.cache.persistence.wal.crc.IgniteDataIntegrityViolationException;
import org.apache.ignite.internal.processors.cache.tree.AbstractDataLeafIO;
import org.apache.ignite.internal.processors.cache.tree.PendingRowIO;
import org.apache.ignite.internal.processors.cache.tree.RowLinkIO;
import org.apache.ignite.internal.processors.cache.tree.mvcc.data.MvccDataLeafIO;
import org.apache.ignite.internal.util.GridLongList;
import org.apache.ignite.internal.util.GridStringBuilder;
import org.apache.ignite.internal.util.lang.GridClosure3;
import org.apache.ignite.internal.util.lang.IgnitePair;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.Nullable;

import static java.lang.Integer.parseInt;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.commandline.argument.parser.CLIArgument.mandatoryArg;
import static org.apache.ignite.internal.commandline.argument.parser.CLIArgument.optionalArg;
import static org.apache.ignite.internal.commandline.indexreader.IgniteIndexReader.Args.CHECK_PARTS;
import static org.apache.ignite.internal.commandline.indexreader.IgniteIndexReader.Args.DEST_FILE;
import static org.apache.ignite.internal.commandline.indexreader.IgniteIndexReader.Args.DIR;
import static org.apache.ignite.internal.commandline.indexreader.IgniteIndexReader.Args.INDEXES;
import static org.apache.ignite.internal.commandline.indexreader.IgniteIndexReader.Args.PAGE_SIZE;
import static org.apache.ignite.internal.commandline.indexreader.IgniteIndexReader.Args.PAGE_STORE_VER;
import static org.apache.ignite.internal.commandline.indexreader.IgniteIndexReader.Args.PART_CNT;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.FLAG_DATA;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.FLAG_IDX;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.INDEX_PARTITION;
import static org.apache.ignite.internal.pagemem.PageIdUtils.flag;
import static org.apache.ignite.internal.pagemem.PageIdUtils.itemId;
import static org.apache.ignite.internal.pagemem.PageIdUtils.pageId;
import static org.apache.ignite.internal.pagemem.PageIdUtils.pageIndex;
import static org.apache.ignite.internal.pagemem.PageIdUtils.partId;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.INDEX_FILE_NAME;
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
        PageIO.registerH2(InnerIO.VERSIONS, LeafIO.VERSIONS, MvccInnerIO.VERSIONS, MvccLeafIO.VERSIONS);

        AbstractInlineInnerIO.register();
        AbstractInlineLeafIO.register();
    }

    /** Page size. */
    private final int pageSize;

    /** Partition count. */
    private final int partCnt;

    /** Index name filter, if {@code null} then is not used. */
    @Nullable private final Predicate<String> idxFilter;

    /** Output strean. */
    private final PrintStream outStream;

    /** Page store of {@link FilePageStoreManager#INDEX_FILE_NAME}. */
    @Nullable private final FilePageStore idxStore;

    /** Partitions page stores, may contains {@code null}. */
    @Nullable private final FilePageStore[] partStores;

    /** Check cache data tree in partition files and it's consistency with indexes. */
    private final boolean checkParts;

    /** */
    private final Set<Integer> missingPartitions = new HashSet<>();

    /** */
    private final PageIOProcessor innerPageIOProcessor = new InnerPageIOProcessor();

    /** */
    private final PageIOProcessor leafPageIOProcessor = new LeafPageIOProcessor();

    /** */
    private final PageIOProcessor metaPageIOProcessor = new MetaPageIOProcessor();

    /**
     * Constructor.
     *
     * @param idxFilter Index name filter, if {@code null} then is not used.
     * @param checkParts Check cache data tree in partition files and it's consistency with indexes.
     * @param outStream {@link PrintStream} for print report, if {@code null} then will be used {@link System#out}.
     * @throws IgniteCheckedException If failed.
     */
    public IgniteIndexReader(
        @Nullable Predicate<String> idxFilter,
        boolean checkParts,
        @Nullable PrintStream outStream,
        IgniteIndexReaderFilePageStoreFactory filePageStoreFactory
    ) throws IgniteCheckedException {
        pageSize = filePageStoreFactory.pageSize();
        partCnt = filePageStoreFactory.partitionCount();
        this.checkParts = checkParts;
        this.idxFilter = idxFilter;

        this.outStream = isNull(outStream) ? System.out : outStream;

        Map<Integer, List<Throwable>> partStoresErrors = new HashMap<>();
        List<Throwable> errors = new ArrayList<>();

        idxStore = filePageStoreFactory.createFilePageStoreWithEnsure(INDEX_PARTITION, FLAG_IDX, errors);

        if (!errors.isEmpty())
            partStoresErrors.put(INDEX_PARTITION, new ArrayList<>(errors));

        if (isNull(idxStore))
            throw new IgniteCheckedException(INDEX_FILE_NAME + " file not found");
        else
            print("Analyzing file: " + INDEX_FILE_NAME);

        partStores = new FilePageStore[partCnt];

        for (int i = 0; i < partCnt; i++) {
            if (!errors.isEmpty())
                errors.clear();

            partStores[i] = filePageStoreFactory.createFilePageStoreWithEnsure(i, FLAG_DATA, errors);

            if (!errors.isEmpty())
                partStoresErrors.put(i, new ArrayList<>(errors));
        }

        printFileReadingErrors(partStoresErrors);
    }

    /** */
    private void print(String s) {
        outStream.println(s);
    }

    /** */
    private void printErr(String s) {
        outStream.println(ERROR_PREFIX + s);
    }

    /** */
    private void printErrors(
        String prefix,
        String caption,
        @Nullable String alternativeCaption,
        String elementFormatPtrn,
        boolean printTrace,
        Map<?, ? extends List<? extends Throwable>> errors
    ) {
        if (errors.isEmpty() && alternativeCaption != null) {
            print(prefix + alternativeCaption);

            return;
        }

        if (caption != null)
            outStream.println(prefix + ERROR_PREFIX + caption);

        errors.forEach((k, v) -> {
            outStream.println(prefix + ERROR_PREFIX + format(elementFormatPtrn, k.toString()));

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

        outStream.println(os.toString());
    }

    /** */
    static long normalizePageId(long pageId) {
        return pageId(partId(pageId), flag(pageId), pageIndex(pageId));
    }

    /**
     * Reading pages into buffer.
     *
     * @param store Source for reading pages.
     * @param pageId Page ID.
     * @param buf Buffer.
     */
    private void readPage(FilePageStore store, long pageId, ByteBuffer buf) throws IgniteCheckedException {
        try {
            store.read(pageId, buf, false);
        }
        catch (IgniteDataIntegrityViolationException | IllegalArgumentException e) {
            // Replacing exception due to security reasons, as IgniteDataIntegrityViolationException prints page content.
            // Catch IllegalArgumentException for output page information.
            throw new IgniteException("Failed to read page, id=" + pageId + ", idx=" + pageIndex(pageId) +
                ", file=" + store.getFileAbsolutePath());
        }
    }

    /**
     * @return Tuple consisting of meta tree root page and pages list root page.
     * @throws IgniteCheckedException If failed.
     */
    IgniteBiTuple<Long, Long> partitionRoots(long pageMetaPageId) throws IgniteCheckedException {
        AtomicLong pageListMetaPageId = new AtomicLong();
        AtomicLong metaTreeRootId = new AtomicLong();

        doWithBuffer((buf, addr) -> {
            readPage(filePageStore(partId(pageMetaPageId)), pageMetaPageId, buf);

            PageMetaIO pageMetaIO = PageIO.getPageIO(addr);

            pageListMetaPageId.set(normalizePageId(pageMetaIO.getReuseListRoot(addr)));

            metaTreeRootId.set(normalizePageId(pageMetaIO.getTreeRoot(addr)));

            return null;
        });

        return new IgniteBiTuple<>(metaTreeRootId.get(), pageListMetaPageId.get());
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

        long pagesNum = isNull(idxStore) ? 0 : (idxStore.size() - idxStore.headerSize()) / pageSize;

        print("Going to check " + pagesNum + " pages.");

        Set<Long> pageIds = new HashSet<>();

        AtomicReference<Map<String, TreeTraversalInfo>> treeInfo = new AtomicReference<>();

        AtomicReference<Map<String, TreeTraversalInfo>> horizontalScans = new AtomicReference<>();

        AtomicReference<PageListsInfo> pageListsInfo = new AtomicReference<>();

        List<Throwable> errors;

        try {
            IgniteBiTuple<Long, Long> indexPartitionRoots = partitionRoots(partMetaPageId(INDEX_PARTITION, FLAG_IDX));

            long metaTreeRootId = indexPartitionRoots.get1();
            long pageListMetaPageId = indexPartitionRoots.get2();

            // Traversing trees.
            treeInfo.set(traverseAllTrees("Index trees traversal", metaTreeRootId, CountOnlyStorage::new, this::traverseTree));

            treeInfo.get().forEach((name, info) -> {
                pageIds.addAll(info.innerPageIds);

                pageIds.add(info.rootPageId);
            });

            Supplier<ItemStorage> itemStorageFactory = checkParts ? LinkStorage::new : CountOnlyStorage::new;

            horizontalScans.set(
                traverseAllTrees("Scan index trees horizontally", metaTreeRootId, itemStorageFactory, this::horizontalTreeScan)
            );

            // Scanning page reuse lists.
            if (pageListMetaPageId != 0)
                pageListsInfo.set(getPageListsInfo(pageListMetaPageId));

            ProgressPrinter progressPrinter = new ProgressPrinter(System.out, "Reading pages sequentially", pagesNum);

            // Scan all pages in file.
            errors = scanFileStore(INDEX_PARTITION, FLAG_IDX, idxStore, (pageId, addr, io) -> {
                progressPrinter.printProgress();

                pageClasses.compute(io.getClass(), (k, v) -> v == null ? 1 : v + 1);

                if (!(io instanceof PageMetaIO || io instanceof PagesListMetaIO)) {
                    if (idxFilter == null) {
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

        if (idxFilter != null)
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
     * Print partitions reading exceptions.
     *
     * @param partStoresErrors Partitions reading exceptions.
     */
    private void printFileReadingErrors(Map<Integer, List<Throwable>> partStoresErrors) {
        List<Throwable> idxPartErrors = partStoresErrors.get(INDEX_PARTITION);

        if (!F.isEmpty(idxPartErrors)) {
            printErr("Errors detected while reading " + INDEX_FILE_NAME);

            idxPartErrors.forEach(err -> printErr(err.getMessage()));

            partStoresErrors.remove(INDEX_PARTITION);
        }

        if (!partStoresErrors.isEmpty()) {
            printErrors("", "Errors detected while reading partition files:", null,
                "Partition id: %s, exceptions: ", false, partStoresErrors);
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
            List<Throwable> errors = new ArrayList<>();

            long pagesNum = isNull(store) ? 0 : (store.size() - store.headerSize()) / pageSize;

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
                long partMetaId = partMetaPageId(i, FLAG_DATA);

                doWithBuffer((buf, addr) -> {
                    readPage(partStore, partMetaId, buf);

                    PagePartitionMetaIO partMetaIO = PageIO.getPageIO(addr);

                    long cacheDataTreeRoot = partMetaIO.getTreeRoot(addr);

                    TreeTraversalInfo cacheDataTreeInfo =
                        horizontalTreeScan(cacheDataTreeRoot, "dataTree-" + partId, new ItemsListStorage());

                    for (Object dataTreeItem : cacheDataTreeInfo.itemStorage) {
                        CacheAwareLink cacheAwareLink = (CacheAwareLink)dataTreeItem;

                        for (Map.Entry<String, TreeTraversalInfo> e : treesInfo.entrySet()) {
                            String name = e.getKey();

                            TreeTraversalInfo tree = e.getValue();

                            int cacheId = getCacheId(name);

                            if (cacheId != cacheAwareLink.cacheId)
                                continue; // It's index for other cache, don't check.

                            // Tombstones are not indexed and shouldn't be tested.
                            if (!tree.itemStorage.contains(cacheAwareLink) && !cacheAwareLink.tombstone)
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
     * @param partId Partition id.
     * @param flag Flag.
     * @return Id of partition meta page.
     */
    public static long partMetaPageId(int partId, byte flag) {
        return PageIdUtils.pageId(partId, flag, 0);
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
        return format(
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

                            for (Long listId : listIds) {
                                try {
                                    allPages.addAll(getPageList(listId, pageListStat));
                                }
                                catch (Exception e) {
                                    errors.put(listId, singletonList(e));
                                }
                            }

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
            traverseProc.traverse(metaTreeRoot, META_TREE_NAME, new ItemsListStorage<IndexStorageImpl.IndexItem>());

        treeInfos.put(META_TREE_NAME, metaTreeTraversalInfo);

        ProgressPrinter progressPrinter =
            new ProgressPrinter(System.out, traverseProcCaption, metaTreeTraversalInfo.itemStorage.size());

        metaTreeTraversalInfo.itemStorage.forEach(item -> {
            progressPrinter.printProgress();

            IndexStorageImpl.IndexItem idxItem = (IndexStorageImpl.IndexItem)item;

            if (nonNull(idxFilter) && !idxFilter.test(idxItem.nameString()))
                return;

            TreeTraversalInfo treeTraversalInfo =
                traverseProc.traverse(normalizePageId(idxItem.pageId()), idxItem.nameString(), itemStorageFactory.get());

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

                printErr("Index size inconsistency: cacheId=" + cacheTypeId.get1() + ", typeId=" + cacheTypeId.get2());

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
            Matcher mId = CACHE_TYPE_ID_SEACH_PATTERN.matcher(k);

            if (mId.find()) {
                String id = mId.group("id");

                String typeId = mId.group("typeId");

                return new IgnitePair<>(parseInt(id), parseInt(typeId));
            }
            else {
                Matcher cId = CACHE_ID_SEACH_PATTERN.matcher(k);

                if (cId.find()) {
                    String id = cId.group("id");

                    return new IgnitePair<>(parseInt(id), 0);
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
     * @param rootPageId Root page id.
     * @param treeName Tree name.
     * @param innerCb Inner pages callback.
     * @param leafCb Leaf pages callback.
     * @param itemCb Items callback.
     * @param itemStorage Items storage.
     * @return Tree traversal info.
     */
    TreeTraversalInfo traverseTree(
        long rootPageId,
        String treeName,
        @Nullable PageCallback innerCb,
        @Nullable PageCallback leafCb,
        @Nullable ItemCallback itemCb,
        ItemStorage itemStorage
    ) {
        FilePageStore store = filePageStore(partId(rootPageId));

        Map<Class, Long> ioStat = new HashMap<>();

        Map<Long, List<Throwable>> errors = new HashMap<>();

        Set<Long> innerPageIds = new HashSet<>();

        PageCallback innerCb0 = (content, pageId) -> {
            if (innerCb != null)
                innerCb.cb(content, pageId);

            innerPageIds.add(normalizePageId(pageId));
        };

        ItemCallback itemCb0 = (currPageId, item, link) -> {
            if (itemCb != null)
                itemCb.cb(currPageId, item, link);

            itemStorage.add(item);
        };

        getTreeNode(rootPageId, new TreeTraverseContext(treeName, store, ioStat, errors, innerCb0, leafCb, itemCb0));

        return new TreeTraversalInfo(ioStat, errors, innerPageIds, rootPageId, itemStorage);
    }

    /**
     * Traverse single index tree from root to leafs.
     *
     * @param rootPageId Root page id.
     * @param treeName Tree name.
     * @param itemStorage Items storage.
     * @return Tree traversal info.
     */
    TreeTraversalInfo traverseTree(long rootPageId, String treeName, ItemStorage itemStorage) {
        return traverseTree(rootPageId, treeName, null, null, null, itemStorage);
    }

    /**
     * Traverse single index tree by each level horizontally.
     *
     * @param rootPageId Root page id.
     * @param treeName Tree name.
     * @param itemStorage Items storage.
     * @return Tree traversal info.
     */
    private TreeTraversalInfo horizontalTreeScan(
        long rootPageId,
        String treeName,
        ItemStorage itemStorage
    ) {
        FilePageStore store = filePageStore(partId(rootPageId));

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
     * @param partId Partition id.
     * @return File page store of given partition.
     */
    private FilePageStore filePageStore(int partId) {
        return partId == INDEX_PARTITION ? idxStore : partStores[partId];
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
        if (nonNull(idxStore))
            idxStore.stop(false);

        if (nonNull(partStores)) {
            for (FilePageStore store : partStores) {
                if (nonNull(store))
                    store.stop(false);
            }
        }
    }

    /**
     * Reading a page from channel into buffer.
     *
     * @param buf Buffer.
     * @param ch Source for reading pages.
     * @param pageSize Size of page to read into buffer.
     */
    private boolean readNextPage(ByteBuffer buf, FileChannel ch, int pageSize) throws IOException {
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

        OutputStream destStream = isNull(destFile) ? null : new FileOutputStream(destFile);

        String dir = p.get(DIR.arg());

        int pageSize = p.get(PAGE_SIZE.arg());

        IgniteIndexReaderFilePageStoreFactory filePageStoreFactory = new IgniteIndexReaderFilePageStoreFactoryImpl(
            new File(dir),
            pageSize,
            p.get(PART_CNT.arg()),
            p.get(PAGE_STORE_VER.arg())
        );

        String[] idxArr = p.get(INDEXES.arg());
        Set<String> idxSet = isNull(idxArr) ? null : new HashSet<>(asList(idxArr));

        try (IgniteIndexReader reader = new IgniteIndexReader(
            isNull(idxSet) ? null : idxSet::contains,
            p.get(CHECK_PARTS.arg()),
            isNull(destStream) ? null : new PrintStream(destFile),
            filePageStoreFactory
        )) {
            reader.readIdx();
        }
    }

    /**
     * Enum of possible utility arguments.
     */
    public enum Args {
        /** */
        DIR("--dir"),
        /** */
        PART_CNT("--part-cnt"),
        /** */
        PAGE_SIZE("--page-size"),
        /** */
        PAGE_STORE_VER("--page-store-ver"),
        /** */
        INDEXES("--indexes"),
        /** */
        DEST_FILE("--dest-file"),
        /** */
        CHECK_PARTS("--check-parts");

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
        TreeTraversalInfo traverse(long rootId, String treeName, ItemStorage itemStorage);
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

                final int cacheId;

                if (io instanceof AbstractDataLeafIO && ((AbstractDataLeafIO)io).storeCacheId())
                    cacheId = ((AbstractDataLeafIO) io).getCacheId(addr, idx);
                else
                    cacheId = nodeCtx.cacheId;

                boolean tombstone = false;

                if (partCnt > 0) {
                    try {
                        long linkedPageId = pageId(link);

                        int linkedPagePartId = partId(linkedPageId);

                        if (missingPartitions.contains(linkedPagePartId))
                            return new CacheAwareLink(cacheId, link, false); // just skip

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

                        tombstone = doWithBuffer((dataBuf, dataBufAddr) -> {
                            readPage(store, linkedPageId, dataBuf);

                            PageIO dataIo = PageIO.getPageIO(getType(dataBuf), getVersion(dataBuf));

                            if (dataIo instanceof AbstractDataPageIO) {
                                AbstractDataPageIO dataPageIO = (AbstractDataPageIO) dataIo;

                                DataPagePayload payload = dataPageIO.readPayload(dataBufAddr, linkedItemId, pageSize);

                                if (payload.offset() <= 0 || payload.payloadSize() <= 0) {
                                    GridStringBuilder payloadInfo = new GridStringBuilder("Invalid data page payload: ")
                                        .a("off=").a(payload.offset())
                                        .a(", size=").a(payload.payloadSize())
                                        .a(", nextLink=").a(payload.nextLink());

                                    throw new IgniteException(payloadInfo.toString());
                                }

                                if (payload.nextLink() == 0) {
                                    if (io instanceof MvccDataLeafIO)
                                        return false;

                                    int off = payload.offset();

                                    int len = PageUtils.getInt(dataBufAddr, off);

                                    byte type = PageUtils.getByte(dataBufAddr, off + len + 9);

                                    return type == CacheObject.TOMBSTONE;
                                }
                            }

                            return false;
                        });
                    }
                    catch (Exception e) {
                        nodeCtx.errors.computeIfAbsent(pageId, k -> new LinkedList<>()).add(e);
                    }
                }

                return new CacheAwareLink(cacheId, link, tombstone);
            }
            else
                throw new IgniteException("Unexpected page io: " + io.getClass().getSimpleName());
        }

        /** */
        private boolean isLinkIo(PageIO io) {
            return io instanceof InlineIO || io instanceof PendingRowIO || io instanceof RowLinkIO;
        }

        /** */
        private long getLink(BPlusLeafIO io, long addr, int idx) {
            if (io instanceof RowLinkIO)
                return ((RowLinkIO)io).getLink(addr, idx);
            if (io instanceof InlineIO)
                return ((InlineIO)io).link(addr, idx);
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

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
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.cache.query.index.IndexProcessor;
import org.apache.ignite.internal.cache.query.index.sorted.inline.io.InlineIO;
import org.apache.ignite.internal.commandline.CommandHandler;
import org.apache.ignite.internal.commandline.ProgressPrinter;
import org.apache.ignite.internal.commandline.StringBuilderOutputStream;
import org.apache.ignite.internal.commandline.argument.parser.CLIArgumentParser;
import org.apache.ignite.internal.commandline.systemview.SystemViewCommand;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.processors.cache.persistence.IndexStorageImpl;
import org.apache.ignite.internal.processors.cache.persistence.StorageException;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStore;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreV2;
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
import org.apache.ignite.internal.util.GridLongList;
import org.apache.ignite.internal.util.GridStringBuilder;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.lang.GridClosure3;
import org.apache.ignite.internal.util.lang.GridPlainClosure2;
import org.apache.ignite.internal.util.lang.IgnitePair;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
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
import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_PAGE_SIZE;
import static org.apache.ignite.internal.commandline.argument.parser.CLIArgument.mandatoryArg;
import static org.apache.ignite.internal.commandline.argument.parser.CLIArgument.optionalArg;
import static org.apache.ignite.internal.commandline.indexreader.IgniteIndexReader.Args.CHECK_PARTS;
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
import static org.apache.ignite.internal.visor.systemview.VisorSystemViewTask.SimpleType.NUMBER;
import static org.apache.ignite.internal.visor.systemview.VisorSystemViewTask.SimpleType.STRING;

/**
 * Offline reader for index files.
 */
public class IgniteIndexReader implements AutoCloseable {
    /** */
    public static final String META_TREE_NAME = "MetaTree";

    /** */
    public static final String RECURSIVE_TRAVERSE_NAME = "<RECURSIVE> ";

    /** */
    public static final String HORIZONTAL_SCAN_NAME = "<HORIZONTAL> ";

    /** */
    private static final String PAGE_LISTS_PREFIX = "<PAGE_LIST> ";

    /** */
    public static final String ERROR_PREFIX = "<ERROR> ";

    /** */
    private static final Pattern CACHE_TYPE_ID_SEARCH_PATTERN =
        Pattern.compile("(?<id>[-0-9]{1,15})_(?<typeId>[-0-9]{1,15})_.*");

    /** */
    private static final Pattern CACHE_ID_SEARCH_PATTERN =
        Pattern.compile("(?<id>[-0-9]{1,15})_.*");

    /** */
    private static final int CHECK_PARTS_MAX_ERRORS_PER_PARTITION = 10;

    /** */
    static {
        IndexProcessor.registerIO();
    }

    /** Page size. */
    private final int pageSize;

    /** Partition count. */
    private final int partCnt;

    /** Index name filter, if {@code null} then is not used. */
    @Nullable private final Predicate<String> idxFilter;

    /** Logger. */
    private final Logger log;

    /** Page store of {@link FilePageStoreManager#INDEX_FILE_NAME}. */
    @Nullable private final FilePageStore idxStore;

    /** Partitions page stores, may contains {@code null}. */
    @Nullable private final FilePageStore[] partStores;

    /** Check cache data tree in partition files and it's consistency with indexes. */
    private final boolean checkParts;

    /** */
    private final Set<Integer> missingPartitions = new HashSet<>();

    /** */
    private final TreePageVisitor innerPageVisitor = new InnerPageVisitor();

    /** */
    private final TreePageVisitor leafPageVisitor = new LeafPageVisitor();

    /** */
    private final TreePageVisitor metaPageVisitor = new MetaPageVisitor();

    /** */
    private final Map<String, IgnitePair<Integer>> cacheTypeIds = new HashMap<>();

    /**
     * Constructor.
     *
     * @param idxFilter Index name filter, if {@code null} then is not used.
     * @param checkParts Check cache data tree in partition files and it's consistency with indexes.
     * @param log Logger.
     * @throws IgniteCheckedException If failed.
     */
    public IgniteIndexReader(
        @Nullable Predicate<String> idxFilter,
        boolean checkParts,
        Logger log,
        IgniteIndexReaderFilePageStoreFactory filePageStoreFactory
    ) throws IgniteCheckedException {
        pageSize = filePageStoreFactory.pageSize();
        partCnt = filePageStoreFactory.partitionCount();
        this.checkParts = checkParts;
        this.idxFilter = idxFilter;

        this.log = log;

        Map<Integer, List<Throwable>> partStoresErrors = new HashMap<>();
        List<Throwable> errors = new ArrayList<>();

        idxStore = filePageStoreFactory.createFilePageStoreWithEnsure(INDEX_PARTITION, FLAG_IDX);

        if (!errors.isEmpty())
            partStoresErrors.put(INDEX_PARTITION, new ArrayList<>(errors));

        if (isNull(idxStore))
            throw new IgniteCheckedException(INDEX_FILE_NAME + " file not found");
        else
            log.info("Analyzing file: " + INDEX_FILE_NAME);

        partStores = new FilePageStore[partCnt];

        for (int i = 0; i < partCnt; i++) {
            if (!errors.isEmpty())
                errors.clear();

            partStores[i] = filePageStoreFactory.createFilePageStoreWithEnsure(i, FLAG_DATA);

            if (!errors.isEmpty())
                partStoresErrors.put(i, new ArrayList<>(errors));
        }

        printFileReadingErrors(partStoresErrors);
    }

    /** Read index file. */
    public void readIdx() throws IgniteCheckedException {
        log.info("Partitions files num: " + Arrays.stream(partStores).filter(Objects::nonNull).count());

        Map<Class<? extends PageIO>, Long> pageClasses = new HashMap<>();

        long pagesNum = isNull(idxStore) ? 0 : (idxStore.size() - idxStore.headerSize()) / pageSize;

        log.info("Going to check " + pagesNum + " pages.");

        long[] indexPartitionRoots = partitionRoots(PageIdAllocator.META_PAGE_ID);

        long metaTreeRootId = indexPartitionRoots[0];
        long pageListMetaPageId = indexPartitionRoots[1];

        Map<String, TreeTraverseContext> recursiveScans = traverseAllTrees(
            "Index trees traversal",
            metaTreeRootId,
            CountOnlyStorage::new,
            this::traverseTree
        );

        Map<String, TreeTraverseContext> horizontalScans = traverseAllTrees(
            "Scan index trees horizontally",
            metaTreeRootId,
            checkParts ? LinkStorage::new : CountOnlyStorage::new,
            this::horizontalTreeScan
        );

        Set<Long> pageIds = new HashSet<>();

        recursiveScans.forEach((name, info) -> {
            pageIds.addAll(info.innerPageIds);
            pageIds.add(info.rootPageId);
        });

        // Scanning page reuse lists.
        PageListsInfo pageListsInfo = pageListMetaPageId == 0 ? null : pageListInfo(pageListMetaPageId);

        ProgressPrinter progressPrinter = progressPrinter("Reading pages sequentially", pagesNum);

        // Scan all pages in file.
        List<Throwable> errors = scanFileStore(INDEX_PARTITION, FLAG_IDX, idxStore, (pageId, addr, io) -> {
            progressPrinter.printProgress();

            pageClasses.compute(io.getClass(), (k, v) -> v == null ? 1 : v + 1);

            if (idxFilter != null)
                return true;

            if (io instanceof PageMetaIO || io instanceof PagesListMetaIO)
                return true;

            if (!((io instanceof BPlusMetaIO || io instanceof BPlusInnerIO)))
                return true;

            if (pageIds.contains(pageId) || pageListsInfo == null || pageListsInfo.allPages.contains(pageId))
                return true;

            throw new IgniteException("Possibly orphan " + io.getClass().getSimpleName() + " page, pageId=" + pageId);
        });

        printTraversalResults(RECURSIVE_TRAVERSE_NAME, recursiveScans);

        printTraversalResults(HORIZONTAL_SCAN_NAME, horizontalScans);

        compareTraversals(recursiveScans, horizontalScans);

        if (pageListsInfo == null)
            log.severe("No page lists meta info found.");
        else
            printPagesListsInfo(pageListsInfo);

        printPageStat("", "---- These pages types were encountered during sequential scan:", pageClasses);

        if (!errors.isEmpty()) {
            log.severe("----");
            log.severe("Errors:");

            errors.forEach(e -> log.severe(e.getMessage()));
        }

        log.info("----");

        SystemViewCommand.printTable(
            null,
            Arrays.asList(STRING, NUMBER),
            Arrays.asList(
                Arrays.asList("Total pages encountered during sequential scan:", pageClasses.values().stream().mapToLong(a -> a).sum()),
                Arrays.asList("Total errors occurred during sequential scan: ", errors.size())
            ),
            log
        );

        if (idxFilter != null)
            log.info("Orphan pages were not reported due to --indexes filter.");

        log.info("Note that some pages can be occupied by meta info, tracking info, etc., so total page count can differ " +
            "from count of pages found in index trees and page lists.");

        if (checkParts) {
            Map<Integer, List<Throwable>> checkPartsErrors = checkParts(horizontalScans);

            log.info("");

            printErrors("",
                "Partitions check:",
                "Partitions check detected no errors.",
                "Errors detected in partition, partId=%s",
                checkPartsErrors
            );

            log.info("Partition check finished, total errors: " +
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
    private <T> T doWithBuffer(GridPlainClosure2<ByteBuffer, Long, T> c) throws IgniteCheckedException {
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
                    long pageId = pageId(partId, flag, i);

                    readPage(store, pageId, buf);

                    PageIO io = PageIO.getPageIO(addr);

                    if (!c.apply(pageId, addr, io))
                        break;
                }
                catch (Throwable e) {
                    errors.add(new IgniteException("Error [step=" + i + ", msg=" + e.getMessage() + ']', e));
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
    private Map<Integer, List<Throwable>> checkParts(Map<String, TreeTraverseContext> aTreesInfo) {
        log.info("");

        // Map partId -> errors.
        Map<Integer, List<Throwable>> res = new HashMap<>();

        Map<String, TreeTraverseContext> treesInfo = new HashMap<>(aTreesInfo);

        treesInfo.remove(META_TREE_NAME);

        ProgressPrinter progressPrinter = progressPrinter("Checking partitions", partCnt);

        for (int i = 0; i < partCnt; i++) {
            progressPrinter.printProgress();

            FilePageStore partStore = partStores[i];

            if (partStore == null)
                continue;

            List<Throwable> errors = new LinkedList<>();

            final int partId = i;

            try {
                long partMetaId = pageId(i, FLAG_DATA, 0);

                doWithBuffer((buf, addr) -> {
                    readPage(partStore, partMetaId, buf);

                    PagePartitionMetaIO partMetaIO = PageIO.getPageIO(addr);

                    long cacheDataTreeRoot = partMetaIO.getTreeRoot(addr);

                    TreeTraverseContext cacheDataTreeInfo =
                        horizontalTreeScan(cacheDataTreeRoot, "dataTree-" + partId, new ItemsListStorage<>());

                    for (Object dataTreeItem : cacheDataTreeInfo.itemStorage) {
                        CacheAwareLink cacheAwareLink = (CacheAwareLink)dataTreeItem;

                        for (Map.Entry<String, TreeTraverseContext> e : treesInfo.entrySet()) {
                            if (cacheAndTypeId(e.getKey()).get1() != cacheAwareLink.cacheId
                                || e.getValue().itemStorage.contains(cacheAwareLink))
                                continue;

                            errors.add(new IgniteException(cacheDataTreeEntryMissingError(e.getKey(), cacheAwareLink)));
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

    /**
     * Compares result of traversals.
     *
     * @param treeInfos Traversal from root to leafs.
     * @param treeScans Traversal using horizontal scan.
     */
    private void compareTraversals(Map<String, TreeTraverseContext> treeInfos, Map<String, TreeTraverseContext> treeScans) {
        List<String> errors = new LinkedList<>();

        Set<String> treeIdxNames = new HashSet<>();

        treeInfos.forEach((name, tree) -> {
            treeIdxNames.add(name);

            TreeTraverseContext ctx = treeScans.get(name);

            if (ctx == null) {
                errors.add("Tree was detected in " + RECURSIVE_TRAVERSE_NAME + " but absent in  "
                    + HORIZONTAL_SCAN_NAME + ": " + name);

                return;
            }

            if (tree.itemStorage.size() != ctx.itemStorage.size())
                errors.add(compareError("items", name, tree.itemStorage.size(), ctx.itemStorage.size(), null));

            Set<Class<? extends PageIO>> classesInStat = new HashSet<>();

            tree.ioStat.forEach((cls, cnt) -> {
                classesInStat.add(cls);

                long scanCnt = ctx.ioStat.getOrDefault(cls, 0L);

                if (scanCnt != cnt)
                    errors.add(compareError("pages", name, cnt, scanCnt, cls));
            });

            ctx.ioStat.forEach((cls, cnt) -> {
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

        errors.forEach(log::severe);

        log.info("Comparing traversals detected " + errors.size() + " errors.");
        log.info("------------------");
    }

    /**
     * Gets info about page lists.
     *
     * @param metaPageListId Page list meta id.
     * @return Page list info.
     */
    private PageListsInfo pageListInfo(long metaPageListId) throws IgniteCheckedException {
        return doWithBuffer((buf, addr) -> {
            Map<IgniteBiTuple<Long, Integer>, List<Long>> bucketsData = new HashMap<>();

            Set<Long> allPages = new HashSet<>();

            Map<Class<? extends PageIO>, Long> pageListStat = new HashMap<>();

            Map<Long, List<Throwable>> errors = new HashMap<>();

            long currPageId = metaPageListId;

            while (currPageId != 0) {
                try {
                    buf.rewind();

                    readPage(idxStore, currPageId, buf);

                    PagesListMetaIO io = PageIO.getPageIO(addr);

                    Map<Integer, GridLongList> data = new HashMap<>();

                    io.getBucketsData(addr, data);

                    for (Map.Entry<Integer, GridLongList> e : data.entrySet()) {
                        List<Long> listIds = LongStream.of(e.getValue().array())
                            .map(IgniteIndexReader::normalizePageId)
                            .boxed()
                            .collect(toList());

                        for (Long listId : listIds) {
                            try {
                                allPages.addAll(pageList(listId, pageListStat));
                            }
                            catch (Exception err) {
                                errors.put(listId, singletonList(err));
                            }
                        }

                        bucketsData.put(new IgniteBiTuple<>(currPageId, e.getKey()), listIds);
                    }

                    currPageId = io.getNextMetaPageId(addr);
                }
                catch (Exception e) {
                    errors.put(currPageId, singletonList(e));

                    break;
                }
            }

            return new PageListsInfo(bucketsData, allPages, pageListStat, errors);
        });
    }

    /**
     * Get single page list.
     *
     * @param pageListStartId Id of the start page of the page list.
     * @param pageStat Page types statistics.
     * @return List of page ids.
     */
    private List<Long> pageList(long pageListStartId, Map<Class<? extends PageIO>, Long> pageStat) {
        ByteBuffer nodeBuf = allocateBuffer(pageSize);
        ByteBuffer pageBuf = allocateBuffer(pageSize);

        long nodeAddr = bufferAddress(nodeBuf);
        long pageAddr = bufferAddress(pageBuf);

        try {
            List<Long> res = new LinkedList<>();

            long currPageId = pageListStartId;

            while (currPageId != 0) {
                nodeBuf.rewind();

                readPage(idxStore, currPageId, nodeBuf);

                PagesListNodeIO io = PageIO.getPageIO(nodeAddr);

                for (int i = 0; i < io.getCount(nodeAddr); i++) {
                    pageBuf.rewind();

                    long pageId = normalizePageId(io.getAt(nodeAddr, i));

                    res.add(pageId);

                    readPage(idxStore, pageId, pageBuf);

                    pageStat.compute(PageIO.getPageIO(pageAddr).getClass(), (k, v) -> v == null ? 1 : v + 1);
                }

                currPageId = io.getNextId(nodeAddr);
            }

            return res;
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
        finally {
            freeBuffer(nodeBuf);
            freeBuffer(pageBuf);
        }
    }

    /** Traverse all trees in file and return their info. */
    private Map<String, TreeTraverseContext> traverseAllTrees(
        String traverseProcCaption,
        long metaTreeRoot,
        Supplier<ItemStorage> itemStorageFactory,
        TraverseProc traverseProc
    ) {
        Map<String, TreeTraverseContext> treeInfos = new LinkedHashMap<>();

        TreeTraverseContext metaTreeCtx =
            traverseProc.traverse(metaTreeRoot, META_TREE_NAME, new ItemsListStorage<IndexStorageImpl.IndexItem>());

        treeInfos.put(META_TREE_NAME, metaTreeCtx);

        ProgressPrinter progressPrinter = progressPrinter(traverseProcCaption, metaTreeCtx.itemStorage.size());

        metaTreeCtx.itemStorage.forEach(item -> {
            progressPrinter.printProgress();

            IndexStorageImpl.IndexItem idxItem = (IndexStorageImpl.IndexItem)item;

            if (nonNull(idxFilter) && !idxFilter.test(idxItem.nameString()))
                return;

            TreeTraverseContext treeTraversalInfo =
                traverseProc.traverse(normalizePageId(idxItem.pageId()), idxItem.nameString(), itemStorageFactory.get());

            treeInfos.put(idxItem.toString(), treeTraversalInfo);
        });

        return treeInfos;
    }

    /** */
    ProgressPrinter progressPrinter(String caption, long total) {
        return new ProgressPrinter(System.out, caption, total);
    }

    /** */
    private String cacheDataTreeEntryMissingError(String treeName, CacheAwareLink cacheAwareLink) {
        long pageId = pageId(cacheAwareLink.link);

        return "Entry is missing in index[" + treeName +
            "cacheId=" + cacheAwareLink.cacheId +
            ", partId=" + partId(pageId) +
            ", pageIndex=" + pageIndex(pageId) +
            ", itemId=" + itemId(cacheAwareLink.link) +
            ", link=" + cacheAwareLink.link + ']';
    }

    /** */
    private String compareError(String itemName, String idxName, long fromRoot, long scan, Class<? extends PageIO> pageType) {
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

    /** Prints traversal info. */
    private void printTraversalResults(String prefix, Map<String, TreeTraverseContext> treeInfos) {
        log.info(prefix + "Tree traversal results");

        Map<Class<? extends PageIO>, Long> totalStat = new HashMap<>();

        int totalErr = 0;

        // Map (cacheId, typeId) -> (map idxName -> size))
        Map<IgnitePair<Integer>, Map<String, Long>> cacheIdxSizes = new HashMap<>();

        for (Map.Entry<String, TreeTraverseContext> e : treeInfos.entrySet()) {
            String idxName = e.getKey();
            TreeTraverseContext validationInfo = e.getValue();

            log.info(prefix + "-----");
            log.info(prefix + "Index tree: " + idxName);
            printPageStat(prefix, "---- Page stat:", validationInfo.ioStat);

            validationInfo.ioStat.forEach((cls, cnt) -> totalStat.compute(cls, (k, v) -> v == null ? cnt : v + cnt));

            log.info(prefix + "---- Count of items found in leaf pages: " + validationInfo.itemStorage.size());

            printErrors(
                prefix,
                "Errors:",
                "No errors occurred while traversing.",
                "Page id=%s, exceptions:",
                validationInfo.errors
            );

            totalErr += validationInfo.errors.size();

            cacheIdxSizes
                .computeIfAbsent(cacheAndTypeId(idxName), k -> new HashMap<>())
                .put(idxName, validationInfo.itemStorage.size());
        }

        log.info(prefix + "----");

        printPageStat(prefix, "Total page stat collected during trees traversal:", totalStat);

        log.info("");

        boolean sizeConsistencyErrorsFound = false;

        for (Map.Entry<IgnitePair<Integer>, Map<String, Long>> entry : cacheIdxSizes.entrySet()) {
            IgnitePair<Integer> cacheTypeId = entry.getKey();
            Map<String, Long> idxSizes = entry.getValue();

            if (idxSizes.values().stream().distinct().count() > 1) {
                sizeConsistencyErrorsFound = true;

                totalErr++;

                log.severe("Index size inconsistency: cacheId=" + cacheTypeId.get1() + ", typeId=" + cacheTypeId.get2());

                idxSizes.forEach((name, size) -> log.severe("     Index name: " + name + ", size=" + size));
            }
        }

        if (!sizeConsistencyErrorsFound)
            log.info(prefix + "No index size consistency errors found.");

        log.info("");

        SystemViewCommand.printTable(
            null,
            Arrays.asList(STRING, NUMBER),
            Arrays.asList(
                Arrays.asList(prefix + "Total trees: ", treeInfos.keySet().size()),
                Arrays.asList(prefix + "Total pages found in trees: ", totalStat.values().stream().mapToLong(a -> a).sum()),
                Arrays.asList(prefix + "Total errors during trees traversal: ", totalErr)
            ),
            log
        );

        log.info("");
        log.info("------------------");
    }

    /**
     * Tries to get cache id and type id from index tree name.
     *
     * @param name Index name.
     * @return Pair of cache id and type id.
     */
    public IgnitePair<Integer> cacheAndTypeId(String name) {
        return cacheTypeIds.computeIfAbsent(name, k -> {
            Matcher mId = CACHE_TYPE_ID_SEARCH_PATTERN.matcher(k);

            if (mId.find())
                return new IgnitePair<>(parseInt(mId.group("id")), parseInt(mId.group("typeId")));

            Matcher cId = CACHE_ID_SEARCH_PATTERN.matcher(k);

            if (cId.find())
                return new IgnitePair<>(parseInt(cId.group("id")), 0);

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

        log.info(prefix + "Page lists info.");

        if (!pageListsInfo.bucketsData.isEmpty())
            log.info(prefix + "---- Printing buckets data:");

        pageListsInfo.bucketsData.forEach((bucket, bucketData) -> {
            GridStringBuilder sb = new GridStringBuilder(prefix)
                .a("List meta id=")
                .a(bucket.get1())
                .a(", bucket number=")
                .a(bucket.get2())
                .a(", lists=[")
                .a(bucketData.stream().map(String::valueOf).collect(joining(", ")))
                .a("]");

            log.info(sb.toString());
        });

        printPageStat(prefix, "---- Page stat:", pageListsInfo.pageListStat);

        printErrors(prefix, "---- Errors:", "---- No errors.", "Page id: %s, exception: ", pageListsInfo.errors);

        log.info("");

        SystemViewCommand.printTable(
            null,
            Arrays.asList(STRING, NUMBER),
            Arrays.asList(
                Arrays.asList(prefix + "Total index pages found in lists:", pageListsInfo.allPages.size()),
                Arrays.asList(prefix + "Total errors during lists scan:", pageListsInfo.errors.size())
            ),
            log
        );

        log.info("------------------");
    }

    /**
     * Traverse single index tree from root to leafs.
     *
     * @param rootPageId Root page id.
     * @param treeName Tree name.
     * @param itemStorage Items storage.
     * @return Tree traversal context.
     */
    TreeTraverseContext traverseTree(
        long rootPageId,
        String treeName,
        ItemStorage itemStorage
    ) {
        Set<Long> innerPageIds = new HashSet<>();

        TreeTraverseContext ctx = createContext(rootPageId, treeName, itemStorage, innerPageIds);

        traverse(rootPageId, ctx);

        return ctx;
    }

    /**
     * Traverse single index tree by each level horizontally.
     *
     * @param rootPageId Root page id.
     * @param treeName Tree name.
     * @param itemStorage Items storage.
     * @return Tree traversal context.
     */
    private TreeTraverseContext horizontalTreeScan(
        long rootPageId,
        String treeName,
        ItemStorage itemStorage
    ) {
        TreeTraverseContext ctx = createContext(rootPageId, treeName, itemStorage, null);

        ByteBuffer buf = allocateBuffer(pageSize);

        try {
            long addr = bufferAddress(buf);

            readPage(ctx.store, rootPageId, buf);

            PageIO pageIO = PageIO.getPageIO(addr);

            if (!(pageIO instanceof BPlusMetaIO))
                throw new IgniteException("Root page is not meta, pageId=" + rootPageId);

            BPlusMetaIO metaIO = (BPlusMetaIO)pageIO;

            ctx.onPageIO(metaIO);

            int lvlsCnt = metaIO.getLevelsCount(addr);

            long[] firstPageIds = IntStream.range(0, lvlsCnt).mapToLong(i -> metaIO.getFirstPageId(addr, i)).toArray();

            for (int i = 0; i < lvlsCnt; i++) {
                long pageId = firstPageIds[i];

                while (pageId > 0) {
                    try {
                        buf.rewind();

                        readPage(ctx.store, pageId, buf);

                        pageIO = PageIO.getPageIO(addr);

                        if (i == 0 && !(pageIO instanceof BPlusLeafIO))
                            throw new IgniteException("Not-leaf page found on leaf level [pageId=" + pageId + ", level=0]");

                        if (!(pageIO instanceof BPlusIO))
                            throw new IgniteException("Not-BPlus page found [pageId=" + pageId + ", level=" + i + ']');

                        ctx.onPageIO(pageIO);

                        if (pageIO instanceof BPlusLeafIO)
                            pageVisitor(pageIO).visit(addr, ctx);

                        pageId = ((BPlusIO<?>)pageIO).getForward(addr);
                    }
                    catch (Throwable e) {
                        ctx.errors.computeIfAbsent(pageId, k -> new LinkedList<>()).add(e);

                        pageId = 0;
                    }
                }
            }
        }
        catch (Throwable e) {
            ctx.errors.computeIfAbsent(rootPageId, k -> new LinkedList<>()).add(e);
        }
        finally {
            freeBuffer(buf);
        }

        return ctx;
    }

    /** */
    TreeTraverseContext createContext(long rootPageId, String treeName, ItemStorage itemStorage, Set<Long> innerPageIds) {
        return new TreeTraverseContext(
            rootPageId,
            treeName,
            filePageStore(partId(rootPageId)),
            itemStorage,
            innerPageIds
        );
    }

    /**
     * @param partId Partition id.
     * @return File page store of given partition.
     */
    FilePageStore filePageStore(int partId) {
        return partId == INDEX_PARTITION ? idxStore : partStores[partId];
    }

    /**
     * Gets tree node and all its children.
     *
     * @param pageId Page id, where tree node is located.
     * @param ctx Tree traverse context.
     */
    private void traverse(long pageId, TreeTraverseContext ctx) {
        try {
            final ByteBuffer buf = allocateBuffer(pageSize);

            try {
                readPage(ctx.store, pageId, buf);

                final long addr = bufferAddress(buf);

                final PageIO io = PageIO.getPageIO(addr);

                ctx.onPageIO(io);

                pageVisitor(io).visit(addr, ctx);
            }
            finally {
                freeBuffer(buf);
            }
        }
        catch (Throwable e) {
            ctx.errors.computeIfAbsent(pageId, k -> new LinkedList<>()).add(e);
        }
    }

    /** */
    private TreePageVisitor pageVisitor(PageIO io) {
        if (io instanceof BPlusLeafIO)
            return leafPageVisitor;
        else if (io instanceof BPlusInnerIO)
            return innerPageVisitor;
        else if (io instanceof BPlusMetaIO)
            return metaPageVisitor;
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

        CLIArgumentParser p = new CLIArgumentParser(asList(
            mandatoryArg(
                DIR.arg(),
                "partition directory, where " + INDEX_FILE_NAME + " and (optionally) partition files are located.",
                String.class
            ),
            optionalArg(PART_CNT.arg(), "full partitions count in cache group.", Integer.class, () -> 0),
            optionalArg(PAGE_SIZE.arg(), "page size.", Integer.class, () -> DFLT_PAGE_SIZE),
            optionalArg(PAGE_STORE_VER.arg(), "page store version.", Integer.class, () -> FilePageStoreV2.VERSION),
            optionalArg(INDEXES.arg(), "you can specify index tree names that will be processed, separated by comma " +
                "without spaces, other index trees will be skipped.", String[].class, () -> U.EMPTY_STRS),
            optionalArg(CHECK_PARTS.arg(),
                "check cache data tree in partition files and it's consistency with indexes.", Boolean.class, () -> false)
        ));

        if (args.length == 0) {
            System.out.println(p.usage());

            return;
        }

        p.parse(asList(args).iterator());

        IgniteIndexReaderFilePageStoreFactory filePageStoreFactory = new IgniteIndexReaderFilePageStoreFactory(
            new File(p.<String>get(DIR.arg())),
            p.get(PAGE_SIZE.arg()),
            p.get(PART_CNT.arg()),
            p.get(PAGE_STORE_VER.arg())
        );

        Set<String> idxs = new HashSet<>(asList(p.get(INDEXES.arg())));

        try (IgniteIndexReader reader = new IgniteIndexReader(
            idxs.isEmpty() ? null : idxs::contains,
            p.get(CHECK_PARTS.arg()),
            CommandHandler.setupJavaLogger("index-reader", IgniteIndexReader.class),
            filePageStoreFactory
        )) {
            reader.readIdx();
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(INDEX_FILE_NAME + " scan problem", e);
        }
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
    long[] partitionRoots(long pageMetaPageId) throws IgniteCheckedException {
        return doWithBuffer((buf, addr) -> {
            readPage(filePageStore(partId(pageMetaPageId)), pageMetaPageId, buf);

            PageMetaIO pageMetaIO = PageIO.getPageIO(addr);

            return new long[] {
                normalizePageId(pageMetaIO.getTreeRoot(addr)),
                normalizePageId(pageMetaIO.getReuseListRoot(addr))
            };
        });
    }

    /** */
    private void printErrors(
        String prefix,
        String caption,
        @Nullable String alternativeCaption,
        String elementFormatPtrn,
        Map<?, ? extends List<? extends Throwable>> errors
    ) {
        if (errors.isEmpty() && alternativeCaption != null) {
            log.info(prefix + alternativeCaption);

            return;
        }

        if (caption != null)
            log.info(prefix + ERROR_PREFIX + caption);

        errors.forEach((k, v) -> {
            log.info(prefix + ERROR_PREFIX + format(elementFormatPtrn, k.toString()));

            v.forEach(e -> log.severe(e.getMessage()));
        });
    }

    /** */
    private void printPageStat(String prefix, String caption, Map<Class<? extends PageIO>, Long> stat) {
        if (caption != null)
            log.info(prefix + caption + (stat.isEmpty() ? " empty" : ""));

        if (stat.isEmpty())
            return;

        List<List<?>> data = new ArrayList<>(stat.size());

        stat.forEach((cls, cnt) -> data.add(Arrays.asList(prefix + cls.getSimpleName(), cnt)));

        SystemViewCommand.printTable(
            null,
            Arrays.asList(STRING, NUMBER),
            data,
            log
        );
    }

    /** */
    private void printStackTrace(Throwable e) {
        OutputStream os = new StringBuilderOutputStream();

        e.printStackTrace(new PrintStream(os));

        log.info(os.toString());
    }

    /**
     * Print partitions reading exceptions.
     *
     * @param partStoresErrors Partitions reading exceptions.
     */
    private void printFileReadingErrors(Map<Integer, List<Throwable>> partStoresErrors) {
        List<Throwable> idxPartErrors = partStoresErrors.get(INDEX_PARTITION);

        if (!F.isEmpty(idxPartErrors)) {
            log.severe("Errors detected while reading " + INDEX_FILE_NAME);

            idxPartErrors.forEach(err -> log.severe(err.getMessage()));

            partStoresErrors.remove(INDEX_PARTITION);
        }

        if (!partStoresErrors.isEmpty())
            printErrors("", "Errors detected while reading partition files:", null, "Partition id: %s, exceptions: ", partStoresErrors);
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
        CHECK_PARTS("--check-parts");

        /** */
        private final String arg;

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
    private interface TraverseProc {
        /** */
        TreeTraverseContext traverse(long rootId, String treeName, ItemStorage itemStorage);
    }

    /**
     * Processor for page IOs.
     */
    private interface TreePageVisitor {
        /**
         * Traverse tree.
         * @param addr Page address.
         * @param ctx Tree traversal context.
         */
        void visit(long addr, TreeTraverseContext ctx) throws IgniteCheckedException;
    }

    /** */
    private class MetaPageVisitor implements TreePageVisitor {
        /** {@inheritDoc} */
        @Override public void visit(long addr, TreeTraverseContext ctx) throws IgniteCheckedException {
            BPlusMetaIO io = PageIO.getPageIO(addr);

            int rootLvl = io.getRootLevel(addr);

            traverse(io.getFirstPageId(addr, rootLvl), ctx);
        }
    }

    /** */
    private class InnerPageVisitor implements TreePageVisitor {
        /** {@inheritDoc} */
        @Override public void visit(long addr, TreeTraverseContext ctx) throws IgniteCheckedException {
            PageIO io = PageIO.getPageIO(addr);

            for (long id : children(io, addr))
                IgniteIndexReader.this.traverse(id, ctx);

            ctx.onInnerPage(PageIO.getPageId(addr));
        }

        /** */
        private long[] children(PageIO io, long addr) {
            BPlusInnerIO<?> innerIo = (BPlusInnerIO<?>)io;

            int cnt = innerIo.getCount(addr);

            if (cnt == 0) {
                long left = innerIo.getLeft(addr, 0);

                return left == 0 ? IgniteUtils.EMPTY_LONGS : new long[] {left};
            }

            long[] children = new long[cnt + 1];

            for (int i = 0; i < cnt; i++)
                children[i] = innerIo.getLeft(addr, i);

            children[cnt] = innerIo.getRight(addr, cnt - 1);

            return children;
        }
    }

    /** */
    private class LeafPageVisitor implements TreePageVisitor {
        /** {@inheritDoc} */
        @Override public void visit(long addr, TreeTraverseContext ctx) throws IgniteCheckedException {
            ctx.onLeafPage(PageIO.getPageId(addr), data(addr, ctx));
        }

        /** */
        private List<Object> data(long addr, TreeTraverseContext ctx) throws IgniteCheckedException {
            List<Object> items = new LinkedList<>();

            BPlusLeafIO<?> io = PageIO.getPageIO(addr);

            try {
                for (int i = 0; i < io.getCount(addr); i++) {
                    if (io instanceof IndexStorageImpl.MetaStoreLeafIO)
                        items.add(((BPlusIO<IndexStorageImpl.IndexItem>)io).getLookupRow(null, addr, i));
                    else
                        items.add(leafItem(io, addr, i, ctx));
                }
            }
            catch (Exception e) {
                ctx.errors.computeIfAbsent(PageIO.getPageId(addr), k -> new LinkedList<>()).add(e);
            }

            return items;
        }

        /** */
        private Object leafItem(BPlusLeafIO<?> io, long addr, int idx, TreeTraverseContext ctx) {
            if (!(io instanceof InlineIO || io instanceof PendingRowIO || io instanceof RowLinkIO))
                throw new IgniteException("Unexpected page io: " + io.getClass().getSimpleName());

            final long link = link(io, addr, idx);

            final int cacheId;

            if (io instanceof AbstractDataLeafIO && ((AbstractDataLeafIO)io).storeCacheId())
                cacheId = ((RowLinkIO)io).getCacheId(addr, idx);
            else
                cacheId = cacheAndTypeId(ctx.treeName).get1();

            if (partCnt == 0)
                return new CacheAwareLink(cacheId, link);

            try {
                long linkedPageId = pageId(link);

                int linkedPagePartId = partId(linkedPageId);

                if (missingPartitions.contains(linkedPagePartId))
                    return new CacheAwareLink(cacheId, link); // just skip

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
                        AbstractDataPageIO<?> dataPageIO = (AbstractDataPageIO<?>)dataIo;

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
                ctx.errors.computeIfAbsent(PageIO.getPageId(addr), k -> new LinkedList<>()).add(e);
            }

            return new CacheAwareLink(cacheId, link);
        }

        /** */
        private long link(BPlusLeafIO<?> io, long addr, int idx) {
            if (io instanceof RowLinkIO)
                return ((RowLinkIO)io).getLink(addr, idx);
            else if (io instanceof InlineIO)
                return ((InlineIO)io).link(addr, idx);
            else if (io instanceof PendingRowIO)
                return ((PendingRowIO)io).getLink(addr, idx);
            else
                throw new IgniteException("No link to data page on idx=" + idx);
        }
    }
}

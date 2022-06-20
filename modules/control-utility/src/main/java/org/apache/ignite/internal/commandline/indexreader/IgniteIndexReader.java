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
import org.apache.ignite.internal.util.lang.GridPlainClosure2;
import org.apache.ignite.internal.util.lang.IgnitePair;
import org.apache.ignite.internal.util.lang.RunnableX;
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
import static org.apache.ignite.internal.pagemem.PageIdAllocator.FLAG_DATA;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.FLAG_IDX;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.INDEX_PARTITION;
import static org.apache.ignite.internal.pagemem.PageIdUtils.flag;
import static org.apache.ignite.internal.pagemem.PageIdUtils.itemId;
import static org.apache.ignite.internal.pagemem.PageIdUtils.pageId;
import static org.apache.ignite.internal.pagemem.PageIdUtils.pageIndex;
import static org.apache.ignite.internal.pagemem.PageIdUtils.partId;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.INDEX_FILE_NAME;
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
    private static final String DIR_ARG = "--dir";

    /** */
    private static final String PART_CNT_ARG = "--part-cnt";

    /** */
    private static final String PAGE_SIZE_ARG = "--page-size";

    /** */
    private static final String PAGE_STORE_VER_ARG = "--page-store-ver";

    /** */
    private static final String INDEXES_ARG = "--indexes";

    /** */
    private static final String CHECK_PARTS_ARG = "--check-parts";

    /** */
    private static final Pattern CACHE_TYPE_ID_SEARCH_PATTERN =
        Pattern.compile("(?<id>[-0-9]{1,15})_(?<typeId>[-0-9]{1,15})_.*");

    /** */
    private static final Pattern CACHE_ID_SEARCH_PATTERN =
        Pattern.compile("(?<id>[-0-9]{1,15})_.*");

    /** */
    private static final int CHECK_PARTS_MAX_ERRORS_PER_PARTITION = 10;

    static {
        IndexProcessor.registerIO();
    }

    /** Page size. */
    private final int pageSize;

    /** Partition count. */
    private final int partCnt;

    /** Check cache data tree in partition files and it's consistency with indexes. */
    private final boolean checkParts;

    /** Index name filter, if {@code null} then is not used. */
    @Nullable private final Predicate<String> idxFilter;

    /** Logger. */
    private final Logger log;

    /** Page store of {@link FilePageStoreManager#INDEX_FILE_NAME}. */
    private final FilePageStore idxStore;

    /** Partitions page stores, may contains {@code null}. */
    private final FilePageStore[] partStores;

    /** */
    private final Set<Integer> missingPartitions = new HashSet<>();

    /** */
    private final Set<Long> pageIds = new HashSet<>();

    /** */
    private final InnerPageVisitor innerPageVisitor = new InnerPageVisitor();

    /** */
    private final LeafPageVisitor leafPageVisitor = new LeafPageVisitor();

    /** */
    private final MetaPageVisitor metaPageVisitor = new MetaPageVisitor();

    /** */
    private final LevelsPageVisitor levelsPageVisitor = new LevelsPageVisitor();

    /** */
    private final Map<String, IgnitePair<Integer>> cacheTypeIds = new HashMap<>();

    /**
     * Constructor.
     *
     * @param idxFilter Index name filter, if {@code null} then is not used.
     * @param checkParts Check cache data tree in partition files and it's consistency with indexes.
     * @param filePageStoreFactory File page store factory.
     * @param log Logger.
     * @throws IgniteCheckedException If failed.
     */
    public IgniteIndexReader(
        @Nullable Predicate<String> idxFilter,
        boolean checkParts,
        IgniteIndexReaderFilePageStoreFactory filePageStoreFactory,
        Logger log
    ) throws IgniteCheckedException {
        pageSize = filePageStoreFactory.pageSize();
        partCnt = filePageStoreFactory.partitionCount();
        this.checkParts = checkParts;
        this.idxFilter = idxFilter;
        this.log = log;
        idxStore = filePageStoreFactory.createFilePageStore(INDEX_PARTITION, FLAG_IDX);

        if (isNull(idxStore))
            throw new IgniteCheckedException(INDEX_FILE_NAME + " file not found");

        log.info("Analyzing file: " + INDEX_FILE_NAME);

        partStores = new FilePageStore[partCnt];

        for (int i = 0; i < partCnt; i++)
            partStores[i] = filePageStoreFactory.createFilePageStore(i, FLAG_DATA);
    }

    /**
     * Entry point.
     *
     * @param args Arguments.
     */
    public static void main(String[] args) {
        System.out.println("THIS UTILITY MUST BE LAUNCHED ON PERSISTENT STORE WHICH IS NOT UNDER RUNNING GRID!");

        CLIArgumentParser p = new CLIArgumentParser(asList(
            mandatoryArg(
                DIR_ARG,
                "partition directory, where " + INDEX_FILE_NAME + " and (optionally) partition files are located.",
                String.class
            ),
            optionalArg(PART_CNT_ARG, "full partitions count in cache group.", Integer.class, () -> 0),
            optionalArg(PAGE_SIZE_ARG, "page size.", Integer.class, () -> DFLT_PAGE_SIZE),
            optionalArg(PAGE_STORE_VER_ARG, "page store version.", Integer.class, () -> FilePageStoreV2.VERSION),
            optionalArg(INDEXES_ARG, "you can specify index tree names that will be processed, separated by comma " +
                "without spaces, other index trees will be skipped.", String[].class, () -> U.EMPTY_STRS),
            optionalArg(CHECK_PARTS_ARG,
                "check cache data tree in partition files and it's consistency with indexes.", Boolean.class, () -> false)
        ));

        if (args.length == 0) {
            System.out.println(p.usage());

            return;
        }

        p.parse(asList(args).iterator());

        IgniteIndexReaderFilePageStoreFactory filePageStoreFactory = new IgniteIndexReaderFilePageStoreFactory(
            new File(p.<String>get(DIR_ARG)),
            p.get(PAGE_SIZE_ARG),
            p.get(PART_CNT_ARG),
            p.get(PAGE_STORE_VER_ARG)
        );

        Set<String> idxs = new HashSet<>(asList(p.get(INDEXES_ARG)));

        try (IgniteIndexReader reader = new IgniteIndexReader(
            idxs.isEmpty() ? null : idxs::contains,
            p.get(CHECK_PARTS_ARG),
            filePageStoreFactory,
            CommandHandler.setupJavaLogger("index-reader", IgniteIndexReader.class)
        )) {
            reader.readIndex();
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(INDEX_FILE_NAME + " scan problem", e);
        }
    }

    /** Read index file. */
    public void readIndex() throws IgniteCheckedException {
        log.info("Partitions files num: " + Arrays.stream(partStores).filter(Objects::nonNull).count());
        log.info("Going to check " + ((idxStore.size() - idxStore.headerSize()) / pageSize) + " pages.");

        long[] indexPartitionRoots = partitionRoots(PageIdAllocator.META_PAGE_ID);

        Map<String, ScanContext> recursiveScans = scanAllTrees(
            "Index trees traversal",
            indexPartitionRoots[0],
            CountOnlyStorage::new,
            this::recursiveTreeScan
        );

        Map<String, ScanContext> horizontalScans = scanAllTrees(
            "Scan index trees horizontally",
            indexPartitionRoots[0],
            checkParts ? LinkStorage::new : CountOnlyStorage::new,
            this::horizontalTreeScan
        );

        printScanResults(RECURSIVE_TRAVERSE_NAME, recursiveScans);
        printScanResults(HORIZONTAL_SCAN_NAME, horizontalScans);

        compareScans(recursiveScans, horizontalScans);

        printPagesListsInfo(indexPartitionRoots[1]);

        printSequentialScanInfo(scanIndexSequentially());

        if (checkParts)
            printCheckPartsInfo(checkParts(horizontalScans));
    }

    /** Traverse all trees in file and return their info. */
    private Map<String, ScanContext> scanAllTrees(
        String caption,
        long metaTreeRoot,
        Supplier<ItemStorage> itemStorageFactory,
        Scanner scanner
    ) {
        Map<String, ScanContext> ctxs = new LinkedHashMap<>();

        ScanContext metaTreeCtx = scanner.scan(metaTreeRoot, META_TREE_NAME, new ItemsListStorage<IndexStorageImpl.IndexItem>());

        ctxs.put(META_TREE_NAME, metaTreeCtx);

        ProgressPrinter progressPrinter = createProgressPrinter(caption, metaTreeCtx.items.size());

        ((ItemsListStorage<IndexStorageImpl.IndexItem>)metaTreeCtx.items).forEach(item -> {
            progressPrinter.printProgress();

            if (nonNull(idxFilter) && !idxFilter.test(item.nameString()))
                return;

            ScanContext ctx =
                scanner.scan(normalizePageId(item.pageId()), item.nameString(), itemStorageFactory.get());

            ctxs.put(item.toString(), ctx);
        });

        return ctxs;
    }

    /**
     * Traverse single index tree from root to leafs.
     *
     * @param rootPageId Root page id.
     * @param idx Index name.
     * @param items Items storage.
     * @return Tree traversal context.
     */
    ScanContext recursiveTreeScan(long rootPageId, String idx, ItemStorage items) {
        pageIds.add(normalizePageId(rootPageId));

        ScanContext ctx = createContext(cacheAndTypeId(idx).get1(), filePageStore(rootPageId), items);

        metaPageVisitor.readAndVisit(rootPageId, ctx);

        return ctx;
    }

    /**
     * Traverse single index tree by each level horizontally.
     *
     * @param rootPageId Root page id.
     * @param idx Index name.
     * @param items Items storage.
     * @return Tree traversal context.
     */
    private ScanContext horizontalTreeScan(long rootPageId, String idx, ItemStorage items) {
        pageIds.add(normalizePageId(rootPageId));

        ScanContext ctx = createContext(cacheAndTypeId(idx).get1(), filePageStore(rootPageId), items);

        levelsPageVisitor.readAndVisit(rootPageId, ctx);

        return ctx;
    }

    /**
     * Gets info about page lists.
     *
     * @param metaPageListId Page list meta id.
     * @return Page list info.
     */
    private PageListsInfo pageListsInfo(long metaPageListId) throws IgniteCheckedException {
        return doWithBuffer((buf, addr) -> {
            Map<IgniteBiTuple<Long, Integer>, List<Long>> bucketsData = new HashMap<>();

            Map<Class<? extends PageIO>, Long> ioStat = new HashMap<>();

            Map<Long, List<String>> errors = new HashMap<>();

            long pagesCnt = 0;
            long currMetaPageId = metaPageListId;

            while (currMetaPageId != 0) {
                try {
                    PagesListMetaIO io = readPage(idxStore, currMetaPageId, buf);

                    Map<Integer, GridLongList> data = new HashMap<>();

                    io.getBucketsData(addr, data);

                    for (Map.Entry<Integer, GridLongList> e : data.entrySet()) {
                        List<Long> listIds = LongStream.of(e.getValue().array())
                            .map(IgniteIndexReader::normalizePageId)
                            .boxed()
                            .collect(toList());

                        for (Long listId : listIds) {
                            try {
                                pagesCnt += visitPageList(listId, ioStat);
                            }
                            catch (Exception err) {
                                errors.put(listId, singletonList(err.getMessage()));
                            }
                        }

                        bucketsData.put(F.t(currMetaPageId, e.getKey()), listIds);
                    }

                    currMetaPageId = io.getNextMetaPageId(addr);
                }
                catch (Exception e) {
                    errors.put(currMetaPageId, singletonList(e.getMessage()));

                    break;
                }
            }

            return new PageListsInfo(bucketsData, pagesCnt, ioStat, errors);
        });
    }

    /**
     * Visit single page list.
     *
     * @param listStartPageId Id of the start page of the page list.
     * @param ioStat Page types statistics.
     * @return List of page ids.
     */
    private long visitPageList(long listStartPageId, Map<Class<? extends PageIO>, Long> ioStat) throws IgniteCheckedException {
        return doWithBuffer((nodeBuf, nodeAddr) -> doWithBuffer((pageBuf, pageAddr) -> {
            long res = 0;

            long currPageId = listStartPageId;

            while (currPageId != 0) {
                PagesListNodeIO io = readPage(idxStore, currPageId, nodeBuf);

                for (int i = 0; i < io.getCount(nodeAddr); i++) {
                    long pageId = normalizePageId(io.getAt(nodeAddr, i));

                    res++;

                    pageIds.add(normalizePageId(pageId));

                    ScanContext.onPageIO(readPage(idxStore, pageId, pageBuf).getClass(), ioStat, 1);
                }

                currPageId = io.getNextId(nodeAddr);
            }

            return res;
        }));
    }

    /**
     * Traverse index file sequentially.
     *
     * @return Traverse results.
     * @throws IgniteCheckedException If failed.
     */
    private ScanContext scanIndexSequentially() throws IgniteCheckedException {
        long pagesNum = (idxStore.size() - idxStore.headerSize()) / pageSize;

        ProgressPrinter progressPrinter = createProgressPrinter("Reading pages sequentially", pagesNum);

        ScanContext ctx = createContext(-1, idxStore, new CountOnlyStorage());

        doWithBuffer((buf, addr) -> {
            for (int i = 0; i < pagesNum; i++) {
                long pageId = -1;

                try {
                    pageId = pageId(INDEX_PARTITION, FLAG_IDX, i);

                    PageIO io = readPage(idxStore, pageId, buf);

                    ctx.onPageIO(io);

                    progressPrinter.printProgress();

                    if (idxFilter != null)
                        continue;

                    if (io instanceof PageMetaIO || io instanceof PagesListMetaIO)
                        continue;

                    if (!((io instanceof BPlusMetaIO || io instanceof BPlusInnerIO)))
                        continue;

                    if (pageIds.contains(normalizePageId(pageId)))
                        continue;

                    ctx.errors.put(pageId, Collections.singletonList("Error [step=" + i +
                        ", msg=Possibly orphan " + io.getClass().getSimpleName() + " page" +
                        ", pageId=" + pageId + ']'));
                }
                catch (Throwable e) {
                    ctx.errors.put(
                        pageId,
                        Collections.singletonList("Error [step=" + i + ", msg=" + e.getMessage() + ']')
                    );
                }
            }

            return null;
        });

        return ctx;
    }

    /**
     * Checks partitions, comparing partition indexes (cache data tree) to indexes given in {@code treesInfo}.
     *
     * @param treesInfo Index trees info to compare cache data tree with.
     * @return Map of errors, bound to partition id.
     */
    private Map<Integer, List<String>> checkParts(Map<String, ScanContext> treesInfo) {
        log.info("");

        // Map partId -> errors.
        Map<Integer, List<String>> res = new HashMap<>();

        ProgressPrinter progressPrinter = createProgressPrinter("Checking partitions", partCnt);

        IntStream.range(0, partCnt).forEach(partId -> {
            progressPrinter.printProgress();

            FilePageStore partStore = partStores[partId];

            if (partStore == null)
                return;

            List<String> errors = new LinkedList<>();

            try {
                long partMetaId = pageId(partId, FLAG_DATA, 0);

                doWithBuffer((buf, addr) -> {
                    PagePartitionMetaIO partMetaIO = readPage(partStore, partMetaId, buf);

                    long cacheDataTreeRoot = partMetaIO.getTreeRoot(addr);

                    ScanContext ctx =
                        horizontalTreeScan(cacheDataTreeRoot, "dataTree-" + partId, new ItemsListStorage<>());

                    for (Object dataTreeItem : ctx.items) {
                        CacheAwareLink cacheAwareLink = (CacheAwareLink)dataTreeItem;

                        for (Map.Entry<String, ScanContext> e : treesInfo.entrySet()) {
                            if (e.getKey().equals(META_TREE_NAME))
                                continue;

                            if (cacheAndTypeId(e.getKey()).get1() != cacheAwareLink.cacheId
                                || e.getValue().items.contains(cacheAwareLink))
                                continue;

                            long pageId = pageId(cacheAwareLink.link);

                            errors.add("Entry is missing in index[name=" + e.getKey() +
                                "cacheId=" + cacheAwareLink.cacheId +
                                ", partId=" + partId(pageId) +
                                ", pageIndex=" + pageIndex(pageId) +
                                ", itemId=" + itemId(cacheAwareLink.link) +
                                ", link=" + cacheAwareLink.link + ']');
                        }

                        if (errors.size() >= CHECK_PARTS_MAX_ERRORS_PER_PARTITION) {
                            errors.add("Too many errors (" + CHECK_PARTS_MAX_ERRORS_PER_PARTITION +
                                ") found for partId=" + partId + ", stopping analysis for this partition.");

                            break;
                        }
                    }

                    return null;
                });
            }
            catch (IgniteCheckedException e) {
                errors.add("Partition check failed, partId=" + partId);
            }

            if (!errors.isEmpty())
                res.put(partId, errors);
        });

        return res;
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

    /** */
    private void doWithoutErrors(RunnableX x, ScanContext ctx, long pageId) {
        try {
            x.run();
        }
        catch (Throwable e) {
            ctx.errors.computeIfAbsent(pageId, k -> new LinkedList<>()).add(e.getMessage());
        }
    }

    /**
     * Tries to get cache id and type id from index name.
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

    /** */
    ScanContext createContext(int cacheId, FilePageStore store, ItemStorage items) {
        return new ScanContext(cacheId, store, items);
    }

    /** */
    ProgressPrinter createProgressPrinter(String caption, long total) {
        return new ProgressPrinter(System.out, caption, total);
    }

    /**
     * @param rootPageId Root page id.
     * @return File page store of given partition.
     */
    FilePageStore filePageStore(long rootPageId) {
        int partId = partId(rootPageId);

        return partId == INDEX_PARTITION ? idxStore : partStores[partId];
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
    private <I extends PageIO> I readPage(FilePageStore store, long pageId, ByteBuffer buf) throws IgniteCheckedException {
        try {
            store.read(pageId, (ByteBuffer)buf.rewind(), false);

            return PageIO.getPageIO(bufferAddress(buf));
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
    long[] partitionRoots(long metaPageId) throws IgniteCheckedException {
        return doWithBuffer((buf, addr) -> {
            PageMetaIO pageMetaIO = readPage(filePageStore(metaPageId), metaPageId, buf);

            return new long[] {
                normalizePageId(pageMetaIO.getTreeRoot(addr)),
                normalizePageId(pageMetaIO.getReuseListRoot(addr))
            };
        });
    }

    /**
     * Compares result of traversals.
     *
     * @param recursiveScans Traversal from root to leafs.
     * @param horizontalScans Traversal using horizontal scan.
     */
    private void compareScans(
        Map<String, ScanContext> recursiveScans,
        Map<String, ScanContext> horizontalScans
    ) {
        List<String> errors = new LinkedList<>();

        recursiveScans.forEach((name, rctx) -> {
            ScanContext hctx = horizontalScans.get(name);

            if (hctx == null) {
                errors.add("Tree was detected in " + RECURSIVE_TRAVERSE_NAME + " but absent in  "
                    + HORIZONTAL_SCAN_NAME + ": " + name);

                return;
            }

            if (rctx.items.size() != hctx.items.size())
                errors.add(compareError("items", name, rctx.items.size(), hctx.items.size(), null));

            rctx.ioStat.forEach((cls, cnt) -> {
                long scanCnt = hctx.ioStat.getOrDefault(cls, 0L);

                if (scanCnt != cnt)
                    errors.add(compareError("pages", name, cnt, scanCnt, cls));
            });

            hctx.ioStat.forEach((cls, cnt) -> {
                if (!rctx.ioStat.containsKey(cls))
                    errors.add(compareError("pages", name, 0, cnt, cls));
            });
        });

        horizontalScans.forEach((name, hctx) -> {
            if (!recursiveScans.containsKey(name))
                errors.add("Tree was detected in " + HORIZONTAL_SCAN_NAME + " but absent in  "
                    + RECURSIVE_TRAVERSE_NAME + ": " + name);
        });

        errors.forEach(log::severe);

        log.info("Comparing traversals detected " + errors.size() + " errors.");
        log.info("------------------");
    }

    /** Prints sequential file scan results. */
    private void printSequentialScanInfo(ScanContext ctx) {
        printPageStat("", "---- These pages types were encountered during sequential scan:", ctx.ioStat);

        if (!ctx.errors.isEmpty()) {
            log.severe("----");
            log.severe("Errors:");

            ctx.errors.values().forEach(e -> log.severe(e.get(0)));
        }

        log.info("----");

        SystemViewCommand.printTable(
            null,
            Arrays.asList(STRING, NUMBER),
            Arrays.asList(
                Arrays.asList("Total pages encountered during sequential scan:", ctx.ioStat.values().stream().mapToLong(a -> a).sum()),
                Arrays.asList("Total errors occurred during sequential scan: ", ctx.errors.size())
            ),
            log
        );

        if (idxFilter != null)
            log.info("Orphan pages were not reported due to --indexes filter.");

        log.info("Note that some pages can be occupied by meta info, tracking info, etc., so total page count can differ " +
            "from count of pages found in index trees and page lists.");
    }

    /** Print check partitions info. */
    private void printCheckPartsInfo(Map<Integer, List<String>> checkPartsErrors) {
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

    /** Prints traversal info. */
    private void printScanResults(String prefix, Map<String, ScanContext> ctxs) {
        log.info(prefix + "Tree traversal results");

        Map<Class<? extends PageIO>, Long> ioStat = new HashMap<>();

        int totalErr = 0;

        // Map (cacheId, typeId) -> (map idxName -> size))
        Map<IgnitePair<Integer>, Map<String, Long>> cacheIdxSizes = new HashMap<>();

        for (Map.Entry<String, ScanContext> e : ctxs.entrySet()) {
            String idxName = e.getKey();
            ScanContext ctx = e.getValue();

            log.info(prefix + "-----");
            log.info(prefix + "Index tree: " + idxName);
            printPageStat(prefix, "---- Page stat:", ctx.ioStat);

            ctx.ioStat.forEach((cls, cnt) -> ScanContext.onPageIO(cls, ioStat, cnt));

            log.info(prefix + "---- Count of items found in leaf pages: " + ctx.items.size());

            printErrors(
                prefix,
                "Errors:",
                "No errors occurred while traversing.",
                "Page id=%s, exceptions:",
                ctx.errors
            );

            totalErr += ctx.errors.size();

            cacheIdxSizes
                .computeIfAbsent(cacheAndTypeId(idxName), k -> new HashMap<>())
                .put(idxName, ctx.items.size());
        }

        log.info(prefix + "----");

        printPageStat(prefix, "Total page stat collected during trees traversal:", ioStat);

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
                Arrays.asList(prefix + "Total trees: ", ctxs.keySet().size()),
                Arrays.asList(prefix + "Total pages found in trees: ", ioStat.values().stream().mapToLong(a -> a).sum()),
                Arrays.asList(prefix + "Total errors during trees traversal: ", totalErr)
            ),
            log
        );

        log.info("");
        log.info("------------------");
    }

    /**
     * Prints page lists info.
     *
     * @param reuseListRoot Page id.
     */
    private void printPagesListsInfo(long reuseListRoot) throws IgniteCheckedException {
        if (reuseListRoot == 0) {
            log.severe("No page lists meta info found.");

            return;
        }

        PageListsInfo pageListsInfo = pageListsInfo(reuseListRoot);

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

        printPageStat(prefix, "---- Page stat:", pageListsInfo.ioStat);

        printErrors(prefix, "---- Errors:", "---- No errors.", "Page id: %s, exception: ", pageListsInfo.errors);

        log.info("");

        SystemViewCommand.printTable(
            null,
            Arrays.asList(STRING, NUMBER),
            Arrays.asList(
                Arrays.asList(prefix + "Total index pages found in lists:", pageListsInfo.pagesCnt),
                Arrays.asList(prefix + "Total errors during lists scan:", pageListsInfo.errors.size())
            ),
            log
        );

        log.info("------------------");
    }

    /** */
    private String compareError(String itemName, String idxName, long fromRoot, long scan, Class<? extends PageIO> io) {
        return format(
            "Different count of %s; index: %s, %s:%s, %s:%s" + (io == null ? "" : ", pageType: " + io.getSimpleName()),
            itemName,
            idxName,
            RECURSIVE_TRAVERSE_NAME,
            fromRoot,
            HORIZONTAL_SCAN_NAME,
            scan
        );
    }

    /** */
    private void printErrors(String prefix, String caption, String emptyCaption, String msg, Map<?, List<String>> errors) {
        if (errors.isEmpty()) {
            log.info(prefix + emptyCaption);

            return;
        }

        log.info(prefix + ERROR_PREFIX + caption);

        errors.forEach((k, v) -> {
            log.info(prefix + ERROR_PREFIX + format(msg, k.toString()));

            v.forEach(log::severe);
        });
    }

    /** */
    private void printPageStat(String prefix, String caption, Map<Class<? extends PageIO>, Long> ioStat) {
        if (caption != null)
            log.info(prefix + caption + (ioStat.isEmpty() ? " empty" : ""));

        if (ioStat.isEmpty())
            return;

        List<List<?>> data = new ArrayList<>(ioStat.size());

        ioStat.forEach((cls, cnt) -> data.add(Arrays.asList(prefix + cls.getSimpleName(), cnt)));

        SystemViewCommand.printTable(
            null,
            Arrays.asList(STRING, NUMBER),
            data,
            log
        );
    }

    /** {@inheritDoc} */
    @Override public void close() throws StorageException {
        idxStore.stop(false);

        for (FilePageStore store : partStores) {
            if (nonNull(store))
                store.stop(false);
        }
    }

    /** */
    private interface Scanner {
        /** */
        ScanContext scan(long rootId, String idx, ItemStorage items);
    }

    /** Processor for page IOs. */
    private abstract class TreePageVisitor {
        /** */
        protected void readAndVisit(long pageId, ScanContext ctx) {
            doWithoutErrors(() -> doWithBuffer((buf, addr) -> {
                final PageIO io = readPage(ctx, pageId, buf);

                if (io instanceof BPlusLeafIO)
                    leafPageVisitor.visit(addr, ctx);
                else if (io instanceof BPlusInnerIO)
                    innerPageVisitor.visit(addr, ctx);
                else
                    throw new IllegalArgumentException("Unknown io [io=" + io.getClass().getSimpleName() + ']');

                return null;
            }), ctx, pageId);
        }

        /** */
        protected <I extends PageIO> I readPage(ScanContext ctx, long pageId, ByteBuffer buf) throws IgniteCheckedException {
            final I io = IgniteIndexReader.this.readPage(ctx.store, pageId, buf);

            ctx.onPageIO(io);

            return io;
        }
    }

    /** */
    private class MetaPageVisitor extends TreePageVisitor {
        /** {@inheritDoc} */
        @Override protected void readAndVisit(long pageId, ScanContext ctx) {
            doWithoutErrors(() -> {
                long firstPageId = doWithBuffer((buf, addr) -> {
                    BPlusMetaIO io = readPage(ctx, pageId, buf);

                    return io.getFirstPageId(addr, io.getRootLevel(addr));
                });

                super.readAndVisit(firstPageId, ctx);
            }, ctx, pageId);
        }
    }

    /** */
    private class LevelsPageVisitor extends TreePageVisitor {
        /** {@inheritDoc} */
        @Override protected void readAndVisit(long rootPageId, ScanContext ctx) {
            doWithoutErrors(() -> doWithBuffer((buf, addr) -> {
                PageIO pageIO = readPage(ctx, rootPageId, buf);

                if (!(pageIO instanceof BPlusMetaIO))
                    throw new IgniteException("Root page is not meta, pageId=" + rootPageId);

                BPlusMetaIO metaIO = (BPlusMetaIO)pageIO;

                int lvlsCnt = metaIO.getLevelsCount(addr);

                long[] firstPageIds = IntStream.range(0, lvlsCnt).mapToLong(i -> metaIO.getFirstPageId(addr, i)).toArray();

                for (int i = 0; i < lvlsCnt; i++) {
                    long pageId = firstPageIds[i];

                    try {
                        while (pageId > 0) {
                            pageIO = readPage(ctx, pageId, buf);

                            if (i == 0 && !(pageIO instanceof BPlusLeafIO))
                                throw new IgniteException("Not-leaf page found on leaf level [pageId=" + pageId + ", level=0]");

                            if (!(pageIO instanceof BPlusIO))
                                throw new IgniteException("Not-BPlus page found [pageId=" + pageId + ", level=" + i + ']');

                            if (pageIO instanceof BPlusLeafIO)
                                leafPageVisitor.visit(addr, ctx);

                            pageId = ((BPlusIO<?>)pageIO).getForward(addr);
                        }
                    }
                    catch (Throwable e) {
                        ctx.errors.computeIfAbsent(pageId, k -> new LinkedList<>()).add(e.getMessage());
                    }
                }

                return null;
            }), ctx, rootPageId);
        }
    }

    /** */
    private class InnerPageVisitor extends TreePageVisitor {
        /** */
        public void visit(long addr, ScanContext ctx) throws IgniteCheckedException {
            BPlusInnerIO<?> io = PageIO.getPageIO(addr);

            pageIds.add(normalizePageId(PageIO.getPageId(addr)));

            for (long id : children(io, addr))
                readAndVisit(id, ctx);
        }

        /** */
        private long[] children(BPlusInnerIO<?> io, long addr) {
            int cnt = io.getCount(addr);

            if (cnt == 0) {
                long left = io.getLeft(addr, 0);

                return left == 0 ? U.EMPTY_LONGS : new long[] {left};
            }

            long[] children = new long[cnt + 1];

            for (int i = 0; i < cnt; i++)
                children[i] = io.getLeft(addr, i);

            children[cnt] = io.getRight(addr, cnt - 1);

            return children;
        }
    }

    /** */
    private class LeafPageVisitor extends TreePageVisitor {
        /** */
        public void visit(long addr, ScanContext ctx) throws IgniteCheckedException {
            List<Object> items = new LinkedList<>();

            BPlusLeafIO<?> io = PageIO.getPageIO(addr);

            doWithoutErrors(() -> {
                for (int i = 0; i < io.getCount(addr); i++) {
                    if (io instanceof IndexStorageImpl.MetaStoreLeafIO)
                        items.add(((BPlusIO<IndexStorageImpl.IndexItem>)io).getLookupRow(null, addr, i));
                    else
                        items.add(leafItem(io, addr, i, ctx));
                }
            }, ctx, PageIO.getPageId(addr));

            ctx.onLeafPage(PageIO.getPageId(addr), items);
        }

        /** */
        private Object leafItem(BPlusLeafIO<?> io, long addr, int idx, ScanContext ctx) {
            if (!(io instanceof InlineIO || io instanceof PendingRowIO || io instanceof RowLinkIO))
                throw new IgniteException("Unexpected page io: " + io.getClass().getSimpleName());

            final long link = link(io, addr, idx);

            final int cacheId = (io instanceof AbstractDataLeafIO && ((AbstractDataLeafIO)io).storeCacheId())
                ? ((RowLinkIO)io).getCacheId(addr, idx)
                : ctx.cacheId;

            if (partCnt == 0)
                return new CacheAwareLink(cacheId, link);

            long linkedPageId = pageId(link);

            int linkedPagePartId = partId(linkedPageId);

            if (missingPartitions.contains(linkedPagePartId))
                return new CacheAwareLink(cacheId, link); // just skip

            doWithoutErrors(() -> {
                if (linkedPagePartId > partStores.length - 1) {
                    missingPartitions.add(linkedPagePartId);

                    throw new IgniteException("Calculated data page partition id exceeds given partitions " +
                        "count: " + linkedPagePartId + ", partCnt=" + partCnt);
                }

                if (partStores[linkedPagePartId] == null) {
                    missingPartitions.add(linkedPagePartId);

                    throw new IgniteException("Corresponding store wasn't found for partId=" +
                        linkedPagePartId + ". Does partition file exist?");
                }

                doWithBuffer((dataBuf, dataBufAddr) -> {
                    PageIO dataIo = IgniteIndexReader.this.readPage(partStores[linkedPagePartId], linkedPageId, dataBuf);

                    if (dataIo instanceof AbstractDataPageIO) {
                        DataPagePayload payload = ((AbstractDataPageIO<?>)dataIo).readPayload(dataBufAddr, itemId(link), pageSize);

                        if (payload.offset() <= 0 || payload.payloadSize() <= 0) {
                            throw new IgniteException(new GridStringBuilder("Invalid data page payload: ")
                                .a("off=").a(payload.offset())
                                .a(", size=").a(payload.payloadSize())
                                .a(", nextLink=").a(payload.nextLink())
                                .toString());
                        }
                    }

                    return null;
                });
            }, ctx, PageIO.getPageId(addr));

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

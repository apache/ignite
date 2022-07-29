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
import java.io.ObjectInputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.ObjLongConsumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.internal.cache.query.index.IndexProcessor;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyTypeSettings;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexKeyTypeRegistry;
import org.apache.ignite.internal.cache.query.index.sorted.inline.io.AbstractInlineLeafIO;
import org.apache.ignite.internal.cache.query.index.sorted.inline.io.InlineIO;
import org.apache.ignite.internal.cache.query.index.sorted.inline.types.NullableInlineIndexKeyType;
import org.apache.ignite.internal.commandline.CommandHandler;
import org.apache.ignite.internal.commandline.ProgressPrinter;
import org.apache.ignite.internal.commandline.argument.parser.CLIArgumentParser;
import org.apache.ignite.internal.commandline.indexreader.ScanContext.PagesStatistic;
import org.apache.ignite.internal.commandline.systemview.SystemViewCommand;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.processors.cache.StoredCacheData;
import org.apache.ignite.internal.processors.cache.persistence.IndexStorageImpl;
import org.apache.ignite.internal.processors.cache.persistence.StorageException;
import org.apache.ignite.internal.processors.cache.persistence.file.AsyncFileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStore;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreV2;
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
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PagePartitionMetaIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.TrackingPageIO;
import org.apache.ignite.internal.processors.cache.persistence.wal.crc.IgniteDataIntegrityViolationException;
import org.apache.ignite.internal.processors.cache.tree.AbstractDataLeafIO;
import org.apache.ignite.internal.processors.cache.tree.PendingRowIO;
import org.apache.ignite.internal.processors.cache.tree.RowLinkIO;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.util.GridLongList;
import org.apache.ignite.internal.util.GridStringBuilder;
import org.apache.ignite.internal.util.lang.GridPlainClosure2;
import org.apache.ignite.internal.util.lang.GridTuple3;
import org.apache.ignite.internal.util.lang.IgnitePair;
import org.apache.ignite.internal.util.lang.RunnableX;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.Nullable;

import static java.lang.Integer.parseInt;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static java.util.logging.Level.WARNING;
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
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.CACHE_DATA_FILENAME;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.INDEX_FILE_NAME;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.PART_FILE_TEMPLATE;
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
    private static final Pattern CACHE_TYPE_ID_INDEX_SEARCH_PATTERN =
        Pattern.compile("(?<id>[-0-9]{1,15})_(?<typeId>[-0-9]{1,15})_(?<indexName>.*)##.*");

    /** */
    private static final Pattern CACHE_TYPE_ID_SEARCH_PATTERN =
        Pattern.compile("(?<id>[-0-9]{1,15})_(?<typeId>[-0-9]{1,15})_.*");

    /** */
    private static final Pattern CACHE_ID_SEARCH_PATTERN =
        Pattern.compile("(?<id>[-0-9]{1,15})_.*");

    /** */
    private static final int MAX_ERRORS_CNT = 10;

    /** */
    static final int UNKNOWN_CACHE = -1;

    static {
        IndexProcessor.registerIO();
    }

    /** Directory with data(partitions and index). */
    private final File root;

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
    private final Map<Integer, StoredCacheData> storedCacheData = new HashMap<>();

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
    private final Map<String, GridTuple3<Integer, Integer, String>> cacheTypeIds = new HashMap<>();

    /**
     * Constructor.
     *
     * @param pageSize Page size.
     * @param partCnt Page count.
     * @param filePageStoreVer Version of file page store.
     * @param checkParts Check cache data tree in partition files and it's consistency with indexes.
     * @param root Root directory.
     * @param idxFilter Index name filter, if {@code null} then is not used.
     * @param log Logger.
     * @throws IgniteCheckedException If failed.
     */
    public IgniteIndexReader(
        int pageSize,
        int partCnt,
        int filePageStoreVer,
        File root,
        @Nullable Predicate<String> idxFilter,
        boolean checkParts,
        Logger log
    ) throws IgniteCheckedException {
        this.pageSize = pageSize;
        this.partCnt = partCnt;
        this.root = root;
        this.checkParts = checkParts;
        this.idxFilter = idxFilter;
        this.log = log;

        FileVersionCheckingFactory storeFactory = new FileVersionCheckingFactory(
            new AsyncFileIOFactory(),
            new AsyncFileIOFactory(),
            () -> pageSize
        ) {
            /** {@inheritDoc} */
            @Override public int latestVersion() {
                return filePageStoreVer;
            }
        };

        idxStore = filePageStore(INDEX_PARTITION, FLAG_IDX, storeFactory);

        if (isNull(idxStore))
            throw new IgniteCheckedException(INDEX_FILE_NAME + " file not found");

        log.info("Analyzing file: " + INDEX_FILE_NAME);

        partStores = new FilePageStore[partCnt];

        for (int i = 0; i < partCnt; i++)
            partStores[i] = filePageStore(i, FLAG_DATA, storeFactory);

        Arrays.stream(root.listFiles(f -> f.getName().endsWith(CACHE_DATA_FILENAME))).forEach(f -> {
            try (ObjectInputStream stream = new ObjectInputStream(Files.newInputStream(f.toPath()))) {
                StoredCacheData data = (StoredCacheData)stream.readObject();

                storedCacheData.put(CU.cacheId(data.config().getName()), data);
            }
            catch (ClassNotFoundException | IOException e) {
                log.log(WARNING, "Can't read stored cache data. Inline for this cache will not be analyzed [f=" + f.getName() + ']', e);
            }
        });
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

        Set<String> idxs = new HashSet<>(asList(p.get(INDEXES_ARG)));

        try (IgniteIndexReader reader = new IgniteIndexReader(
            p.get(PAGE_SIZE_ARG),
            p.get(PART_CNT_ARG),
            p.get(PAGE_STORE_VER_ARG),
            new File(p.<String>get(DIR_ARG)),
            idxs.isEmpty() ? null : idxs::contains,
            p.get(CHECK_PARTS_ARG),
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
            checkParts(horizontalScans);
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
        ScanContext ctx = createContext(idx, filePageStore(rootPageId), items);

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
        ScanContext ctx = createContext(idx, filePageStore(rootPageId), items);

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

            Map<Class<? extends PageIO>, PagesStatistic> stats = new HashMap<>();

            long pagesCnt = 0;
            long currMetaPageId = metaPageListId;

            ObjLongConsumer<String> onError = errorHandler(PAGE_LISTS_PREFIX);

            while (currMetaPageId != 0) {
                try {
                    PagesListMetaIO io = readPage(idxStore, currMetaPageId, buf);

                    ScanContext.addToStats(io, stats, 1, addr, idxStore.getPageSize());

                    Map<Integer, GridLongList> data = new HashMap<>();

                    io.getBucketsData(addr, data);

                    for (Map.Entry<Integer, GridLongList> e : data.entrySet()) {
                        List<Long> listIds = LongStream.of(e.getValue().array())
                            .map(IgniteIndexReader::normalizePageId)
                            .boxed()
                            .collect(toList());

                        for (Long listId : listIds) {
                            try {
                                pagesCnt += visitPageList(listId, stats);
                            }
                            catch (Exception err) {
                                onError.accept(err.getMessage(), listId);
                            }
                        }

                        bucketsData.put(F.t(currMetaPageId, e.getKey()), listIds);
                    }

                    currMetaPageId = io.getNextMetaPageId(addr);
                }
                catch (Exception err) {
                    onError.accept(err.getMessage(), currMetaPageId);

                    break;
                }
            }

            return new PageListsInfo(bucketsData, pagesCnt, stats, new HashMap<>());
        });
    }

    /**
     * Visit single page list.
     *
     * @param listStartPageId Id of the start page of the page list.
     * @param stats Page types statistics.
     * @return List of page ids.
     */
    private long visitPageList(long listStartPageId, Map<Class<? extends PageIO>, PagesStatistic> stats) throws IgniteCheckedException {
        return doWithBuffer((nodeBuf, nodeAddr) -> doWithBuffer((pageBuf, pageAddr) -> {
            long res = 0;

            long currPageId = listStartPageId;

            while (currPageId != 0) {
                PagesListNodeIO io = readPage(idxStore, currPageId, nodeBuf);

                ScanContext.addToStats(io, stats, 1, nodeAddr, idxStore.getPageSize());

                ScanContext.addToStats(readPage(idxStore, currPageId, pageBuf), stats, 1, pageAddr, idxStore.getPageSize());

                res += io.getCount(nodeAddr);

                for (int i = 0; i < io.getCount(nodeAddr); i++) {
                    long pageId = normalizePageId(io.getAt(nodeAddr, i));

                    ScanContext.addToStats(readPage(idxStore, pageId, pageBuf), stats, 1, pageAddr, idxStore.getPageSize());
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

        ScanContext ctx = createContext(null, idxStore, new CountOnlyStorage());

        ObjLongConsumer<String> onError = errorHandler("");

        doWithBuffer((buf, addr) -> {
            for (int i = 0; i < pagesNum; i++) {
                long pageId = -1;

                try {
                    pageId = pageId(INDEX_PARTITION, FLAG_IDX, i);

                    PageIO io = readPage(ctx, pageId, buf, false);

                    progressPrinter.printProgress();

                    if (idxFilter != null)
                        continue;

                    if (io instanceof TrackingPageIO)
                        continue;

                    if (pageIds.contains(normalizePageId(pageId)))
                        continue;

                    ctx.errCnt++;

                    onError.accept("Error [step=" + i +
                            ", msg=Possibly orphan " + io.getClass().getSimpleName() + " page" +
                            ", pageId=" + normalizePageId(pageId) + ']', pageId);
                }
                catch (Throwable e) {
                    ctx.errCnt++;

                    onError.accept("Error [step=" + i + ", msg=" + e.getMessage() + ']', pageId);
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
    private void checkParts(Map<String, ScanContext> treesInfo) {
        log.info("");

        AtomicInteger partWithErrs = new AtomicInteger();
        AtomicInteger errSum = new AtomicInteger();

        ProgressPrinter progressPrinter = createProgressPrinter("Checking partitions", partCnt);

        IntStream.range(0, partCnt).forEach(partId -> {
            progressPrinter.printProgress();

            FilePageStore partStore = partStores[partId];

            if (partStore == null)
                return;

            AtomicInteger errCnt = new AtomicInteger();

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

                            errCnt.getAndIncrement();
                            log.severe(ERROR_PREFIX + "Entry is missing in index[name=" + e.getKey() +
                                "cacheId=" + cacheAwareLink.cacheId +
                                ", partId=" + partId(pageId) +
                                ", pageIndex=" + pageIndex(pageId) +
                                ", itemId=" + itemId(cacheAwareLink.link) +
                                ", link=" + cacheAwareLink.link + ']');
                        }

                        if (errCnt.get() >= MAX_ERRORS_CNT) {
                            log.severe(ERROR_PREFIX + "Too many errors (" + MAX_ERRORS_CNT +
                                ") found for partId=" + partId + ", stopping analysis for this partition.");

                            break;
                        }
                    }

                    return null;
                });
            }
            catch (IgniteCheckedException e) {
                log.severe(ERROR_PREFIX + "Partition check failed [partId=" + partId + ']');
            }

            if (errCnt.get() != 0) {
                partWithErrs.getAndIncrement();
                errSum.addAndGet(errCnt.get());
            }
        });

        if (errSum.get() == 0)
            log.info("Partitions check detected no errors.");

        log.info("Partition check finished, total errors: " + errSum.get() +
                ", total problem partitions: " + partWithErrs.get());
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
    public GridTuple3<Integer, Integer, String> cacheAndTypeId(String name) {
        return cacheTypeIds.computeIfAbsent(name, k -> {
            Matcher xId = CACHE_TYPE_ID_INDEX_SEARCH_PATTERN.matcher(k);

            if (xId.find())
                return new GridTuple3<>(parseInt(xId.group("id")), parseInt(xId.group("typeId")), xId.group("indexName"));

            Matcher mId = CACHE_TYPE_ID_SEARCH_PATTERN.matcher(k);

            if (mId.find())
                return new GridTuple3<>(parseInt(mId.group("id")), parseInt(mId.group("typeId")), null);

            Matcher cId = CACHE_ID_SEARCH_PATTERN.matcher(k);

            if (cId.find())
                return new GridTuple3<>(parseInt(cId.group("id")), 0, null);

            return new GridTuple3<>(0, 0, null);
        });
    }

    /** */
    ScanContext createContext(String idxName, FilePageStore store, ItemStorage items) {
        GridTuple3<Integer, Integer, String> parsed;

        if (idxName != null)
            parsed = cacheAndTypeId(idxName);
        else
            parsed = new GridTuple3<>(UNKNOWN_CACHE, 0, null);

        return new ScanContext(parsed.get1(), inlineFieldsCount(parsed), store, items);
    }

    /**
     * Search index definition inside cache query entities.
     *
     * @param parsed Parsed index name.
     * @return Count of inlined fields or {@code 0} if index definition not found.
     * @see QueryEntity
     */
    protected int inlineFieldsCount(GridTuple3<Integer, Integer, String> parsed) {
        if (parsed.get1() == UNKNOWN_CACHE || !storedCacheData.containsKey(parsed.get1()))
            return 0;

        StoredCacheData data = storedCacheData.get(parsed.get1());

        if (Objects.equals(QueryUtils.PRIMARY_KEY_INDEX, parsed.get3())) {
            if (data.queryEntities().size() > 1) {
                log.warning("Can't parse inline for PK index when multiple query entities defined for a cache " +
                    "[idx=" + parsed.get3() + ']');

                return 0;
            }

            QueryEntity qe = data.queryEntities().iterator().next();

            return qe.getKeyFields() == null ? 1 : qe.getKeyFields().size();
        }

        QueryIndex idx = null;

        for (QueryEntity qe : data.queryEntities()) {
            for (QueryIndex idx0 : qe.getIndexes()) {
                if (Objects.equals(idx0.getName(), parsed.get3())) {
                    idx = idx0;

                    break;
                }
            }

            if (idx != null)
                break;
        }

        if (idx == null) {
            log.warning("Can't find index definition. Inline information not available [idx=" + parsed.get3() + ']');

            return 0;
        }

        return idx.getFields().size();
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

    /** */
    static long normalizePageId(long pageId) {
        return pageId(partId(pageId), flag(pageId), pageIndex(pageId));
    }

    /** */
    private <I extends PageIO> I readPage(FilePageStore store, long pageId, ByteBuffer buf) throws IgniteCheckedException {
        return readPage(store, pageId, buf, true);
    }

    /**
     * Reads pages into buffer.
     *
     * @param store Source for reading pages.
     * @param pageId Page ID.
     * @param buf Buffer.
     * @param addToPageIds If {@code true} then add page ID to global set.
     */
    private <I extends PageIO> I readPage(
        FilePageStore store,
        long pageId,
        ByteBuffer buf,
        boolean addToPageIds
    ) throws IgniteCheckedException {
        try {
            store.read(pageId, (ByteBuffer)buf.rewind(), false);

            long addr = bufferAddress(buf);

            if (store == idxStore && addToPageIds)
                pageIds.add(normalizePageId(pageId));

            I io = PageIO.getPageIO(addr);

            return io;
        }
        catch (IgniteDataIntegrityViolationException | IllegalArgumentException e) {
            // Replacing exception due to security reasons, as IgniteDataIntegrityViolationException prints page content.
            // Catch IllegalArgumentException for output page information.
            throw new IgniteException("Failed to read page, id=" + pageId + ", idx=" + pageIndex(pageId) +
                ", file=" + store.getFileAbsolutePath());
        }
    }

    /** */
    protected <I extends PageIO> I readPage(ScanContext ctx, long pageId, ByteBuffer buf) throws IgniteCheckedException {
        return readPage(ctx, pageId, buf, true);
    }

    /** */
    protected <I extends PageIO> I readPage(
        ScanContext ctx,
        long pageId,
        ByteBuffer buf,
        boolean addToPageIds
    ) throws IgniteCheckedException {
        final I io = readPage(ctx.store, pageId, buf, addToPageIds);

        ctx.addToStats(io, bufferAddress(buf));

        return io;
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
        AtomicInteger errCnt = new AtomicInteger();

        recursiveScans.forEach((name, rctx) -> {
            ScanContext hctx = horizontalScans.get(name);

            if (hctx == null) {
                errCnt.incrementAndGet();
                log.severe("Tree was detected in " + RECURSIVE_TRAVERSE_NAME + " but absent in  "
                    + HORIZONTAL_SCAN_NAME + ": " + name);

                return;
            }

            if (rctx.items.size() != hctx.items.size()) {
                errCnt.incrementAndGet();
                log.severe(compareError("items", name, rctx.items.size(), hctx.items.size(), null));
            }

            rctx.stats.forEach((cls, stat) -> {
                long scanCnt = hctx.stats.getOrDefault(cls, new PagesStatistic()).cnt;

                if (scanCnt != stat.cnt) {
                    errCnt.incrementAndGet();
                    log.severe(compareError("pages", name, stat.cnt, scanCnt, cls));
                }
            });

            hctx.stats.forEach((cls, stat) -> {
                if (!rctx.stats.containsKey(cls)) {
                    errCnt.incrementAndGet();
                    log.severe(compareError("pages", name, 0, stat.cnt, cls));
                }
            });
        });

        horizontalScans.forEach((name, hctx) -> {
            if (!recursiveScans.containsKey(name)) {
                errCnt.incrementAndGet();
                log.severe("Tree was detected in " + HORIZONTAL_SCAN_NAME + " but absent in  "
                        + RECURSIVE_TRAVERSE_NAME + ": " + name);
            }
        });

        log.info("Comparing traversals detected " + errCnt + " errors.");
        log.info("------------------");
    }

    /** Prints sequential file scan results. */
    private void printSequentialScanInfo(ScanContext ctx) {
        printIoStat("", "---- These pages types were encountered during sequential scan:", ctx.stats);

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
                Arrays.asList("Total pages encountered during sequential scan:", ctx.stats.values().stream().mapToLong(a -> a.cnt).sum()),
                Arrays.asList("Total errors occurred during sequential scan: ", ctx.errCnt)
            ),
            log
        );

        if (idxFilter != null)
            log.info("Orphan pages were not reported due to --indexes filter.");

        log.info("Note that some pages can be occupied by meta info, tracking info, etc., so total page count can differ " +
            "from count of pages found in index trees and page lists.");
    }

    /** Prints traversal info. */
    private void printScanResults(String prefix, Map<String, ScanContext> ctxs) {
        log.info(prefix + "Tree traversal results");

        Map<Class<? extends PageIO>, PagesStatistic> stats = new HashMap<>();

        int totalErr = 0;

        // Map (cacheId, typeId) -> (map idxName -> size))
        Map<IgnitePair<Integer>, Map<String, Long>> cacheIdxSizes = new HashMap<>();

        for (Map.Entry<String, ScanContext> e : ctxs.entrySet()) {
            String idxName = e.getKey();
            ScanContext ctx = e.getValue();

            log.info(prefix + "-----");
            log.info(prefix + "Index tree: " + idxName);
            printIoStat(prefix, "---- Page stat:", ctx.stats);

            ctx.stats.forEach((cls, stat) -> ScanContext.addToStats(cls, stats, stat));

            log.info(prefix + "---- Count of items found in leaf pages: " + ctx.items.size());

            boolean hasInlineStat = ctx.inline != null && IntStream.of(ctx.inline).anyMatch(i -> i > 0);

            if (hasInlineStat) {
                log.info(prefix + "---- Inline usage statistics [inlineSize=" + ctx.inline.length + " bytes]");

                List<List<?>> data = new ArrayList<>(ctx.inline.length);
                for (int i = 0; i < ctx.inline.length; i++) {
                    if (ctx.inline[i] == 0)
                        continue;

                    data.add(Arrays.asList(prefix, i + 1, ctx.inline[i]));
                }

                SystemViewCommand.printTable(
                    Arrays.asList(prefix, "Used bytes", "Entries count"),
                    Arrays.asList(STRING, NUMBER, NUMBER),
                    data,
                    log
                );
            }

            printErrors(
                prefix,
                "Errors:",
                "No errors occurred while traversing.",
                "Page id=%s, exceptions:",
                ctx.errors
            );

            totalErr += ctx.errors.size();

            GridTuple3<Integer, Integer, String> parsed = cacheAndTypeId(idxName);

            cacheIdxSizes
                .computeIfAbsent(new IgnitePair<>(parsed.get1(), parsed.get2()), k -> new HashMap<>())
                .put(idxName, ctx.items.size());
        }

        log.info(prefix + "----");

        printIoStat(prefix, "Total page stat collected during trees traversal:", stats);

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
                Arrays.asList(prefix + "Total pages found in trees: ", stats.values().stream().mapToLong(a -> a.cnt).sum()),
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

        log.info(PAGE_LISTS_PREFIX + "Page lists info.");

        if (!pageListsInfo.bucketsData.isEmpty())
            log.info(PAGE_LISTS_PREFIX + "---- Printing buckets data:");

        pageListsInfo.bucketsData.forEach((bucket, bucketData) -> {
            GridStringBuilder sb = new GridStringBuilder(PAGE_LISTS_PREFIX)
                .a("List meta id=")
                .a(bucket.get1())
                .a(", bucket number=")
                .a(bucket.get2())
                .a(", lists=[")
                .a(bucketData.stream().map(IgniteIndexReader::normalizePageId).map(String::valueOf).collect(joining(", ")))
                .a("]");

            log.info(sb.toString());
        });

        printIoStat(PAGE_LISTS_PREFIX, "---- Page stat:", pageListsInfo.stats);

        printErrors(PAGE_LISTS_PREFIX, "---- Errors:", "---- No errors.", "Page id: %s, exception: ", pageListsInfo.errors);

        log.info("");

        SystemViewCommand.printTable(
            null,
            Arrays.asList(STRING, NUMBER),
            Arrays.asList(
                Arrays.asList(PAGE_LISTS_PREFIX + "Total index pages found in lists:", pageListsInfo.pagesCnt),
                Arrays.asList(PAGE_LISTS_PREFIX + "Total errors during lists scan:", pageListsInfo.errors.size())
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
    private ObjLongConsumer<String> errorHandler(String prefix) {
        return new ObjLongConsumer<String>() {
            /** */
            private boolean errFound;

            /** */
            private final String pfx = prefix;

            /** {@inheritDoc} */
            @Override public void accept(String err, long pageId) {
                if (!errFound)
                    log.warning(pfx + "---- Errors:");

                errFound = true;

                log.warning(pfx + "Page id: " + pageId + ", exception: " + err);
            }
        };
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
    private void printIoStat(String prefix, String caption, Map<Class<? extends PageIO>, PagesStatistic> stats) {
        if (caption != null)
            log.info(prefix + caption + (stats.isEmpty() ? " empty" : ""));

        if (stats.isEmpty())
            return;

        List<List<?>> data = new ArrayList<>(stats.size());

        stats.forEach((cls, stat) -> data.add(Arrays.asList(
            prefix + cls.getSimpleName(),
            stat.cnt,
            String.format("%.2f", ((double)stat.freeSpace) / U.KB),
            String.format("%.2f", (stat.freeSpace * 100.0d) / (pageSize * stat.cnt))
        )));

        Collections.sort(data, Comparator.comparingLong(l -> (Long)l.get(1)));

        SystemViewCommand.printTable(
            Arrays.asList(prefix + "Type", "Pages", "Free space (Kb)", "Free space (%)"),
            Arrays.asList(STRING, NUMBER, NUMBER, NUMBER),
            data,
            log
        );
    }

    /**
     * Creating new {@link FilePageStore} and initializing it.
     * It can return {@code null} if partition file were not found, for example: node should not contain it by affinity.
     *
     * @param partId Partition ID.
     * @param type Data type, can be {@link PageIdAllocator#FLAG_IDX} or {@link PageIdAllocator#FLAG_DATA}.
     * @param storeFactory Store factory.
     * @return New instance of {@link FilePageStore} or {@code null}.
     * @throws IgniteCheckedException If there are errors when creating or initializing {@link FilePageStore}.
     */
    @Nullable private FilePageStore filePageStore(
        int partId,
        byte type,
        FileVersionCheckingFactory storeFactory
    ) throws IgniteCheckedException {
        File file = new File(root, partId == INDEX_PARTITION ? INDEX_FILE_NAME : format(PART_FILE_TEMPLATE, partId));

        if (!file.exists())
            return null;

        FilePageStore filePageStore = (FilePageStore)storeFactory.createPageStore(type, file, l -> {});

        filePageStore.ensure();

        return filePageStore;
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

            if (io instanceof AbstractInlineLeafIO)
                visitInline(addr, (AbstractInlineLeafIO)io, ctx);

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
        private void visitInline(long addr, AbstractInlineLeafIO io, ScanContext ctx) {
            int inlineSz = ((InlineIO)io).inlineSize();

            if (ctx.inlineFldCnt == 0)
                return;

            if (ctx.inline == null)
                ctx.inline = new int[inlineSz];

            IndexKeyTypeSettings settings = new IndexKeyTypeSettings();

            for (int i = 0; i < io.getCount(addr); i++) {
                int itemOff = io.offset(i);
                int realInlineSz = 0;
                int fldCnt = 0;

                while (realInlineSz < inlineSz && fldCnt < ctx.inlineFldCnt) {
                    int type0 = PageUtils.getByte(addr, itemOff + realInlineSz);

                    IndexKeyType idxKeyType;

                    try {
                        idxKeyType = IndexKeyType.forCode(type0);
                    }
                    catch (Throwable t) {
                        log.log(Level.FINEST, "Unknown index key type [type=" + type0 + ']');

                        break;
                    }

                    if (idxKeyType == IndexKeyType.UNKNOWN || idxKeyType == IndexKeyType.NULL) {
                        realInlineSz += 1;

                        continue;
                    }

                    InlineIndexKeyType type = InlineIndexKeyTypeRegistry.get(idxKeyType, settings);

                    if (type == null) {
                        log.log(Level.FINEST, "Unknown inline type [type=" + type0 + ']');

                        break;
                    }

                    if (type.keySize() == UNKNOWN_CACHE) {
                        try {
                            // Assuming all variable length keys written using `writeBytes` method.
                            byte[] bytes = NullableInlineIndexKeyType.readBytes(addr, itemOff + realInlineSz);

                            realInlineSz += Short.BYTES; /* size of the array is short number. */
                            realInlineSz += bytes.length;
                        }
                        catch (Throwable e) {
                            log.warning("Error while reading inline [msg=" + e.getMessage() + ']');

                            break;
                        }
                    }
                    else
                        realInlineSz += type.keySize();

                    realInlineSz++; // One more byte for type.
                    fldCnt++;
                }

                ctx.inline[realInlineSz - 1]++;
            }
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
                    PageIO dataIo = readPage(partStores[linkedPagePartId], linkedPageId, dataBuf);

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

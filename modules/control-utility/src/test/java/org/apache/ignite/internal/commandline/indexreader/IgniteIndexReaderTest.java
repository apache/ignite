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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.cluster.IgniteClusterEx;
import org.apache.ignite.internal.processors.cache.persistence.IndexStorageImpl;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.util.GridStringBuilder;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.lang.IgnitePair;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static java.lang.String.format;
import static java.lang.String.valueOf;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static java.util.Objects.requireNonNull;
import static java.util.regex.Pattern.compile;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.commandline.indexreader.IgniteIndexReader.ERROR_PREFIX;
import static org.apache.ignite.internal.commandline.indexreader.IgniteIndexReader.HORIZONTAL_SCAN_NAME;
import static org.apache.ignite.internal.commandline.indexreader.IgniteIndexReader.RECURSIVE_TRAVERSE_NAME;
import static org.apache.ignite.internal.commandline.indexreader.IgniteIndexReader.normalizePageId;
import static org.apache.ignite.internal.commandline.indexreader.IgniteIndexReader.partMetaPageId;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.FLAG_IDX;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.INDEX_PARTITION;
import static org.apache.ignite.internal.pagemem.PageIdUtils.pageIndex;
import static org.apache.ignite.internal.pagemem.PageIdUtils.partId;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.CACHE_GRP_DIR_PREFIX;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.INDEX_FILE_NAME;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.PART_FILE_TEMPLATE;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;
import static org.apache.ignite.testframework.GridTestUtils.assertNotContains;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;

/**
 * Class for testing {@link IgniteIndexReader}.
 */
public class IgniteIndexReaderTest extends GridCommonAbstractTest {
    /** Page size. */
    protected static final int PAGE_SIZE = 4096;

    /** Partitions count. */
    protected static final int PART_CNT = RendezvousAffinityFunction.DFLT_PARTITION_COUNT;

    /** Version of file page stores. */
    private static final int PAGE_STORE_VER = 2;

    /** Cache group name. */
    protected static final String CACHE_GROUP_NAME = "defaultGroup";

    /** Cache group without indexes. */
    private static final String EMPTY_CACHE_NAME = "empty";

    /** Cache group without indexes. */
    private static final String EMPTY_CACHE_GROUP_NAME = "emptyGroup";

    /** Cache with static query configuration. */
    private static final String QUERY_CACHE_NAME = "query";

    /** Cache group with static query configuration. */
    private static final String QUERY_CACHE_GROUP_NAME = "queryGroup";

    /** Count of tables that will be created for test. */
    private static final int CREATED_TABLES_CNT = 3;

    /** Line delimiter. */
    private static final String LINE_DELIM = System.lineSeparator();

    /** Common part of regexp for single index output validation. */
    private static final String CHECK_IDX_PTRN_COMMON =
        "<PREFIX>Index tree: I \\[idxName=[\\-_0-9]{1,20}_%s##H2Tree.0, pageId=[0-9a-f]{16}\\]" +
            LINE_DELIM + "<PREFIX>-- Page stat:" +
            LINE_DELIM + "<PREFIX>([0-9a-zA-Z]{1,50}: [0-9]{1,5}" +
            LINE_DELIM + "<PREFIX>){%s,1000}-- Count of items found in leaf pages: %s" +
            LINE_DELIM;

    /** Regexp to validate output of correct index. */
    private static final String CHECK_IDX_PTRN_CORRECT =
        CHECK_IDX_PTRN_COMMON + "<PREFIX>No errors occurred while traversing.";

    /** Regexp to validate output of corrupted index. */
    private static final String CHECK_IDX_PTRN_WITH_ERRORS =
        CHECK_IDX_PTRN_COMMON + "<PREFIX>" + ERROR_PREFIX + "Errors:" +
            LINE_DELIM + "<PREFIX>" + ERROR_PREFIX + "Page id=[0-9]{1,30}, exceptions:" +
            LINE_DELIM + "class.*?Exception.*";

    /** Work directory, containing cache group directories. */
    private static File workDir;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        cleanPersistenceDir();

        prepareWorkDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName).setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setPageSize(PAGE_SIZE)
                .setWalSegmentSize(4 * 1024 * 1024)
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setPersistenceEnabled(true)
                        .setInitialSize(20 * 1024L * 1024L)
                        .setMaxSize(64 * 1024L * 1024L)
                )
        ).setCacheConfiguration(
            new CacheConfiguration(DEFAULT_CACHE_NAME)
                .setGroupName(CACHE_GROUP_NAME)
                .setSqlSchema(QueryUtils.DFLT_SCHEMA),
            new CacheConfiguration(EMPTY_CACHE_NAME)
                .setGroupName(EMPTY_CACHE_GROUP_NAME),
            new CacheConfiguration(QUERY_CACHE_NAME)
                .setGroupName(QUERY_CACHE_GROUP_NAME)
                .setQueryEntities(asList(
                    new QueryEntity(Integer.class, TestClass1.class)
                        .addQueryField("id", Integer.class.getName(), null)
                        .addQueryField("f", Integer.class.getName(), null)
                        .addQueryField("s", String.class.getName(), null)
                        .setIndexes(singleton(new QueryIndex("f")))
                        .setTableName("QT1"),
                    new QueryEntity(Integer.class, TestClass2.class)
                        .addQueryField("id", Integer.class.getName(), null)
                        .addQueryField("f", Integer.class.getName(), null)
                        .addQueryField("s", String.class.getName(), null)
                        .setIndexes(singleton(new QueryIndex("s")))
                        .setTableName("QT2")
                ))
        );
    }

    /**
     * Creating and filling {@link #workDir}.
     *
     * @throws Exception If fails.
     */
    protected void prepareWorkDir() throws Exception {
        try (IgniteEx node = startGrid(0)) {
            populateData(node, null);

            workDir = ((FilePageStoreManager)node.context().cache().context().pageStore()).workDir();
        }
    }

    /**
     * Filling node with data.
     *
     * @param node Node.
     * @param insert {@code True} for inserting some data and creating tables and indexes,
     *      {@code false} for inserting, updating and deleting data and deleting indexes, {@code null} for all data processing.
     * {@code True} if only insert data, {@code false} if delete from {@link #DEFAULT_CACHE_NAME} and {@code null} all at once.
     * @throws Exception If fails.
     */
    protected void populateData(IgniteEx node, @Nullable Boolean insert) throws Exception {
        requireNonNull(node);

        IgniteClusterEx cluster = node.cluster();

        if (!cluster.active())
            cluster.active(true);

        IgniteCache<Integer, Object> qryCache = node.cache(QUERY_CACHE_NAME);

        int s = isNull(insert) || insert ? 0 : 70;
        int e = isNull(insert) || !insert ? 100 : 80;

        for (int i = s; i < e; i++)
            qryCache.put(i, new TestClass1(i, valueOf(i)));

        s = isNull(insert) || insert ? 0 : 50;
        e = isNull(insert) || !insert ? 70 : 60;

        for (int i = s; i < e; i++)
            qryCache.put(i, new TestClass2(i, valueOf(i)));

        IgniteCache<Integer, Integer> cache = node.cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < CREATED_TABLES_CNT; i++)
            createAndFillTable(cache, TableInfo.generate(i), insert);

        forceCheckpoint(node);
    }

    /**
     * Get data directory for cache group.
     *
     * @param cacheGrpName Cache group name.
     * @return Directory name.
     */
    protected String dataDir(String cacheGrpName) {
        return CACHE_GRP_DIR_PREFIX + cacheGrpName;
    }

    /**
     * @param workDir Working directory.
     * @param cacheGrp Cache group.
     * @return Tuple that consists of some inner page id of any index tree, and some link to data.
     * @throws IgniteCheckedException If failed.
     */
    private IgniteBiTuple<Long, Long> findPagesForAnyCacheKey(File workDir, String cacheGrp) throws IgniteCheckedException {
        File dir = new File(workDir, dataDir(cacheGrp));

        try (IgniteIndexReader reader = new IgniteIndexReader(null, false, null, createFilePageStoreFactory(dir))) {
            IgniteBiTuple<Long, Long> partitionRoots = reader.partitionRoots(partMetaPageId(INDEX_PARTITION, FLAG_IDX));

            ItemsListStorage<IndexStorageImpl.IndexItem> idxItemStorage = new ItemsListStorage<>();

            reader.traverseTree(partitionRoots.get1(), "MetaTree", idxItemStorage);

            // Take any index item.
            IndexStorageImpl.IndexItem idxItem = idxItemStorage.iterator().next();

            ItemsListStorage<CacheAwareLink> linkStorage = new ItemsListStorage<>();

            // Take any inner page from tree.
            AtomicLong anyLeafId = new AtomicLong();

            reader.traverseTree(
                normalizePageId(idxItem.pageId()),
                idxItem.nameString(),
                null,
                (c, pageId) -> anyLeafId.set(pageId),
                null,
                linkStorage
            );

            // Take any link.
            long link = linkStorage.store.get(0).link;

            return new IgniteBiTuple<>(anyLeafId.get(), link);
        }
    }

    /**
     * Corrupts partition file.
     *
     * @param workDir Work directory.
     * @param pageToCorrupt Page to corrupt.
     * @throws Exception If failed.
     */
    private void corruptFile(File workDir, long pageToCorrupt) throws Exception {
        int partId = partId(pageToCorrupt);
        int pageIdxCorrupt = pageIndex(pageToCorrupt);

        String fileName = partId == INDEX_PARTITION ? INDEX_FILE_NAME : format(PART_FILE_TEMPLATE, partId);

        File cacheWorkDir = new File(workDir, dataDir(CACHE_GROUP_NAME));

        File file = new File(cacheWorkDir, fileName);

        File backup = new File(cacheWorkDir, fileName + ".backup");

        if (!backup.exists())
            Files.copy(file.toPath(), backup.toPath());

        ByteBuffer buf = GridUnsafe.allocateBuffer(PAGE_SIZE);

        try (FileChannel c = new RandomAccessFile(file, "rw").getChannel()) {
            byte[] trash = new byte[PAGE_SIZE];

            ThreadLocalRandom.current().nextBytes(trash);

            int pageIdx = realPageIdxInFile(c, pageIdxCorrupt);

            buf.rewind();
            buf.put(trash);
            buf.rewind();

            c.write(buf, pageIdx * PAGE_SIZE);
        }
        finally {
            GridUnsafe.freeBuffer(buf);
        }
    }

    /**
     * @param c Opened file channel.
     * @param pageIdxToFind Page index to find. May be different in snapshot files.
     * @return Page index.
     * @throws IOException If failed.
     */
    protected int realPageIdxInFile(FileChannel c, int pageIdxToFind) throws IOException {
        ByteBuffer buf = GridUnsafe.allocateBuffer(PAGE_SIZE);

        long addr = GridUnsafe.bufferAddress(buf);

        try {
            int pageCnt = 0;

            while (true) {
                buf.rewind();

                if (c.read(buf) == -1)
                    break;

                buf.rewind();

                long pageId = PageIO.getPageId(addr);
                int pageIdx = pageIndex(pageId);

                if (pageIdx == pageIdxToFind)
                    return pageCnt;

                pageCnt++;
            }
        }
        finally {
            GridUnsafe.freeBuffer(buf);
        }

        return -1;
    }

    /**
     * Restores corrupted file from backup after corruption if exists.
     *
     * @param workDir Work directory.
     * @param partId Partition id.
     * @throws IOException If failed.
     */
    private void restoreFile(File workDir, int partId) throws IOException {
        String fileName = partId == INDEX_PARTITION ? INDEX_FILE_NAME : format(PART_FILE_TEMPLATE, partId);

        File cacheWorkDir = new File(workDir, dataDir(CACHE_GROUP_NAME));

        File backupFiles = new File(cacheWorkDir, fileName + ".backup");

        if (!backupFiles.exists())
            return;

        Path backupFilesPath = backupFiles.toPath();

        Files.copy(backupFilesPath, new File(cacheWorkDir, fileName).toPath(), REPLACE_EXISTING);

        Files.delete(backupFilesPath);
    }

    /**
     * Generates fields for sql table.
     *
     * @param cnt Count of fields.
     * @return List of pairs, first is field name, second is field type.
     */
    private static List<IgnitePair<String>> fields(int cnt) {
        List<IgnitePair<String>> res = new LinkedList<>();

        for (int i = 0; i < cnt; i++)
            res.add(new IgnitePair<>("f" + i, i % 2 == 0 ? "integer" : "varchar(100)"));

        return res;
    }

    /**
     * Generates indexes for given table and fields.
     *
     * @param tblName Table name.
     * @param fields Fields list, returned by {@link #fields}.
     * @return List of pairs, first is index name, second is list of fields, covered by index, divived by comma.
     */
    private static List<IgnitePair<String>> idxs(String tblName, List<IgnitePair<String>> fields) {
        List<IgnitePair<String>> res = new LinkedList<>();

        res.addAll(fields.stream().map(f -> new IgnitePair<>(tblName + "_" + f.get1() + "_idx", f.get1())).collect(toList()));

        // Add one multicolumn index.
        if (fields.size() > 1) {
            res.add(new IgnitePair<>(
                tblName + "_" + fields.get(0).get1() + "_" + fields.get(1).get1() + "_idx",
                fields.get(0).get1() + "," + fields.get(1).get1()
            ));
        }

        return res;
    }

    /**
     * Creates an sql table, indexes and fill it with some data.
     *
     * @param cache Ignite cache.
     * @param insert {@code True} for inserting some data and creating tables and indexes,
     *      {@code false} for inserting, updating and deleting data and deleting indexes, {@code null} for all data processing.
     * @param info Table info.
     */
    private static void createAndFillTable(IgniteCache cache, TableInfo info, @Nullable Boolean insert) {
        String idxToDelName = info.tblName + "_idx_to_delete";

        List<IgnitePair<String>> fields = fields(info.fieldsCnt);
        List<IgnitePair<String>> idxs = idxs(info.tblName, fields);

        String strFields = fields.stream().map(f -> f.get1() + " " + f.get2()).collect(joining(", "));

        if (isNull(insert) || insert) {
            query(
                cache,
                "create table " + info.tblName + " (id integer primary key, " + strFields + ") with " +
                    "\"CACHE_NAME=" + info.tblName + ", CACHE_GROUP=" + CACHE_GROUP_NAME + "\""
            );

            for (IgnitePair<String> idx : idxs)
                query(cache, format("create index %s on %s (%s)", idx.get1(), info.tblName, idx.get2()));

            query(cache, format("create index %s on %s (%s)", idxToDelName, info.tblName, fields.get(0).get1()));
        }

        int s = isNull(insert) || insert ? 0 : info.rec - 10;
        int e = isNull(insert) || !insert ? info.rec : info.rec - 10;

        for (int i = s; i < e; i++)
            insertQuery(cache, info.tblName, fields, i);

        if (nonNull(insert) && !insert) {
            for (int i = s - 10; i < s; i++)
                updateQuery(cache, info.tblName, fields, i);
        }

        if (isNull(insert) || !insert) {
            for (int i = info.rec - info.del; i < info.rec; i++)
                query(cache, "delete from " + info.tblName + " where id = " + i);
        }

        if (isNull(insert) || !insert)
            query(cache, "drop index " + idxToDelName);
    }

    /**
     * Performs an insert query.
     *
     * @param cache Ignite cache.
     * @param tblName Table name.
     * @param fields List of fields.
     * @param cntr Counter which is used to generate data.
     */
    private static void insertQuery(IgniteCache cache, String tblName, List<IgnitePair<String>> fields, int cntr) {
        GridStringBuilder q = new GridStringBuilder().a("insert into ").a(tblName).a(" (id, ");

        q.a(fields.stream().map(IgniteBiTuple::get1).collect(joining(", ")));
        q.a(") values (");
        q.a(fields.stream().map(f -> "?").collect(joining(", ", "?, ", ")")));

        Object[] paramVals = new Object[fields.size() + 1];

        for (int i = 0; i < fields.size() + 1; i++)
            paramVals[i] = (i % 2 == 0) ? cntr : valueOf(cntr);

        query(cache, q.toString(), paramVals);
    }

    /**
     * Performs an update query.
     *
     * @param cache Ignite cache.
     * @param tblName Table name.
     * @param fields List of fields.
     * @param cntr Counter which is used to generate data.
     */
    private static void updateQuery(IgniteCache cache, String tblName, List<IgnitePair<String>> fields, int cntr) {
        GridStringBuilder q = new GridStringBuilder().a("update ").a(tblName).a(" set ")
            .a(fields.stream().map(IgniteBiTuple::get1).collect(joining("=?, ", "", "=?")))
            .a(" where id=?");

        Object[] paramVals = new Object[fields.size() + 1];

        for (int i = 0; i < fields.size() + 1; i++)
            paramVals[i] = (i % 2 == 0) ? cntr : valueOf(cntr);

        Object id = paramVals[0];

        paramVals[0] = paramVals[paramVals.length - 1];
        paramVals[paramVals.length - 1] = id;

        query(cache, q.toString(), paramVals);
    }

    /**
     * Performs a query.
     *
     * @param cache Ignite cache.
     * @param qry Query string.
     * @return Result.
     */
    private static List<List<?>> query(IgniteCache cache, String qry) {
        return cache.query(new SqlFieldsQuery(qry)).getAll();
    }

    /**
     * Performs a query.
     *
     * @param cache Ignite cache.
     * @param qry Query string.
     * @param args Query arguments.
     * @return Result.
     */
    private static List<List<?>> query(IgniteCache cache, String qry, Object... args) {
        return cache.query(new SqlFieldsQuery(qry).setArgs(args)).getAll();
    }

    /**
     * Checks the index reader output.
     *
     * @param output Output.
     * @param treesCnt Count of b+ trees.
     * @param travErrCnt Count of errors that can occur during traversal.
     * @param horizScanErrCnt Count of errors that can occur during horizontal scan.
     * @param pageListsErrCnt Count of errors that can occur during page lists scan.
     * @param seqErrCnt Count of errors that can occur during sequential scan.
     * @param partReadingErr partition file reading errors should be present.
     * @param idxSizeConsistent Index size should be consistent
     */
    private void checkOutput(
        String output,
        int treesCnt,
        int travErrCnt,
        int pageListsErrCnt,
        int seqErrCnt,
        boolean idxReadingErr,
        boolean partReadingErr,
        boolean idxSizeConsistent
    ) {
        assertContains(log, output, RECURSIVE_TRAVERSE_NAME + "Total trees: " + treesCnt);
        assertContains(log, output, HORIZONTAL_SCAN_NAME + "Total trees: " + treesCnt);
        assertContains(log, output, RECURSIVE_TRAVERSE_NAME + "Total errors during trees traversal: " + travErrCnt);
        assertContains(log, output, HORIZONTAL_SCAN_NAME + "Total errors during trees traversal: " + travErrCnt);
        assertContains(log, output, "Total errors during lists scan: " + pageListsErrCnt);

        if (idxSizeConsistent)
            assertContains(log, output, "No index size consistency errors found.");
        else
            assertContains(log, output, "Index size inconsistency");

        if (seqErrCnt >= 0)
            assertContains(log, output, "Total errors occurred during sequential scan: " + seqErrCnt);
        else
            assertContains(log, output, "Orphan pages were not reported due to --indexes filter.");

        if (travErrCnt == 0 && pageListsErrCnt == 0 && seqErrCnt == 0)
            assertNotContains(log, output, ERROR_PREFIX);

        if (idxReadingErr)
            assertContains(log, output, ERROR_PREFIX + "Errors detected while reading index.bin");

        if (partReadingErr)
            assertContains(log, output, ERROR_PREFIX + "Errors detected while reading partition files:");
    }

    /**
     * Checks output info for indexes.
     *
     * @param output Output string.
     * @param info Table info, which is used to calculate index info.
     * @param corruptedIdx Whether some index is corrupted.
     */
    private void checkIdxs(String output, TableInfo info, boolean corruptedIdx) {
        List<IgnitePair<String>> fields = fields(info.fieldsCnt);
        List<IgnitePair<String>> idxs = idxs(info.tblName, fields);

        int entriesCnt = info.rec - info.del;

        idxs.stream().map(IgniteBiTuple::get1)
            .forEach(idx -> {
                checkIdx(output, RECURSIVE_TRAVERSE_NAME, idx.toUpperCase(), entriesCnt, corruptedIdx);
                checkIdx(output, HORIZONTAL_SCAN_NAME, idx.toUpperCase(), entriesCnt, corruptedIdx);
            });
    }

    /**
     * Checks output info for single index.
     *
     * @param output Output string.
     * @param traversePrefix Traverse prefix.
     * @param idx Index name.
     * @param entriesCnt Count of entries that should be present in index.
     * @param canBeCorrupted Whether index can be corrupted.
     */
    private void checkIdx(String output, String traversePrefix, String idx, int entriesCnt, boolean canBeCorrupted) {
        Pattern ptrnCorrect = compile(checkIdxRegex(traversePrefix, false, idx, 1, valueOf(entriesCnt)));

        Matcher mCorrect = ptrnCorrect.matcher(output);

        if (canBeCorrupted) {
            Pattern ptrnCorrupted = compile(checkIdxRegex(traversePrefix, true, idx, 0, "[0-9]{1,4}"));

            Matcher mCorrupted = ptrnCorrupted.matcher(output);

            assertTrue("could not find index " + idx + ":\n" + output, mCorrect.find() || mCorrupted.find());
        }
        else
            assertTrue("could not find index " + idx + ":\n" + output, mCorrect.find());
    }

    /**
     * Returns regexp string for index check.
     *
     * @param traversePrefix Traverse prefix.
     * @param withErrors Whether errors should be present.
     * @param idxName Index name.
     * @param minimumPageStatSize Minimum size of page stats for index.
     * @param itemsCnt Count of data entries.
     * @return Regexp string.
     */
    private String checkIdxRegex(
        String traversePrefix,
        boolean withErrors,
        String idxName,
        int minimumPageStatSize,
        String itemsCnt
    ) {
        return format(
            withErrors ? CHECK_IDX_PTRN_WITH_ERRORS : CHECK_IDX_PTRN_CORRECT,
            idxName,
            minimumPageStatSize,
            itemsCnt
        ).replace("<PREFIX>", traversePrefix);
    }

    /**
     * Runs index reader on given cache group.
     *
     * @param workDir Work directory.
     * @param cacheGrp Cache group name.
     * @param idxs Indexes to check.
     * @param checkParts Whether to check cache data tree in partitions..
     * @return Index reader output.
     * @throws IgniteCheckedException If failed.
     */
    private String runIndexReader(
        File workDir,
        String cacheGrp,
        @Nullable String[] idxs,
        boolean checkParts
    ) throws IgniteCheckedException {
        File dir = new File(workDir, dataDir(cacheGrp));

        OutputStream destStream = new ByteArrayOutputStream();

        List<String> idxList = isNull(idxs) ? null : asList(idxs);

        try (IgniteIndexReader reader = new IgniteIndexReader(
            isNull(idxList) ? null : idx -> idxList.stream().anyMatch(idx::endsWith),
            checkParts,
            new PrintStream(destStream),
            createFilePageStoreFactory(dir)
        )) {
            reader.readIdx();
        }

        return destStream.toString();
    }

    /**
     * Create new {@link IgniteIndexReaderFilePageStoreFactory}.
     *
     * @param dir Data rirectory.
     * @throws IgniteCheckedException If failed.
     */
    protected IgniteIndexReaderFilePageStoreFactory createFilePageStoreFactory(
        File dir
    ) throws IgniteCheckedException {
        return new IgniteIndexReaderFilePageStoreFactoryImpl(dir, PAGE_SIZE, PART_CNT, PAGE_STORE_VER);
    }

    /**
     * Test checks correctness of index.
     *
     * Steps:
     * 1)Run {@link IgniteIndexReader} for {@link #CACHE_GROUP_NAME};
     * 2)Check that there are no errors in output and 19 B+trees were found;
     * 3)Check that all indexes are present in output.
     *
     * @throws IgniteCheckedException If failed.
     */
    @Test
    public void testCorrectIdx() throws IgniteCheckedException {
        checkCorrectIdx(workDir);
    }

    /**
     * Test checks correctness of index and consistency of partitions.
     *
     * Steps:
     * 1)Run {@link IgniteIndexReader} for {@link #CACHE_GROUP_NAME} with check partitions;
     * 2)Check that there are no errors in output and 19 B+trees were found;
     * 3)Check that all indexes are present in output.
     *
     * @throws IgniteCheckedException If failed.
     */
    @Test
    public void testCorrectIdxWithCheckParts() throws IgniteCheckedException {
        checkCorrectIdxWithCheckParts(workDir);
    }

    /**
     * Test verifies that specific indexes being checked are correct.
     *
     * Steps:
     * 1)Run {@link IgniteIndexReader} for {@link #CACHE_GROUP_NAME} with filter by indexes;
     * 2)Check that there are no errors in output and 3 B+trees were found;
     * 3)Check filtered indexes are present in output.
     *
     * @throws IgniteCheckedException If failed.
     */
    @Test
    public void testCorrectIdxWithFilter() throws IgniteCheckedException {
        checkCorrectIdxWithFilter(workDir);
    }

    /**
     * Test checks whether the index of an empty group is correct.
     *
     * Steps:
     * 1)Run {@link IgniteIndexReader} for {@link #EMPTY_CACHE_GROUP_NAME};
     * 2)Check that there are no errors in output and 1 B+tree were found;
     * 4)Run {@link IgniteIndexReader} for a non-existent cache group;
     * 3)Check that an exception is thrown.
     *
     * @throws IgniteCheckedException If failed.
     */
    @Test
    public void testEmpty() throws IgniteCheckedException {
        checkEmpty(workDir);
    }

    /**
     * Test for finding corrupted pages in index.
     *
     * Steps:
     * 1)Currupt page in index.bin and backup it;
     * 2)Run {@link IgniteIndexReader} for {@link #CACHE_GROUP_NAME};
     * 3)Check that found 19 B+trees, 2 errors in sequential and transversal analysis;
     * 4)Check that all indexes are present in output;
     * 5)Restore backup index.bin.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCorruptedIdx() throws Exception {
        checkCorruptedIdx(asList(workDir));
    }

    /**
     * Test for finding corrupted pages in index and checking for consistency in partitions.
     *
     * Steps:
     * 1)Currupt pages not in reuseList of index.bin and backup it;
     * 2)Run {@link IgniteIndexReader} for {@link #CACHE_GROUP_NAME} with check partitions;
     * 3)Check that there are errors when checking partitions and there are no errors in reuseList;
     * 4)Restore backup index.bin.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCorruptedIdxWithCheckParts() throws Exception {
        checkCorruptedIdxWithCheckParts(asList(workDir));
    }

    /**
     * Test for finding corrupted page in partition.
     *
     * Steps:
     * 1)Currupt page in part-0.bin and backup it;
     * 2)Run {@link IgniteIndexReader} for {@link #CACHE_GROUP_NAME};
     * 2)Check that found 19 B+trees and errors when reading currupted page from partition;
     * 3)Check that all indexes are present in output;
     * 4)Restore backup part-0.bin.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCorruptedPart() throws Exception {
        checkCorruptedPart(asList(workDir));
    }

    /**
     * Test for finding corrupted pages in index and partition.
     *
     * Steps:
     * 1)Currupt page in index.bin, part-0.bin and backup them;
     * 2)Run {@link IgniteIndexReader} for {@link #CACHE_GROUP_NAME};
     * 2)Check that found 19 B+trees and errors when reading currupted page from index and partition;
     * 3)Check that all indexes are present in output;
     * 4)Restore backup index.bin and part-0.bin.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCorruptedIdxAndPart() throws Exception {
        checkCorruptedIdxAndPart(asList(workDir));
    }

    /**
     * Test checks correctness of index of {@link #QUERY_CACHE_GROUP_NAME}.
     *
     * Steps:
     * 1)Run {@link IgniteIndexReader} for {@link #QUERY_CACHE_GROUP_NAME};
     * 2)Check that there are no errors in output and 5 B+trees were found.
     *
     * @throws IgniteCheckedException If failed.
     */
    @Test
    public void testQryCacheGroup() throws IgniteCheckedException {
        checkQryCacheGroup(workDir);
    }

    /**
     * Checks whether corrupted pages are found in index and partition.
     * Index and partition {@code 0} will be corrupted for work directories,
     * and first work directory will be used to start {@link IgniteIndexReader}.
     *
     * @param workDirs Work directories.
     * @throws Exception If failed.
     */
    protected void checkCorruptedIdxAndPart(List<File> workDirs) throws Exception {
        A.ensure(nonNull(workDirs) && !workDirs.isEmpty(), "empty workDirs");

        try {
            for (File dir : workDirs) {
                IgniteBiTuple<Long, Long> pagesToCorrupt = findPagesForAnyCacheKey(dir, CACHE_GROUP_NAME);

                corruptFile(dir, pagesToCorrupt.get1());
                corruptFile(dir, pagesToCorrupt.get2());
            }

            String output = runIndexReader(workDirs.get(0), CACHE_GROUP_NAME, null, false);

            boolean idxReadingErr = isReportIdxAndPartFilesReadingErr();
            boolean partReadingErr = isReportIdxAndPartFilesReadingErr();

            checkOutput(output, 19, 24, 0, 1, idxReadingErr, partReadingErr, false);

            for (int i = 0; i < CREATED_TABLES_CNT; i++)
                checkIdxs(output, TableInfo.generate(i), true);
        }
        finally {
            for (File dir : workDirs) {
                restoreFile(dir, INDEX_PARTITION);
                restoreFile(dir, 0);
            }
        }
    }

    /**
     * Checks whether corrupted page are found in partition.
     * Partition {@code 0} will be corrupted for work directories,
     * and first work directory will be used to start {@link IgniteIndexReader}.
     *
     * @param workDirs Work directories.
     * @throws Exception If failed.
     */
    protected void checkCorruptedPart(List<File> workDirs) throws Exception {
        A.ensure(nonNull(workDirs) && !workDirs.isEmpty(), "empty workDirs");

        try {
            for (File dir : workDirs) {
                IgniteBiTuple<Long, Long> pagesToCorrupt = findPagesForAnyCacheKey(dir, CACHE_GROUP_NAME);

                corruptFile(dir, pagesToCorrupt.get2());
            }

            String output = runIndexReader(workDirs.get(0), CACHE_GROUP_NAME, null, false);

            boolean partReadingErr = isReportIdxAndPartFilesReadingErr();

            checkOutput(output, 19, 22, 0, 0, false, partReadingErr, true);

            for (int i = 0; i < CREATED_TABLES_CNT; i++)
                checkIdxs(output, TableInfo.generate(i), true);
        }
        finally {
            for (File dir : workDirs)
                restoreFile(dir, 0);
        }
    }

    /**
     * Checking for corrupted pages in index and checking for consistency in partitions.
     * Index will be corrupted for work directories, and first work directory will be used to start {@link IgniteIndexReader}.
     *
     * @param workDirs Work directories.
     * @throws Exception If failed.
     */
    protected void checkCorruptedIdxWithCheckParts(List<File> workDirs) throws Exception {
        A.ensure(nonNull(workDirs) && !workDirs.isEmpty(), "empty workDirs");

        try {
            for (File dir : workDirs) {
                IgniteBiTuple<Long, Long> pagesToCorrupt = findPagesForAnyCacheKey(dir, CACHE_GROUP_NAME);

                corruptFile(dir, pagesToCorrupt.get1());
            }

            String output = runIndexReader(workDirs.get(0), CACHE_GROUP_NAME, null, true);

            // Pattern with errors count > 9
            Pattern ptrn = compile(
                "Partition check finished, total errors: [0-9]{2,5}, total problem partitions: [0-9]{2,5}"
            );

            assertTrue(output, ptrn.matcher(output).find());

            assertContains(log, output, "Total errors during lists scan: 0");
        }
        finally {
            for (File dir : workDirs)
                restoreFile(dir, INDEX_PARTITION);
        }
    }

    /**
     * Checks whether corrupted pages are found in index.
     * Index will be corrupted for work directories, and first work directory will be used to start {@link IgniteIndexReader}.
     *
     * @param workDirs Work directories.
     * @throws Exception If failed.
     */
    protected void checkCorruptedIdx(List<File> workDirs) throws Exception {
        A.ensure(nonNull(workDirs) && !workDirs.isEmpty(), "empty workDirs");

        try {
            for (File dir : workDirs) {
                IgniteBiTuple<Long, Long> pagesToCorrupt = findPagesForAnyCacheKey(dir, CACHE_GROUP_NAME);

                corruptFile(dir, pagesToCorrupt.get1());
            }

            String output = runIndexReader(workDirs.get(0), CACHE_GROUP_NAME, null, false);

            // 1 corrupted page detected while traversing, and 1 index size inconsistency error.
            int travErrCnt = 2;

            int seqErrCnt = 1;

            boolean idxReadingErr = isReportIdxAndPartFilesReadingErr();

            checkOutput(output, 19, travErrCnt, 0, seqErrCnt, idxReadingErr, false, false);

            for (int i = 0; i < CREATED_TABLES_CNT; i++)
                checkIdxs(output, TableInfo.generate(i), true);
        }
        finally {
            for (File dir : workDirs)
                restoreFile(dir, INDEX_PARTITION);
        }
    }

    /**
     * Checking correctness of index for {@link #QUERY_CACHE_GROUP_NAME}.
     *
     * @param workDir Work directory.
     * @throws IgniteCheckedException If failed.
     */
    protected void checkQryCacheGroup(File workDir) throws IgniteCheckedException {
        String output = runIndexReader(workDir, QUERY_CACHE_GROUP_NAME, null, false);

        checkOutput(output, 5, 0, 0, 0, false, false, true);
    }

    /**
     * Checking correctness of index.
     *
     * @param workDir Work directory.
     * @throws IgniteCheckedException If failed.
     */
    protected void checkCorrectIdx(File workDir) throws IgniteCheckedException {
        String output = runIndexReader(workDir, CACHE_GROUP_NAME, null, false);

        checkOutput(output, 19, 0, 0, 0, false, false, true);

        for (int i = 0; i < CREATED_TABLES_CNT; i++)
            checkIdxs(output, TableInfo.generate(i), false);
    }

    /**
     * Checking correctness of index and consistency of partitions with it.
     *
     * @param workDir Work directory.
     * @throws IgniteCheckedException If failed.
     */
    protected void checkCorrectIdxWithCheckParts(File workDir) throws IgniteCheckedException {
        String output = runIndexReader(workDir, CACHE_GROUP_NAME, null, true);

        checkOutput(output, 19, 0, 0, 0, false, false, true);

        for (int i = 0; i < CREATED_TABLES_CNT; i++)
            checkIdxs(output, TableInfo.generate(i), false);

        assertContains(log, output, "Partitions check detected no errors.");
        assertContains(log, output, "Partition check finished, total errors: 0, total problem partitions: 0");
    }

    /**
     * Checking that specific indexes being checked are correct.
     *
     * @param workDir Work directory.
     * @throws IgniteCheckedException If failed.
     */
    protected void checkCorrectIdxWithFilter(File workDir) throws IgniteCheckedException {
        String[] idxsToCheck = {"T2_F1_IDX##H2Tree%0", "T2_F2_IDX##H2Tree%0"};

        String output = runIndexReader(workDir, CACHE_GROUP_NAME, idxsToCheck, false);

        checkOutput(output, 3, 0, 0, -1, false, false, true);

        Set<String> idxSet = new HashSet<>(asList(idxsToCheck));

        for (int i = 0; i < CREATED_TABLES_CNT; i++) {
            TableInfo info = TableInfo.generate(i);

            List<IgnitePair<String>> fields = fields(info.fieldsCnt);
            List<IgnitePair<String>> idxs = idxs(info.tblName, fields);

            int entriesCnt = info.rec - info.del;

            idxs.stream().map(IgniteBiTuple::get1)
                .filter(idxSet::contains)
                .forEach(idx -> {
                    checkIdx(output, RECURSIVE_TRAVERSE_NAME, idx.toUpperCase(), entriesCnt, false);
                    checkIdx(output, HORIZONTAL_SCAN_NAME, idx.toUpperCase(), entriesCnt, false);
                });
        }
    }

    /**
     * Validating index of an empty group.
     *
     * @param workDir Work directory.
     * @throws IgniteCheckedException If failed.
     */
    protected void checkEmpty(File workDir) throws IgniteCheckedException {
        // Check output for empty cache group.
        String output = runIndexReader(workDir, EMPTY_CACHE_GROUP_NAME, null, false);

        checkOutput(output, 1, 0, 0, 0, false, false, true);

        // Create an empty directory and try to check it.
        String newCleanGrp = "noCache";

        File cleanDir = new File(workDir, dataDir(newCleanGrp));

        try {
            cleanDir.mkdir();

            assertThrows(
                log,
                () -> runIndexReader(workDir, newCleanGrp, null, false),
                IgniteCheckedException.class,
                null
            );
        }
        finally {
            U.delete(cleanDir);
        }
    }

    /**
     * @return Flag indicates partition file reading errors should be present in output.
     */
    protected boolean isReportIdxAndPartFilesReadingErr() {
        return false;
    }

    /**
     *
     */
    private static class TableInfo {
        /** Table name. */
        final String tblName;

        /** Fields count. */
        final int fieldsCnt;

        /** Count of records that should be inserted. */
        final int rec;

        /** Count of records that should be deleted after insert.*/
        final int del;

        /** */
        public TableInfo(String tblName, int fieldsCnt, int rec, int del) {
            this.tblName = tblName;
            this.fieldsCnt = fieldsCnt;
            this.rec = rec;
            this.del = del;
        }

        /**
         * Generates some table info for given int.
         * @param i Some integer.
         * @return Table info.
         */
        public static TableInfo generate(int i) {
            return new TableInfo("T" + i, 3 + (i % 3), 1700 - (i % 3) * 500, (i % 3) * 250);
        }
    }

    /**
     *
     */
    private static class TestClass1 {
        /** */
        private final Integer f;

        /** */
        private final String s;

        /** */
        public TestClass1(Integer f, String s) {
            this.f = f;
            this.s = s;
        }
    }

    /**
     *
     */
    private static class TestClass2 extends TestClass1 {
        /** */
        public TestClass2(Integer f, String s) {
            super(f, s);
        }
    }
}

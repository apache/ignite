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

package org.apache.ignite.internal.managers.communication;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.OpenOption;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIODecorator;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.wal.crc.FastCrc;
import org.apache.ignite.internal.util.lang.IgniteThrowableConsumer;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.FILE_SUFFIX;
import static org.apache.ignite.internal.util.IgniteUtils.fileCount;

/**
 * Test file transmission mamanger operations.
 */
public class GridIoManagerFileTransmissionSelfTest extends GridCommonAbstractTest {
    /** Number of cache keys to generate. */
    private static final long CACHE_SIZE = 50_000L;

    /** Temporary directory to store files. */
    private static final String TEMP_FILES_DIR = "ctmp";

    /** Factory to produce IO interfaces over files to transmit. */
    private static final FileIOFactory IO_FACTORY = new RandomAccessFileIOFactory();

    /** The topic to send files to. */
    private static Object topic;

    /** File filter. */
    private static FilenameFilter fileBinFilter;

    /** Locally used fileIo to interact with output file. */
    private final FileIO[] fileIo = new FileIO[1];

    /** The temporary directory to store files. */
    private File tempStore;

    /**
     * @throws Exception If fails.
     */
    @BeforeClass
    public static void beforeAll() throws Exception {
        topic = GridTopic.TOPIC_CACHE.topic("test", 0);

        fileBinFilter = new FilenameFilter() {
            @Override public boolean accept(File dir, String name) {
                return name.endsWith(FILE_SUFFIX);
            }
        };
    }

    /**
     * @throws Exception if failed.
     */
    @Before
    public void before() throws Exception {
        cleanPersistenceDir();

        tempStore = U.resolveWorkDirectory(U.defaultWorkDirectory(), TEMP_FILES_DIR, true);
    }

    /**
     * @throws Exception if failed.
     */
    @After
    public void after() throws Exception {
        stopAllGrids();

        U.closeQuiet(fileIo[0]);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setPersistenceEnabled(true)
                    .setMaxSize(500L * 1024 * 1024)))
            .setCacheConfiguration(new CacheConfiguration<Integer, Integer>(DEFAULT_CACHE_NAME));
    }

    /**
     * Transmit all cache partition to particular topic on the remote node.
     *
     * @throws Exception If fails.
     */
    @Test
    public void testFileHandlerBase() throws Exception {
        IgniteEx snd = startGrid(0);
        IgniteEx rcv = startGrid(1);

        snd.cluster().active(true);

        addCacheData(snd, DEFAULT_CACHE_NAME);

        awaitPartitionMapExchange();

        Map<String, Long> fileSizes = new HashMap<>();
        Map<String, Integer> fileCrcs = new HashMap<>();
        Map<String, Serializable> fileParams = new HashMap<>();

        assertTrue(snd.context().io().fileTransmissionSupported(rcv.localNode()));

        rcv.context().io().addTransmissionHandler(topic, new TransmissionHandlerAdapter() {
            @Override public void onBegin(UUID nodeId) {
                assertEquals(snd.localNode().id(), nodeId);
            }

            @Override public String filePath(UUID nodeId, TransmissionMeta fileMeta) {
                return new File(tempStore, fileMeta.name()).getAbsolutePath();
            }

            @Override public IgniteThrowableConsumer<File> fileHandler(UUID nodeId, TransmissionMeta initMeta) {
                return new IgniteThrowableConsumer<File>() {

                    @Override public void accept(File file) {
                        assertTrue(fileSizes.containsKey(file.getName()));
                        // Save all params.
                        fileParams.putAll(initMeta.params());
                    }
                };
            }
        });

        File cacheDirIg0 = cacheWorkDir(snd, DEFAULT_CACHE_NAME);

        File[] cacheParts = cacheDirIg0.listFiles(fileBinFilter);

        for (File file : cacheParts) {
            fileSizes.put(file.getName(), file.length());
            fileCrcs.put(file.getName(), FastCrc.calcCrc(file));
        }

        try (GridIoManager.TransmissionSender sender = snd.context()
            .io()
            .openTransmissionSender(rcv.localNode().id(), topic)) {
            // Iterate over cache partition cacheParts.
            for (File file : cacheParts) {
                Map<String, Serializable> params = new HashMap<>();

                params.put(file.getName(), file.hashCode());

                sender.send(file,
                    // Put additional params <file_name, file_hashcode> to map.
                    params,
                    TransmissionPolicy.FILE);
            }
        }

        log.info("Writing test cacheParts finished. All Ignite instances will be stopped.");

        stopAllGrids();

        assertEquals(fileSizes.size(), tempStore.listFiles(fileBinFilter).length);

        for (File file : cacheParts) {
            // Check received file lenghs
            assertEquals("Received file lenght is incorrect: " + file.getName(),
                fileSizes.get(file.getName()), new Long(file.length()));

            // Check received params
            assertEquals("File additional parameters are not fully transmitted",
                fileParams.get(file.getName()), file.hashCode());
        }

        // Check received file CRCs.
        for (File file : tempStore.listFiles(fileBinFilter)) {
            assertEquals("Received file CRC-32 checksum is incorrect: " + file.getName(),
                fileCrcs.get(file.getName()), new Integer(FastCrc.calcCrc(file)));
        }
    }

    /**
     * @throws Exception If fails.
     */
    @Test(expected = IgniteCheckedException.class)
    public void testFileHandlerOnBeginFails() throws Exception {
        final String exTestMessage = "Test exception. Handler initialization failed at onBegin.";

        IgniteEx snd = startGrid(0);
        IgniteEx rcv = startGrid(1);

        snd.cluster().active(true);

        File fileToSend = createFileRandomData("1Mb", 1024 * 1024);

        rcv.context().io().addTransmissionHandler(topic, new DefaultTransmissionHandler(rcv, fileToSend, tempStore) {
            @Override public void onBegin(UUID nodeId) {
                throw new IgniteException(exTestMessage);
            }

            @Override public void onException(UUID nodeId, Throwable err) {
                assertEquals(exTestMessage, err.getMessage());
            }
        });

        try (GridIoManager.TransmissionSender sender = snd.context()
            .io()
            .openTransmissionSender(rcv.localNode().id(), topic)) {
            sender.send(fileToSend, TransmissionPolicy.FILE);
        }
    }

    /**
     * @throws Exception If fails.
     */
    @Test(expected = IgniteCheckedException.class)
    public void testFileHandlerOnReceiverLeft() throws Exception {
        final int fileSizeBytes = 5 * 1024 * 1024;
        final AtomicInteger chunksCnt = new AtomicInteger();

        IgniteEx snd = startGrid(0);
        IgniteEx rcv = startGrid(1);

        snd.cluster().active(true);

        File fileToSend = createFileRandomData("testFile", fileSizeBytes);

        snd.context().io().transfererFileIoFactory(new FileIOFactory() {
            @Override public FileIO create(File file, OpenOption... modes) throws IOException {
                FileIO fileIo = IO_FACTORY.create(file, modes);

                // Blocking writer and stopping node FileIo.
                return new FileIODecorator(fileIo) {
                    /** {@inheritDoc} */
                    @Override public long transferTo(long position, long count, WritableByteChannel target)
                        throws IOException {
                        // Send 5 chunks than stop the rcv.
                        if (chunksCnt.incrementAndGet() == 5)
                            stopGrid(rcv.name(), true);

                        return super.transferTo(position, count, target);
                    }
                };
            }
        });

        rcv.context().io().addTransmissionHandler(topic, new DefaultTransmissionHandler(rcv, fileToSend, tempStore));

        try (GridIoManager.TransmissionSender sender = snd.context()
            .io()
            .openTransmissionSender(rcv.localNode().id(), topic)) {
            sender.send(fileToSend, TransmissionPolicy.FILE);
        }
    }

    /**
     * @throws Exception If fails.
     */
    @Test
    public void testFileHandlerCleanedUpIfSenderLeft() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);

        IgniteEx snd = startGrid(0);
        IgniteEx rcv = startGrid(1);

        snd.cluster().active(true);

        File fileToSend = createFileRandomData("tempFile15Mb", 15 * 1024 * 1024);

        snd.context().io().transfererFileIoFactory(new FileIOFactory() {
            @Override public FileIO create(File file, OpenOption... modes) throws IOException {
                FileIO fileIo = IO_FACTORY.create(file, modes);

                return new FileIODecorator(fileIo) {
                    /** {@inheritDoc} */
                    @Override public long transferTo(long position, long count, WritableByteChannel target)
                        throws IOException {

                        long transferred = super.transferTo(position, count, target);

                        stopGrid(snd.name(), true);

                        return transferred;
                    }
                };
            }
        });

        rcv.context().io().addTransmissionHandler(topic, new DefaultTransmissionHandler(rcv, fileToSend, tempStore));

        try (GridIoManager.TransmissionSender sender = snd.context()
            .io()
            .openTransmissionSender(rcv.localNode().id(), topic)) {
            sender.send(fileToSend, TransmissionPolicy.FILE);
        }
        catch (IgniteCheckedException e) {
            // Ignore node stopping exception.
            U.log(log,"Expected node stopping exception", e);
        }

        assertEquals("Uncomplete resources must be cleaned up on sender left",
            1, // only fileToSend is expected to exist
            fileCount(tempStore.toPath()));
    }

    /**
     * @throws Exception If fails.
     */
    @Test(expected = IgniteCheckedException.class)
    public void testFileHandlerReconnectOnReadFail() throws Exception {
        final String chunkDownloadExMsg = "Test exception. Chunk processing error.";

        IgniteEx snd = startGrid(0);
        IgniteEx rcv = startGrid(1);

        snd.cluster().active(true);

        File fileToSend = createFileRandomData("testFile", 5 * 1024 * 1024);
        final AtomicInteger readedChunks = new AtomicInteger();

        rcv.context().io().transfererFileIoFactory(new FileIOFactory() {
            @Override public FileIO create(File file, OpenOption... modes) throws IOException {
                fileIo[0] = IO_FACTORY.create(file, modes);

                // Blocking writer and stopping node FileIo.
                return new FileIODecorator(fileIo[0]) {
                    @Override public long transferFrom(ReadableByteChannel src, long position, long count)
                        throws IOException {
                        // Read 4 chunks than throw an exception to emulate error processing.
                        if (readedChunks.incrementAndGet() == 4)
                            throw new IgniteException(chunkDownloadExMsg);

                        return super.transferFrom(src, position, count);
                    }
                };
            }
        });

        rcv.context().io().addTransmissionHandler(topic, new DefaultTransmissionHandler(rcv, fileToSend, tempStore) {
            @Override public void onException(UUID nodeId, Throwable err) {
                assertEquals(chunkDownloadExMsg, err.getMessage());
            }
        });

        try (GridIoManager.TransmissionSender sender = snd.context()
            .io()
            .openTransmissionSender(rcv.localNode().id(), topic)) {
            sender.send(fileToSend, TransmissionPolicy.FILE);
        }
    }

    /**
     * @throws Exception If fails.
     */
    @Test(expected = IgniteCheckedException.class)
    public void testFileHandlerReconnectOnInitFail() throws Exception {
        final int fileSizeBytes = 5 * 1024 * 1024;
        final AtomicBoolean throwFirstTime = new AtomicBoolean();

        IgniteEx snd = startGrid(0);
        IgniteEx rcv = startGrid(1);

        snd.cluster().active(true);

        File fileToSend = createFileRandomData("testFile", fileSizeBytes);
        File rcvFile = new File(tempStore, "testFile" + "_" + rcv.localNode().id());

        rcv.context().io().addTransmissionHandler(topic, new DefaultTransmissionHandler(rcv, fileToSend, tempStore) {
            @Override public String filePath(UUID nodeId, TransmissionMeta fileMeta) {
                if (throwFirstTime.compareAndSet(false, true))
                    throw new IgniteException("Test exception. Initialization fail.");

                return rcvFile.getAbsolutePath();
            }
        });

        try (GridIoManager.TransmissionSender sender = snd.context()
            .io()
            .openTransmissionSender(rcv.localNode().id(), topic)) {
            sender.send(fileToSend, TransmissionPolicy.FILE);
        }

        assertEquals(fileToSend.length(), rcvFile.length());
        assertCrcEquals(fileToSend, rcvFile);
    }

    /**
     * @throws Exception If fails.
     */
    @Test
    public void testFileHandlerNextWriterOpened() throws Exception {
        final int fileSizeBytes = 5 * 1024 * 1024;
        final AtomicBoolean networkExThrown = new AtomicBoolean();

        IgniteEx snd = startGrid(0);
        IgniteEx rcv = startGrid(1);

        snd.cluster().active(true);

        File fileToSend = createFileRandomData("File_5MB", fileSizeBytes);
        File rcvFile = new File(tempStore, "File_5MB" + "_" + rcv.localNode().id());

        rcv.context().io().addTransmissionHandler(topic, new DefaultTransmissionHandler(rcv, fileToSend, tempStore) {
            @Override public void onException(UUID nodeId, Throwable err) {
                assertEquals("Previous session is not closed properly", IgniteCheckedException.class, err.getClass());
                assertTrue(err.getMessage().startsWith("The handler has been aborted"));
            }

            @Override public String filePath(UUID nodeId, TransmissionMeta fileMeta) {
                if (networkExThrown.compareAndSet(false, true))
                    return null;

                return rcvFile.getAbsolutePath();
            }
        });

        try (GridIoManager.TransmissionSender sender = snd.context()
            .io()
            .openTransmissionSender(rcv.localNode().id(), topic)) {
            sender.send(fileToSend, TransmissionPolicy.FILE);
        }
        catch (IgniteCheckedException e) {
            // Expected exception.
            assertTrue(e.toString(), e.getCause().getMessage().startsWith("Channel processing error"));
        }

        //Open next session and complete successfull.
        try (GridIoManager.TransmissionSender sender = snd.context()
            .io()
            .openTransmissionSender(rcv.localNode().id(), topic)) {
            sender.send(fileToSend, TransmissionPolicy.FILE);
        }

        assertEquals(fileToSend.length(), rcvFile.length());
        assertCrcEquals(fileToSend, rcvFile);

        // Remove topic handler and fail
        rcv.context().io().removeTransmissionHandler(topic);

        IgniteCheckedException err = null;

        // Open next writer on removed topic.
        try (GridIoManager.TransmissionSender sender = snd.context()
            .io()
            .openTransmissionSender(rcv.localNode().id(), topic)) {
            sender.send(fileToSend, TransmissionPolicy.FILE);
        }
        catch (IgniteCheckedException e) {
            // Must catch execption here.
            err = e;
        }

        assertNotNull(err);
    }

    /**
     * @throws Exception If fails.
     */
    @Test(expected = IgniteCheckedException.class)
    public void testFileHandlerChannelCloseIfAnotherOpened() throws Exception {
        final int fileSizeBytes = 5 * 1024 * 1024;
        final CountDownLatch waitLatch = new CountDownLatch(2);
        final CountDownLatch completionWait = new CountDownLatch(2);

        IgniteEx snd = startGrid(0);
        IgniteEx rcv = startGrid(1);

        snd.cluster().active(true);

        File fileToSend = createFileRandomData("file5MBSize", fileSizeBytes);

        rcv.context().io().addTransmissionHandler(topic, new DefaultTransmissionHandler(rcv, fileToSend, tempStore) {
            @Override public void onBegin(UUID nodeId) {
                waitLatch.countDown();

                try {
                    waitLatch.await(5, TimeUnit.SECONDS);
                }
                catch (InterruptedException e) {
                    throw new IgniteException(e);
                }
            }
        });

        IgniteCheckedException[] errs = new IgniteCheckedException[1];

        try (GridIoManager.TransmissionSender sender = snd.context()
            .io()
            .openTransmissionSender(rcv.localNode().id(), topic);
             GridIoManager.TransmissionSender anotherSender = snd.context()
                 .io()
                 .openTransmissionSender(rcv.localNode().id(), topic)) {
            // Will connect on write attempt.
            GridTestUtils.runAsync(() -> {
                try {
                    sender.send(fileToSend, TransmissionPolicy.FILE);
                }
                catch (IgniteCheckedException e) {
                    errs[0] = e;
                }
                finally {
                    completionWait.countDown();
                }
            });

            GridTestUtils.runAsync(() -> {
                try {
                    anotherSender.send(fileToSend, TransmissionPolicy.FILE);
                }
                catch (IgniteCheckedException e) {
                    errs[0] = e;
                }
                finally {
                    completionWait.countDown();
                }
            });

            waitLatch.await(5, TimeUnit.SECONDS);

            // Expected that one of the writers will throw exception.
            assertFalse("An error must be thrown if connected to the same topic during processing",
                errs[0] == null);

            completionWait.await(5, TimeUnit.SECONDS);

            throw errs[0];
        }
    }

    /**
     * @throws Exception If fails.
     */
    @Test
    public void testChunkHandlerWithReconnect() throws Exception {
        IgniteEx snd = startGrid(0);
        IgniteEx rcv = startGrid(1);

        final String filePrefix = "testFile";
        final AtomicInteger cnt = new AtomicInteger();
        final AtomicInteger acceptedChunks = new AtomicInteger();
        final File file = new File(tempStore, filePrefix + "_" + rcv.localNode().id());

        snd.cluster().active(true);

        File fileToSend = createFileRandomData(filePrefix, 10 * 1024 * 1024);

        snd.context().io().transfererFileIoFactory(new FileIOFactory() {
            @Override public FileIO create(File file, OpenOption... modes) throws IOException {
                FileIO fileIo = IO_FACTORY.create(file, modes);

                return new FileIODecorator(fileIo) {
                    /** {@inheritDoc} */
                    @Override public long transferTo(long position, long count, WritableByteChannel target)
                        throws IOException {
                        // Send 5 chunks and close the channel.
                        if (cnt.incrementAndGet() == 10)
                            target.close();

                        return super.transferTo(position, count, target);
                    }
                };
            }
        });

        rcv.context().io().addTransmissionHandler(topic, new TransmissionHandlerAdapter() {
            /** {@inheritDoc} */
            @Override public void onException(UUID nodeId, Throwable err) {
                U.closeQuiet(fileIo[0]);

                fileIo[0] = null;
            }

            /** {@inheritDoc} */
            @Override public IgniteThrowableConsumer<ByteBuffer> chunkHandler(UUID nodeId, TransmissionMeta initMeta)
                throws IgniteCheckedException {

                if (fileIo[0] == null) {
                    try {
                        fileIo[0] = IO_FACTORY.create(file);
                        fileIo[0].position(initMeta.offset());
                    }
                    catch (IOException e) {
                        throw new IgniteCheckedException(e);
                    }
                }

                return new IgniteThrowableConsumer<ByteBuffer>() {
                    final LongAdder transferred = new LongAdder();

                    @Override public void accept(ByteBuffer buff) throws IgniteCheckedException {
                        try {
                            assertTrue(buff.order() == ByteOrder.nativeOrder());
                            assertEquals(0, buff.position());
                            assertEquals(buff.limit(), buff.capacity());

                            fileIo[0].writeFully(buff);

                            acceptedChunks.getAndIncrement();
                            transferred.add(buff.capacity());
                        }
                        catch (Throwable e) {
                            throw new IgniteCheckedException(e);
                        }
                        finally {
                            closeIfTransferred();
                        }
                    }

                    private void closeIfTransferred() {
                        if (transferred.longValue() == initMeta.count()) {
                            U.closeQuiet(fileIo[0]);

                            fileIo[0] = null;
                        }
                    }
                };
            }
        });

        try (GridIoManager.TransmissionSender sender = snd.context()
            .io()
            .openTransmissionSender(rcv.localNode().id(), topic)) {
            sender.send(fileToSend, TransmissionPolicy.CHUNK);
        }

        assertEquals("Total number of accepted chunks by remote node is not as expected",
            fileToSend.length() / rcv.configuration().getDataStorageConfiguration().getPageSize(),
            acceptedChunks.get());
        assertEquals("Received file and sent files have not the same lenght", fileToSend.length(), file.length());
        assertCrcEquals(fileToSend, file);
        assertNull(fileIo[0]);
    }

    /**
     * @throws Exception If fails.
     */
    @Test(expected = IgniteCheckedException.class)
    public void testChunkHandlerInitSizeFail() throws Exception {
        IgniteEx snd = startGrid(0);
        IgniteEx rcv = startGrid(1);

        snd.cluster().active(true);

        File fileToSend = createFileRandomData("testFile", 1024 * 1024);

        rcv.context().io().addTransmissionHandler(topic, new TransmissionHandlerAdapter() {
            /** {@inheritDoc} */
            @Override public IgniteThrowableConsumer<ByteBuffer> chunkHandler(UUID nodeId, TransmissionMeta initMeta) {
                throw new IgniteException("Test exception. Initialization failed");
            }
        });

        try (GridIoManager.TransmissionSender sender = snd.context()
            .io()
            .openTransmissionSender(rcv.localNode().id(), topic)) {
            sender.send(fileToSend, TransmissionPolicy.CHUNK);
        }
    }

    /**
     * @param ignite Ignite instance.
     * @param cacheName Cache name to add data to.
     */
    private void addCacheData(Ignite ignite, String cacheName) {
        try (IgniteDataStreamer<Integer, Integer> dataStreamer = ignite.dataStreamer(cacheName)) {
            dataStreamer.allowOverwrite(true);

            for (int i = 0; i < CACHE_SIZE; i++) {
                if ((i + 1) % (CACHE_SIZE / 10) == 0)
                    log.info("Prepared " + (i + 1) * 100 / (CACHE_SIZE) + "% entries.");

                dataStreamer.addData(i, i + cacheName.hashCode());
            }
        }
    }

    /**
     * @param ignite The ignite instance.
     * @param cacheName Cache name string representation.
     * @return The cache working directory.
     */
    private File cacheWorkDir(IgniteEx ignite, String cacheName) {
        // Resolve cache directory
        IgniteInternalCache<?, ?> cache = ignite.cachex(cacheName);

        FilePageStoreManager pageStoreMgr = (FilePageStoreManager)cache.context()
            .shared()
            .pageStore();

        return pageStoreMgr.cacheWorkDir(cache.configuration());
    }

    /**
     * @param name The file name to create.
     * @param size The file size.
     * @throws IOException If fails.
     */
    private File createFileRandomData(String name, final int size) throws IOException {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        File out = new File(tempStore, name);

        try (RandomAccessFile raf = new RandomAccessFile(out, "rw")) {
            byte[] buf = new byte[size];
            rnd.nextBytes(buf);
            raf.write(buf);
        }

        return out;
    }

    /**
     * @param fileToSend Source file to check CRC.
     * @param fileReceived Destination file to check CRC.
     */
    private static void assertCrcEquals(File fileToSend, File fileReceived) {
        try {
            assertEquals(FastCrc.calcCrc(fileToSend), FastCrc.calcCrc(fileReceived));
        }
        catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    /**
     * The defailt implementation of transmit session.
     */
    private static class DefaultTransmissionHandler extends TransmissionHandlerAdapter {
        /** Ignite recevier node. */
        private final IgniteEx rcv;

        /** File to be send. */
        private final File fileToSend;

        /** Temporary local storage. */
        private final File tempStorage;

        /**
         * @param rcv Ignite recevier node.
         * @param fileToSend File to be send.
         * @param tempStorage Temporary local storage.
         */
        public DefaultTransmissionHandler(IgniteEx rcv, File fileToSend, File tempStorage) {
            this.rcv = rcv;
            this.fileToSend = fileToSend;
            this.tempStorage = tempStorage;
        }

        /** {@inheritDoc} */
        @Override public String filePath(UUID nodeId, TransmissionMeta fileMeta) {
            return new File(tempStorage, fileMeta.name() + "_" + rcv.localNode().id()).getAbsolutePath();
        }

        /** {@inheritDoc} */
        @Override public IgniteThrowableConsumer<File> fileHandler(UUID nodeId, TransmissionMeta initMeta)
            throws IgniteCheckedException {
            return new IgniteThrowableConsumer<File>() {
                @Override public void accept(File file) {
                    assertEquals(fileToSend.length(), file.length());
                    assertCrcEquals(fileToSend, file);
                }
            };
        }
    }

    /**
     * The defailt implementation of transmit session.
     */
    private static class TransmissionHandlerAdapter implements TransmissionHandler {
        /** {@inheritDoc} */
        @Override public void onBegin(UUID nodeId) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public String filePath(UUID nodeId, TransmissionMeta fileMeta) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public IgniteThrowableConsumer<ByteBuffer> chunkHandler(UUID nodeId, TransmissionMeta initMeta)
            throws IgniteCheckedException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public IgniteThrowableConsumer<File> fileHandler(UUID nodeId, TransmissionMeta initMeta)
            throws IgniteCheckedException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void onEnd(UUID nodeId) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void onException(UUID nodeId, Throwable err) {
            // No-op.
        }
    }
}

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
import java.nio.channels.Channel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.OpenOption;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIODecorator;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.wal.crc.FastCrc;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.FILE_SUFFIX;
import static org.apache.ignite.internal.util.IgniteUtils.fileCount;
import static org.apache.ignite.testframework.GridTestUtils.setFieldValue;

/**
 * Test file transmission manager operations.
 */
public class GridIoManagerFileTransmissionSelfTest extends GridCommonAbstractTest {
    /** Number of cache keys to generate. */
    private static final long CACHE_SIZE = 50_000L;

    /** Network timeout in ms. */
    private static final long NET_TIMEOUT_MS = 2000L;

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

    /** Ignite instance which receives files. */
    private IgniteEx rcv;

    /** Ignite instance which sends files. */
    private IgniteEx snd;

    /** {@code false} for most of tests persistence can be disabled. */
    private boolean enablePersistence;

    /** Called before tests started. */
    @BeforeClass
    public static void beforeAll() {
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

    /** Called after test run. */
    @After
    public void after() {
        try {
            ensureResourcesFree(snd);
            ensureResourcesFree(rcv);
        }
        finally {
            stopAllGrids();

            U.closeQuiet(fileIo[0]);

            snd = null;
            rcv = null;
        }
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setPersistenceEnabled(enablePersistence)
                    .setMaxSize(500L * 1024 * 1024)))
            .setCacheConfiguration(new CacheConfiguration<Integer, Integer>(DEFAULT_CACHE_NAME))
            .setCommunicationSpi(new BlockingOpenChannelCommunicationSpi())
            .setNetworkTimeout(NET_TIMEOUT_MS);
    }

    /**
     * Transmit all cache partition to particular topic on the remote node.
     *
     * @throws Exception If fails.
     */
    @Test
    public void testFileHandlerBase() throws Exception {
        enablePersistence = true;

        snd = startGrid(0);
        rcv = startGrid(1);

        snd.cluster().active(true);

        addCacheData(snd, DEFAULT_CACHE_NAME);

        awaitPartitionMapExchange();

        Map<String, Long> fileSizes = new HashMap<>();
        Map<String, Integer> fileCrcs = new HashMap<>();
        Map<String, Serializable> fileParams = new HashMap<>();

        assertTrue(snd.context().io().fileTransmissionSupported(rcv.localNode()));

        rcv.context().io().addTransmissionHandler(topic, new TransmissionHandlerAdapter() {
            @Override public void onEnd(UUID rmtNodeId) {
                ensureResourcesFree(snd);
                ensureResourcesFree(rcv);
            }

            @Override public String filePath(UUID nodeId, TransmissionMeta fileMeta) {
                return new File(tempStore, fileMeta.name()).getAbsolutePath();
            }

            @Override public Consumer<File> fileHandler(UUID nodeId, TransmissionMeta initMeta) {
                return new Consumer<File>() {
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

                sender.send(file, params, TransmissionPolicy.FILE);
            }
        }

        stopAllGrids();

        assertEquals(fileSizes.size(), tempStore.listFiles(fileBinFilter).length);

        for (File file : cacheParts) {
            // Check received file lengths.
            assertEquals("Received the file length is incorrect: " + file.getName(),
                fileSizes.get(file.getName()), new Long(file.length()));

            // Check received params.
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
    public void testFileHandlerFilePathThrowsEx() throws Exception {
        final String exTestMessage = "Test exception. Handler initialization failed at onBegin.";

        snd = startGrid(0);
        rcv = startGrid(1);

        File fileToSend = createFileRandomData("1Mb", 1024 * 1024);

        rcv.context().io().addTransmissionHandler(topic, new DefaultTransmissionHandler(rcv, fileToSend, tempStore) {
            @Override public String filePath(UUID nodeId, TransmissionMeta fileMeta) {
                throw new IgniteException(exTestMessage);
            }

            @Override public Consumer<File> fileHandler(UUID nodeId, TransmissionMeta initMeta) {
                fail("fileHandler must never be called");

                return super.fileHandler(nodeId, initMeta);
            }

            @Override public Consumer<ByteBuffer> chunkHandler(UUID nodeId, TransmissionMeta initMeta) {
                fail("chunkHandler must never be called");

                return super.chunkHandler(nodeId, initMeta);
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

        snd = startGrid(0);
        rcv = startGrid(1);

        File fileToSend = createFileRandomData("testFile", fileSizeBytes);

        transmissionFileIoFactory(snd, new FileIOFactory() {
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
    public void tesFileHandlerReconnectTimeouted() throws Exception {
        rcv = startGrid(1);
        snd = startGrid(0);

        final AtomicInteger chunksCnt = new AtomicInteger();
        final CountDownLatch sndLatch = ((BlockingOpenChannelCommunicationSpi)snd.context()
            .config()
            .getCommunicationSpi()).latch;
        final AtomicReference<Throwable> refErr = new AtomicReference<>();

        File fileToSend = createFileRandomData("testFile", 5 * 1024 * 1024);

        transmissionFileIoFactory(snd, new FileIOFactory() {
            @Override public FileIO create(File file, OpenOption... modes) throws IOException {
                FileIO fileIo = IO_FACTORY.create(file, modes);

                return new FileIODecorator(fileIo) {
                    /** {@inheritDoc} */
                    @Override public long transferTo(long position, long count, WritableByteChannel target)
                        throws IOException {
                        if (chunksCnt.incrementAndGet() == 10) {
                            target.close();

                            ((BlockingOpenChannelCommunicationSpi)snd.context()
                                .config()
                                .getCommunicationSpi()).block = true;
                        }

                        return super.transferTo(position, count, target);
                    }
                };
            }
        });

        rcv.context().io().addTransmissionHandler(topic, new DefaultTransmissionHandler(rcv, fileToSend, tempStore) {
            @Override public void onException(UUID nodeId, Throwable err) {
                refErr.compareAndSet(null, err);

                sndLatch.countDown();
            }
        });

        try (GridIoManager.TransmissionSender sender = snd.context()
            .io()
            .openTransmissionSender(rcv.localNode().id(), topic)) {
            sender.send(fileToSend, TransmissionPolicy.FILE);
        }
        catch (IgniteCheckedException | IOException | InterruptedException e) {
            // Ignore err.
            U.warn(log, e);
        }

        assertNotNull("Timeout exception not occurred", refErr.get());
        assertEquals("Type of timeout exception incorrect: " + refErr.get(),
            IgniteCheckedException.class,
            refErr.get().getClass());
    }

    /**
     * @throws Exception If fails.
     */
    @Test
    public void testFileHandlerCleanedUpIfSenderLeft() throws Exception {
        snd = startGrid(0);
        rcv = startGrid(1);

        File fileToSend = createFileRandomData("tempFile15Mb", 15 * 1024 * 1024);
        File downloadTo = U.resolveWorkDirectory(tempStore.getAbsolutePath(), "download", true);

        transmissionFileIoFactory(snd, new FileIOFactory() {
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

        rcv.context().io().addTransmissionHandler(topic, new DefaultTransmissionHandler(rcv, fileToSend, tempStore) {
            /** {@inheritDoc} */
            @Override public String filePath(UUID nodeId, TransmissionMeta fileMeta) {
                return new File(downloadTo, fileMeta.name()).getAbsolutePath();
            }
        });

        Exception err = null;

        try (GridIoManager.TransmissionSender sender = snd.context()
            .io()
            .openTransmissionSender(rcv.localNode().id(), topic)) {
            sender.send(fileToSend, TransmissionPolicy.FILE);
        }
        catch (Exception e) {
            // Ignore node stopping exception.
            err = e;
        }

        assertEquals(NodeStoppingException.class, err.getClass());
        assertEquals("Incomplete resources must be cleaned up on sender left",
            0,
            fileCount(downloadTo.toPath()));
    }

    /**
     * @throws Exception If fails.
     */
    @Test(expected = IgniteCheckedException.class)
    public void testFileHandlerReconnectOnReadFail() throws Exception {
        final String chunkDownloadExMsg = "Test exception. Chunk processing error.";

        snd = startGrid(0);
        rcv = startGrid(1);

        File fileToSend = createFileRandomData("testFile", 5 * 1024 * 1024);
        final AtomicInteger readChunks = new AtomicInteger();

        transmissionFileIoFactory(rcv, new FileIOFactory() {
            @Override public FileIO create(File file, OpenOption... modes) throws IOException {
                fileIo[0] = IO_FACTORY.create(file, modes);

                // Blocking writer and stopping node FileIo.
                return new FileIODecorator(fileIo[0]) {
                    @Override public long transferFrom(ReadableByteChannel src, long position, long count)
                        throws IOException {
                        // Read 4 chunks than throw an exception to emulate error processing.
                        if (readChunks.incrementAndGet() == 4)
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
    public void testFileHandlerSenderStoppedIfReceiverInitFail() throws Exception {
        final int fileSizeBytes = 5 * 1024 * 1024;
        final AtomicBoolean throwFirstTime = new AtomicBoolean();

        snd = startGrid(0);
        rcv = startGrid(1);

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

        snd = startGrid(0);
        rcv = startGrid(1);

        File fileToSend = createFileRandomData("File_5MB", fileSizeBytes);
        File rcvFile = new File(tempStore, "File_5MB" + "_" + rcv.localNode().id());

        rcv.context().io().addTransmissionHandler(topic, new DefaultTransmissionHandler(rcv, fileToSend, tempStore) {
            @Override public void onException(UUID nodeId, Throwable err) {
                assertEquals("Previous session is not closed properly", IgniteCheckedException.class, err.getClass());
            }

            @Override public String filePath(UUID nodeId, TransmissionMeta fileMeta) {
                if (networkExThrown.compareAndSet(false, true))
                    return null;

                return rcvFile.getAbsolutePath();
            }
        });

        Exception expectedErr = null;

        try (GridIoManager.TransmissionSender sender = snd.context()
            .io()
            .openTransmissionSender(rcv.localNode().id(), topic)) {
            sender.send(fileToSend, TransmissionPolicy.FILE);
        }
        catch (IgniteCheckedException e) {
            // Expected exception.
            expectedErr = e;
        }

        assertNotNull("Transmission must ends with an exception", expectedErr);

        // Open next session and complete successfull.
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
    @Test(expected = IgniteException.class)
    public void testFileHandlerSendToNullTopic() throws Exception {
        snd = startGrid(0);
        rcv = startGrid(1);

        // Ensure topic handler is empty.
        rcv.context().io().removeTransmissionHandler(topic);

        // Open next writer on removed topic.
        try (GridIoManager.TransmissionSender sender = snd.context()
            .io()
            .openTransmissionSender(rcv.localNode().id(), topic)) {
            sender.send(createFileRandomData("File_1MB", 1024 * 1024), TransmissionPolicy.FILE);
        }
    }

    /**
     * @throws Exception If fails.
     */
    @Test(expected = IgniteCheckedException.class)
    public void testFileHandlerChannelCloseIfAnotherOpened() throws Exception {
        final int fileSizeBytes = 5 * 1024 * 1024;
        final CountDownLatch waitLatch = new CountDownLatch(2);
        final CountDownLatch completionWait = new CountDownLatch(2);

        snd = startGrid(0);
        rcv = startGrid(1);

        File fileToSend = createFileRandomData("file5MBSize", fileSizeBytes);

        rcv.context().io().addTransmissionHandler(topic, new DefaultTransmissionHandler(rcv, fileToSend, tempStore) {
            @Override public String filePath(UUID nodeId, TransmissionMeta fileMeta) {
                waitLatch.countDown();

                return super.filePath(nodeId, fileMeta);
            }
        });

        Exception[] errs = new Exception[1];

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
                catch (IgniteCheckedException | IOException | InterruptedException e) {
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
                catch (IgniteCheckedException | IOException | InterruptedException e) {
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
        snd = startGrid(0);
        rcv = startGrid(1);

        final String filePrefix = "testFile";
        final AtomicInteger cnt = new AtomicInteger();
        final AtomicInteger acceptedChunks = new AtomicInteger();
        final File file = new File(tempStore, filePrefix + "_" + rcv.localNode().id());

        File fileToSend = createFileRandomData(filePrefix, 10 * 1024 * 1024);

        transmissionFileIoFactory(snd, new FileIOFactory() {
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
            @Override public void onEnd(UUID rmtNodeId) {
                U.closeQuiet(fileIo[0]);

                fileIo[0] = null;
            }

            /** {@inheritDoc} */
            @Override public void onException(UUID nodeId, Throwable err) {
                U.closeQuiet(fileIo[0]);

                fileIo[0] = null;
            }

            /** {@inheritDoc} */
            @Override public Consumer<ByteBuffer> chunkHandler(UUID nodeId, TransmissionMeta initMeta) {

                if (fileIo[0] == null) {
                    try {
                        fileIo[0] = IO_FACTORY.create(file);
                        fileIo[0].position(initMeta.offset());
                    }
                    catch (IOException e) {
                        throw new IgniteException(e);
                    }
                }

                return new Consumer<ByteBuffer>() {
                    @Override public void accept(ByteBuffer buff) {
                        try {
                            assertTrue(buff.order() == ByteOrder.nativeOrder());
                            assertEquals(0, buff.position());
                            assertEquals(buff.limit(), buff.capacity());

                            fileIo[0].writeFully(buff);

                            acceptedChunks.getAndIncrement();
                        }
                        catch (Throwable e) {
                            throw new IgniteException(e);
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

        assertEquals("Remote node must accept all chunks",
            fileToSend.length() / rcv.configuration().getDataStorageConfiguration().getPageSize(),
            acceptedChunks.get());
        assertEquals("Received file and sent files have not the same lengtgh", fileToSend.length(), file.length());
        assertCrcEquals(fileToSend, file);
        assertNull(fileIo[0]);
    }

    /**
     * @throws Exception If fails.
     */
    @Test(expected = IgniteCheckedException.class)
    public void testChunkHandlerInitSizeFail() throws Exception {
        snd = startGrid(0);
        rcv = startGrid(1);

        File fileToSend = createFileRandomData("testFile", 1024 * 1024);

        rcv.context().io().addTransmissionHandler(topic, new TransmissionHandlerAdapter() {
            /** {@inheritDoc} */
            @Override public Consumer<ByteBuffer> chunkHandler(UUID nodeId, TransmissionMeta initMeta) {
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
     * @throws Exception If fails.
     */
    @Test(expected = TransmissionCancelledException.class)
    public void testChunkHandlerCancelTransmission() throws Exception {
        snd = startGrid(0);
        rcv = startGrid(1);

        snd.cluster().active(true);

        File fileToSend = createFileRandomData("testFile", 1024 * 1024);

        rcv.context().io().addTransmissionHandler(topic, new TransmissionHandlerAdapter() {
            /** {@inheritDoc} */
            @Override public Consumer<ByteBuffer> chunkHandler(UUID nodeId, TransmissionMeta initMeta) {
                return new Consumer<ByteBuffer>() {
                    @Override public void accept(ByteBuffer buffer) {
                        throw new TransmissionCancelledException("Operation cancelled by the user");
                    }
                };
            }
        });

        try (GridIoManager.TransmissionSender sender = snd.context()
            .io()
            .openTransmissionSender(rcv.localNode().id(), topic)) {
            sender.send(fileToSend, TransmissionPolicy.CHUNK);
        }
        catch (TransmissionCancelledException e) {
            log.warning("Transmission cancelled", e);

            throw e;
        }
    }

    /**
     * @throws Exception If fails.
     */
    @Test
    public void testFileHandlerCrossConnections() throws Exception {
        snd = startGrid(0);
        rcv = startGrid(1);

        File from0To1 = createFileRandomData("1Mb_1_0", 1024 * 1024);
        File from1To0 = createFileRandomData("1Mb_0_1", 1024 * 1024);

        CountDownLatch touched = new CountDownLatch(2);

        snd.context().io().addTransmissionHandler(topic, new TransmissionHandlerAdapter() {
            @Override public String filePath(UUID nodeId, TransmissionMeta fileMeta) {
                return new File(tempStore, fileMeta.name()).getAbsolutePath();
            }

            @Override public Consumer<File> fileHandler(UUID nodeId, TransmissionMeta initMeta) {
                return new Consumer<File>() {
                    @Override public void accept(File file) {
                        assertEquals(from1To0.getName(), file.getName());
                    }
                };
            }
        });

        rcv.context().io().addTransmissionHandler(topic, new TransmissionHandlerAdapter() {
            @Override public String filePath(UUID nodeId, TransmissionMeta fileMeta) {
                return new File(tempStore, fileMeta.name()).getAbsolutePath();
            }

            @Override public Consumer<File> fileHandler(UUID nodeId, TransmissionMeta initMeta) {
                return new Consumer<File>() {
                    @Override public void accept(File file) {
                        assertEquals(from0To1.getName(), file.getName());
                    }
                };
            }
        });

        Exception[] ex = new Exception[1];

        GridTestUtils.runAsync(() -> {
            try (GridIoManager.TransmissionSender snd0 = snd.context()
                .io()
                .openTransmissionSender(rcv.localNode().id(), topic)) {
                // Iterate over cache partition cacheParts.
                snd0.send(from0To1, TransmissionPolicy.FILE);
            }
            catch (Exception e) {
                log.error("Send fail from 0 to 1", e);

                ex[0] = e;
            }

            touched.countDown();
        });

        GridTestUtils.runAsync(() -> {
            try (GridIoManager.TransmissionSender snd1 = rcv.context()
                .io()
                .openTransmissionSender(snd.localNode().id(), topic)) {
                // Iterate over cache partition cacheParts.
                snd1.send(from1To0, TransmissionPolicy.FILE);
            }
            catch (Exception e) {
                log.error("Send fail from 1 to 0", e);

                ex[0] = e;
            }

            touched.countDown();
        });

        touched.await(10_000L, TimeUnit.MILLISECONDS);

        assertNull("Exception occurred during file sending: " + ex[0], ex[0]);
    }

    /**
     * @param ig Ignite instance to check.
     */
    private static void ensureResourcesFree(IgniteEx ig) {
        if (ig == null)
            return;

        final GridIoManager io = ig.context().io();

        ConcurrentMap<Object, Object> ctxs = GridTestUtils.getFieldValue(io, "rcvCtxs");
        ConcurrentMap<T2<UUID, IgniteUuid>, AtomicBoolean> sndrFlags = GridTestUtils.getFieldValue(io, "senderStopFlags");

        assertTrue("Receiver context map must be empty: " + ctxs, ctxs.isEmpty());
        assertTrue("Sender stop flags must be empty: " + sndrFlags, sndrFlags.isEmpty());
    }

    /**
     * @param ignite Ignite instance.
     * @param cacheName Cache name to add data to.
     */
    private void addCacheData(Ignite ignite, String cacheName) {
        try (IgniteDataStreamer<Integer, Integer> dataStreamer = ignite.dataStreamer(cacheName)) {
            dataStreamer.allowOverwrite(true);

            for (int i = 0; i < CACHE_SIZE; i++)
                dataStreamer.addData(i, i + cacheName.hashCode());
        }
    }

    /**
     * @param ignite An ignite instance.
     * @param cacheName Cache name.
     * @return The cache working directory.
     */
    private File cacheWorkDir(IgniteEx ignite, String cacheName) {
        // Resolve cache directory.
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
     * @param ignite Ignite instance to set factory.
     * @param factory New factory to use.
     */
    private static void transmissionFileIoFactory(IgniteEx ignite, FileIOFactory factory) {
        setFieldValue(ignite.context().io(), "fileIoFactory", factory);
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

    /** The defailt implementation of transmit session. */
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
        @Override public Consumer<File> fileHandler(UUID nodeId, TransmissionMeta initMeta) {
            return new Consumer<File>() {
                @Override public void accept(File file) {
                    assertEquals(fileToSend.length(), file.length());
                    assertCrcEquals(fileToSend, file);
                }
            };
        }
    }

    /** */
    private static class BlockingOpenChannelCommunicationSpi extends TcpCommunicationSpi {
        /** Latch to wait at. */
        private final CountDownLatch latch = new CountDownLatch(1);

        /** {@code true} to start waiting. */
        private volatile boolean block;

        /** {@inheritDoc} */
        @Override public IgniteInternalFuture<Channel> openChannel(ClusterNode remote,
            Message initMsg) throws IgniteSpiException {
            try {
                if (block) {
                    U.log(log, "Start waiting on trying open a new channel");

                    latch.await(5, TimeUnit.SECONDS);
                }
            }
            catch (InterruptedException e) {
                throw new IgniteException(e);
            }

            return super.openChannel(remote, initMsg);
        }
    }

    /**
     * The defailt implementation of transmit session.
     */
    private static class TransmissionHandlerAdapter implements TransmissionHandler {
        /** {@inheritDoc} */
        @Override public void onEnd(UUID rmtNodeId) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public String filePath(UUID nodeId, TransmissionMeta fileMeta) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public Consumer<ByteBuffer> chunkHandler(UUID nodeId, TransmissionMeta initMeta) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public Consumer<File> fileHandler(UUID nodeId, TransmissionMeta initMeta) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void onException(UUID nodeId, Throwable err) {
            // No-op.
        }
    }
}

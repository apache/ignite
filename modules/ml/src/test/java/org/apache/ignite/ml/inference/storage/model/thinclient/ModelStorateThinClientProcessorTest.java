/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.ml.inference.storage.model.thinclient;

import java.util.Arrays;
import java.util.Collections;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.binary.BinaryContext;
import org.apache.ignite.internal.binary.BinaryNoopMetadataHandler;
import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.internal.binary.BinaryWriterHandles;
import org.apache.ignite.internal.binary.BinaryWriterSchemaHolder;
import org.apache.ignite.internal.binary.streams.BinaryHeapInputStream;
import org.apache.ignite.internal.binary.streams.BinaryHeapOutputStream;
import org.apache.ignite.internal.binary.streams.BinaryOutputStream;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.odbc.ClientListenerProtocolVersion;
import org.apache.ignite.internal.processors.platform.client.ClientAffinityTopologyVersion;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientCustomQueryRequest;
import org.apache.ignite.internal.processors.platform.client.ClientResponse;
import org.apache.ignite.internal.processors.platform.client.ClientStatus;
import org.apache.ignite.internal.processors.platform.client.ThinClientCustomQueryRegistry;
import org.apache.ignite.logger.NullLogger;
import org.apache.ignite.ml.inference.storage.model.DefaultModelStorage;
import org.apache.ignite.ml.inference.storage.model.FileOrDirectory;
import org.apache.ignite.ml.inference.storage.model.FileStat;
import org.apache.ignite.ml.inference.storage.model.IgniteModelStorageProvider;
import org.apache.ignite.ml.inference.storage.model.ModelStorage;
import org.apache.ignite.ml.inference.storage.model.ModelStorageFactory;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for ModelStorateThinClientProcessor.
 */
public class ModelStorateThinClientProcessorTest extends GridCommonAbstractTest {
    /** */
    private static final ClientConnectionContext connCtx = mock(ClientConnectionContext.class);

    static {
        when(connCtx.currentVersion()).thenReturn(ClientListenerProtocolVersion.create(1, 4, 0));
        when(connCtx.checkAffinityTopologyVersion()).thenReturn(new ClientAffinityTopologyVersion(AffinityTopologyVersion.ZERO, false));
    }

    /** */
    private static final IgniteConfiguration conf = new IgniteConfiguration()
        .setDiscoverySpi(new TcpDiscoverySpi()
            .setIpFinder(new TcpDiscoveryVmIpFinder()
                .setAddresses(Collections.singletonList("127.0.0.1:47500..47509"))));

    /** */
    private static final BinaryContext bctx = new BinaryContext(
        BinaryNoopMetadataHandler.instance(),
        conf,
        new NullLogger()
    );

    /** */
    private static final byte[] file1 = new byte[] {0, 1, 2, 3, 4, 5};

    /** */
    private static final String pathToFile1 = "/a/b/file_1";

    /** */
    private Ignite ignite;

    /** */
    private ModelStorage ms;

    /** */
    private ModelStorateThinClientProcessor msp;

    /** */
    private IgniteCache<String, FileOrDirectory> cache;

    @Before
    public void setUp() throws Exception {
        ThinClientCustomQueryRegistry.unregister(ModelStorateThinClientProcessor.PROCESSOR_ID);

        ignite = startGrid(0);

        CacheConfiguration<String, FileOrDirectory> storageCfg = new CacheConfiguration<>();

        storageCfg.setName(ModelStorageFactory.MODEL_STORAGE_CACHE_NAME);
        storageCfg.setCacheMode(CacheMode.PARTITIONED);
        storageCfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        cache = ignite.createCache(storageCfg);

        ms = new DefaultModelStorage(new IgniteModelStorageProvider(cache));
        msp = new ModelStorateThinClientProcessor(ms);

        ThinClientCustomQueryRegistry.registerIfAbsent(msp);

        ms.mkdirs(pathToFile1);
        ms.putFile(pathToFile1, file1);
    }

    @After
    public void tearDown() throws Exception {
        ThinClientCustomQueryRegistry.unregister(msp);
        stopAllGrids();
    }

    /** */
    @Test
    public void testReadFile() {
        long reqId = 1;
        BinaryRawWriterEx message = createMessage(reqId, ModelStorateThinClientProcessor.PROCESSOR_ID, ModelStorateThinClientProcessor.Method.READ_FILE);
        message.writeString(pathToFile1);
        ClientCustomQueryRequest req = new ClientCustomQueryRequest(toReader(message));
        ClientResponse resp = req.process(connCtx);
        assertArrayEquals(file1, ((FileRespose)resp).getData());

        BinaryRawWriterEx out = createWriter();
        resp.encode(connCtx, out);
        byte[] result = out.out().arrayCopy();

        BinaryRawWriterEx expectedOut = getExpectedMessageHeader(reqId);
        expectedOut.writeByteArray(file1);

        byte[] expected = expectedOut.out().arrayCopy();
        assertArrayEquals(expected, result);
    }

    /** */
    @Test
    public void testReadDirectory() {
        long reqId = 2;
        BinaryRawWriterEx message = createMessage(reqId, ModelStorateThinClientProcessor.PROCESSOR_ID, ModelStorateThinClientProcessor.Method.READ_FILE);
        message.writeString("/a/b");
        ClientCustomQueryRequest req = new ClientCustomQueryRequest(toReader(message));
        ClientResponse resp = req.process(connCtx);

        assertEquals(ClientStatus.FAILED, resp.status()); // error
        assertFalse(resp.error().isEmpty());
    }

    /** */
    @Test
    public void testReadUnexistedFile() {
        long reqId = 3;
        BinaryRawWriterEx message = createMessage(reqId, ModelStorateThinClientProcessor.PROCESSOR_ID, ModelStorateThinClientProcessor.Method.READ_FILE);
        message.writeString("/a/b/unexisted_file");
        ClientCustomQueryRequest req = new ClientCustomQueryRequest(toReader(message));
        ClientResponse resp = req.process(connCtx);

        assertEquals(ClientStatus.FAILED, resp.status()); // error
        assertFalse(resp.error().isEmpty());
    }

    /** */
    @Test
    public void testMoveFile() {
        long reqId = 4;
        String from = "/a/need_to_be_moved";
        String to = "/a/moved_file";
        ms.putFile(from, file1);

        BinaryRawWriterEx message = createMessage(reqId, ModelStorateThinClientProcessor.PROCESSOR_ID, ModelStorateThinClientProcessor.Method.MOVE);

        message.writeString(from);
        message.writeString(to);

        ClientCustomQueryRequest req = new ClientCustomQueryRequest(toReader(message));
        ClientResponse resp = req.process(connCtx);
        assertEquals(ClientStatus.SUCCESS, resp.status());

        assertFalse(ms.exists(from));
        assertTrue(ms.exists(to));
    }

    /** */
    @Test
    public void testMoveDirectory() {
        long reqId = 5;
        String from = "/a/need_to_be_moved_dir";
        String to = "/a/moved_dir";
        ms.mkdirs(from);

        BinaryRawWriterEx message = createMessage(reqId, ModelStorateThinClientProcessor.PROCESSOR_ID, ModelStorateThinClientProcessor.Method.MOVE);

        message.writeString(from);
        message.writeString(to);

        ClientCustomQueryRequest req = new ClientCustomQueryRequest(toReader(message));
        ClientResponse resp = req.process(connCtx);
        assertEquals(ClientStatus.FAILED, resp.status()); // error
        assertFalse(resp.error().isEmpty());
    }

    /** */
    @Test
    public void testMoveUnexistedFile() {
        long reqId = 6;
        String from = "/a/b/unexisted_file";
        String to = "/a/b/move_to";
        ms.mkdirs(from);

        BinaryRawWriterEx message = createMessage(reqId, ModelStorateThinClientProcessor.PROCESSOR_ID, ModelStorateThinClientProcessor.Method.MOVE);

        message.writeString(from);
        message.writeString(to);

        ClientCustomQueryRequest req = new ClientCustomQueryRequest(toReader(message));
        ClientResponse resp = req.process(connCtx);
        assertEquals(ClientStatus.FAILED, resp.status()); // error
        assertFalse(resp.error().isEmpty());
    }

    /** */
    @Test
    public void testMoveWithExistedName() {
        long reqId = 7;
        String from = "/a/b/testMoveWithExistedName";
        ms.putFile(from, file1);
        String to = pathToFile1;

        BinaryRawWriterEx message = createMessage(reqId, ModelStorateThinClientProcessor.PROCESSOR_ID, ModelStorateThinClientProcessor.Method.MOVE);

        message.writeString(from);
        message.writeString(to);

        ClientCustomQueryRequest req = new ClientCustomQueryRequest(toReader(message));
        ClientResponse resp = req.process(connCtx);
        assertEquals(ClientStatus.FAILED, resp.status()); // error
        assertFalse(resp.error().isEmpty());
    }

    /** */
    @Test
    public void testGetFileStat1() {
        long reqId = 7;

        BinaryRawWriterEx message = createMessage(reqId, ModelStorateThinClientProcessor.PROCESSOR_ID, ModelStorateThinClientProcessor.Method.STAT);
        message.writeString(pathToFile1);
        ClientCustomQueryRequest req = new ClientCustomQueryRequest(toReader(message));
        ClientResponse resp = req.process(connCtx);
        FileStat stat = ((FileStatResponse)resp).getStat();
        assertFalse(stat.isDirectory());
        assertEquals(file1.length, stat.getSize());

        BinaryRawWriterEx out = createWriter();
        resp.encode(connCtx, out);
        byte[] result = out.out().arrayCopy();

        BinaryRawWriterEx expectedOut = getExpectedMessageHeader(reqId);
        expectedOut.writeBoolean(stat.isDirectory());
        expectedOut.writeInt(stat.getSize());
        expectedOut.writeLong(stat.getModificationTime());

        byte[] expected = expectedOut.out().arrayCopy();
        assertArrayEquals(expected, result);
    }

    /** */
    @Test
    public void testGetFileStat2() {
        long reqId = 8;

        BinaryRawWriterEx message = createMessage(reqId, ModelStorateThinClientProcessor.PROCESSOR_ID, ModelStorateThinClientProcessor.Method.STAT);
        message.writeString("/a/b");
        ClientCustomQueryRequest req = new ClientCustomQueryRequest(toReader(message));
        ClientResponse resp = req.process(connCtx);
        FileStat stat = ((FileStatResponse)resp).getStat();
        assertTrue(stat.isDirectory());
        assertEquals(0, stat.getSize());

        BinaryRawWriterEx out = createWriter();
        resp.encode(connCtx, out);
        byte[] result = out.out().arrayCopy();

        BinaryRawWriterEx expectedOut = getExpectedMessageHeader(reqId);
        expectedOut.writeBoolean(stat.isDirectory());
        expectedOut.writeInt(stat.getSize());
        expectedOut.writeLong(stat.getModificationTime());

        byte[] expected = expectedOut.out().arrayCopy();
        assertArrayEquals(expected, result);
    }

    /** */
    @Test
    public void testIsExists1() {
        long reqId = 9;

        BinaryRawWriterEx message = createMessage(reqId, ModelStorateThinClientProcessor.PROCESSOR_ID, ModelStorateThinClientProcessor.Method.EXISTS);
        message.writeString(pathToFile1);
        ClientCustomQueryRequest req = new ClientCustomQueryRequest(toReader(message));
        ClientResponse resp = req.process(connCtx);
        BinaryRawWriterEx out = createWriter();
        resp.encode(connCtx, out);
        byte[] result = out.out().arrayCopy();

        BinaryRawWriterEx expectedOut = getExpectedMessageHeader(reqId);
        expectedOut.writeBoolean(true);

        byte[] expected = expectedOut.out().arrayCopy();
        assertArrayEquals(expected, result);
    }

    /** */
    @Test
    public void testIsExists2() {
        long reqId = 10;

        BinaryRawWriterEx message = createMessage(reqId, ModelStorateThinClientProcessor.PROCESSOR_ID, ModelStorateThinClientProcessor.Method.EXISTS);
        message.writeString("/a/b");
        ClientCustomQueryRequest req = new ClientCustomQueryRequest(toReader(message));
        ClientResponse resp = req.process(connCtx);
        BinaryRawWriterEx out = createWriter();
        resp.encode(connCtx, out);
        byte[] result = out.out().arrayCopy();

        BinaryRawWriterEx expectedOut = getExpectedMessageHeader(reqId);
        expectedOut.writeBoolean(true);

        byte[] expected = expectedOut.out().arrayCopy();
        assertArrayEquals(expected, result);
    }

    /** */
    @Test
    public void testIsExists3() {
        long reqId = 11;

        BinaryRawWriterEx message = createMessage(reqId, ModelStorateThinClientProcessor.PROCESSOR_ID, ModelStorateThinClientProcessor.Method.EXISTS);
        message.writeString("/a/b/unexisted_file");
        ClientCustomQueryRequest req = new ClientCustomQueryRequest(toReader(message));
        ClientResponse resp = req.process(connCtx);
        BinaryRawWriterEx out = createWriter();
        resp.encode(connCtx, out);
        byte[] result = out.out().arrayCopy();

        BinaryRawWriterEx expectedOut = getExpectedMessageHeader(reqId);
        expectedOut.writeBoolean(false);

        byte[] expected = expectedOut.out().arrayCopy();
        assertArrayEquals(expected, result);
    }

    /** */
    @Test
    public void testRemoveFile() {
        long reqId = 12;

        String path1 = "/a/b/to_remove_1";
        ms.putFile(path1, file1);

        BinaryRawWriterEx message = createMessage(reqId, ModelStorateThinClientProcessor.PROCESSOR_ID, ModelStorateThinClientProcessor.Method.REMOVE);
        message.writeString(path1);
        ClientCustomQueryRequest req = new ClientCustomQueryRequest(toReader(message));
        ClientResponse resp = req.process(connCtx);
        BinaryRawWriterEx out = createWriter();
        resp.encode(connCtx, out);
        byte[] result = out.out().arrayCopy();

        assertFalse(ms.exists(path1));
        assertArrayEquals(getExpectedMessageHeader(reqId).out().arrayCopy(), result);
    }

    /** */
    @Test
    public void testRemoveNonEmptyDirectory() {
        long reqId = 13;

        String path1 = "/a/b/test_dir/to_remove_1";
        String path2 = "/a/b/test_dir/to_remove_1";
        ms.mkdirs("/a/b/test_dir");
        ms.putFile(path1, file1);
        ms.putFile(path2, file1);

        BinaryRawWriterEx message = createMessage(reqId, ModelStorateThinClientProcessor.PROCESSOR_ID, ModelStorateThinClientProcessor.Method.REMOVE);
        message.writeString("/a/b/test_dir");
        ClientCustomQueryRequest req = new ClientCustomQueryRequest(toReader(message));
        ClientResponse resp = req.process(connCtx);
        assertEquals(ClientStatus.FAILED, resp.status()); // error
        assertFalse(resp.error().isEmpty());
    }

    /** */
    @Test
    public void testRemoveEmptyDirectory() {
        long reqId = 13;

        ms.mkdirs("/a/b/test_dir");

        BinaryRawWriterEx message = createMessage(reqId, ModelStorateThinClientProcessor.PROCESSOR_ID, ModelStorateThinClientProcessor.Method.REMOVE);
        message.writeString("/a/b/test_dir");
        ClientCustomQueryRequest req = new ClientCustomQueryRequest(toReader(message));
        ClientResponse resp = req.process(connCtx);

        BinaryRawWriterEx out = createWriter();
        resp.encode(connCtx, out);
        byte[] result = out.out().arrayCopy();

        assertFalse(ms.exists("/a/b/test_dir"));
        assertTrue(ms.exists("/a/b"));
        assertArrayEquals(getExpectedMessageHeader(reqId).out().arrayCopy(), result);
    }

    /** */
    @Test
    public void testMkDir() {
        long reqId = 14;

        String path1 = "/a/b/test_mk_dir";
        BinaryRawWriterEx message = createMessage(reqId, ModelStorateThinClientProcessor.PROCESSOR_ID, ModelStorateThinClientProcessor.Method.MKDIR);
        message.writeString(path1);
        message.writeBoolean(false);
        ClientCustomQueryRequest req = new ClientCustomQueryRequest(toReader(message));
        ClientResponse resp = req.process(connCtx);
        BinaryRawWriterEx out = createWriter();
        resp.encode(connCtx, out);
        byte[] result = out.out().arrayCopy();

        assertTrue(ms.exists(path1));
        assertArrayEquals(getExpectedMessageHeader(reqId).out().arrayCopy(), result);
    }

    /** */
    @Test
    public void testRepeatedMkDir1() {
        long reqId = 15;
        String path = "/mk/dirs/test";
        ms.mkdirs(path);

        BinaryRawWriterEx message = createMessage(reqId, ModelStorateThinClientProcessor.PROCESSOR_ID, ModelStorateThinClientProcessor.Method.MKDIR);
        message.writeString(path);
        message.writeBoolean(true); // only if not exists

        ClientCustomQueryRequest req = new ClientCustomQueryRequest(toReader(message));
        ClientResponse resp = req.process(connCtx);
        assertEquals(ClientStatus.FAILED, resp.status()); // error
        assertFalse(resp.error().isEmpty());
    }

    /** */
    @Test
    public void testRepeatedMkDir2() {
        long reqId = 15;
        String path = "/mk/dirs/test";
        ms.mkdirs(path);

        BinaryRawWriterEx message = createMessage(reqId, ModelStorateThinClientProcessor.PROCESSOR_ID, ModelStorateThinClientProcessor.Method.MKDIR);
        message.writeString(path);
        message.writeBoolean(false); // only if not exists

        ClientCustomQueryRequest req = new ClientCustomQueryRequest(toReader(message));
        ClientResponse resp = req.process(connCtx);
        BinaryRawWriterEx out = createWriter();
        resp.encode(connCtx, out);
        byte[] result = out.out().arrayCopy();

        assertArrayEquals(getExpectedMessageHeader(reqId).out().arrayCopy(), result);
    }

    /** */
    @Test
    public void testListFiles1() {
        long reqId = 16;
        String path = "/list/files/test";
        ms.mkdirs(path);

        String file1path = path + "/a";
        String file2path = path + "/b";

        ms.putFile(file1path, file1);
        ms.putFile(file2path, file1);

        BinaryRawWriterEx message = createMessage(reqId, ModelStorateThinClientProcessor.PROCESSOR_ID, ModelStorateThinClientProcessor.Method.LIST_FILES);
        message.writeString(path);
        ClientCustomQueryRequest req = new ClientCustomQueryRequest(toReader(message));
        ClientResponse resp = req.process(connCtx);
        assertTrue(((FilesListResponse)resp).getFilesList().containsAll(Arrays.asList(file1path, file2path)));

        BinaryRawWriterEx out = createWriter();
        resp.encode(connCtx, out);
        byte[] result = out.out().arrayCopy();

        BinaryRawWriterEx expectedOut = getExpectedMessageHeader(reqId);
        expectedOut.writeInt(2);
        expectedOut.writeString(file1path);
        expectedOut.writeString(file2path);

        byte[] expected = expectedOut.out().arrayCopy();
        assertArrayEquals(expected, result);
    }

    /** */
    @Test
    public void testListFiles2() {
        long reqId = 17;
        BinaryRawWriterEx message = createMessage(reqId, ModelStorateThinClientProcessor.PROCESSOR_ID, ModelStorateThinClientProcessor.Method.LIST_FILES);
        message.writeString(pathToFile1);

        ClientCustomQueryRequest req = new ClientCustomQueryRequest(toReader(message));
        ClientResponse resp = req.process(connCtx);
        assertEquals(ClientStatus.FAILED, resp.status()); // error
        assertFalse(resp.error().isEmpty());
    }

    /** */
    @Test
    public void testListFiles3() {
        long reqId = 17;
        BinaryRawWriterEx message = createMessage(reqId, ModelStorateThinClientProcessor.PROCESSOR_ID, ModelStorateThinClientProcessor.Method.LIST_FILES);
        message.writeString("/test/list/files/3/unexisted_file");

        ClientCustomQueryRequest req = new ClientCustomQueryRequest(toReader(message));
        ClientResponse resp = req.process(connCtx);
        assertEquals(ClientStatus.FAILED, resp.status()); // error
        assertFalse(resp.error().isEmpty());
    }

    /** */
    @Test
    public void testPutFile1() {
        long reqId = 18;

        String path = "/a/b/new_file_1";
        boolean create = true;
        boolean append = false;
        byte[] data = new byte[] {6, 5, 4, 3, 2, 1, 0};

        BinaryRawWriterEx message = createMessage(reqId, ModelStorateThinClientProcessor.PROCESSOR_ID, ModelStorateThinClientProcessor.Method.WRITE_FILE);
        message.writeString(path);
        message.writeBoolean(create);
        message.writeBoolean(append);
        message.writeByteArray(data);

        ClientCustomQueryRequest req = new ClientCustomQueryRequest(toReader(message));
        ClientResponse resp = req.process(connCtx);
        BinaryRawWriterEx out = createWriter();
        resp.encode(connCtx, out);
        byte[] result = out.out().arrayCopy();

        assertTrue(ms.exists(path));
        assertArrayEquals(data, ms.getFile(path));
        assertArrayEquals(getExpectedMessageHeader(reqId).out().arrayCopy(), result);
    }

    /** */
    @Test
    public void testPutFile2() {
        long reqId = 20;

        String path = "/a/b/testPutFile2";
        boolean create = false;
        boolean append = false;
        byte[] data = new byte[] {6, 5, 4, 3, 2, 1, 0};

        BinaryRawWriterEx message = createMessage(reqId, ModelStorateThinClientProcessor.PROCESSOR_ID, ModelStorateThinClientProcessor.Method.WRITE_FILE);
        message.writeString(path);
        message.writeBoolean(create);
        message.writeBoolean(append);
        message.writeByteArray(data);

        ClientCustomQueryRequest req = new ClientCustomQueryRequest(toReader(message));
        ClientResponse resp = req.process(connCtx);
        assertEquals(ClientStatus.FAILED, resp.status()); // error
        assertFalse(resp.error().isEmpty());
    }

    /** */
    @Test
    public void testPutFile3() {
        long reqId = 21;

        String path = "/a/b/testPutFile3";
        boolean create = false;
        boolean append = true;
        byte[] data = new byte[] {6, 5, 4, 3, 2, 1, 0};

        BinaryRawWriterEx message = createMessage(reqId, ModelStorateThinClientProcessor.PROCESSOR_ID, ModelStorateThinClientProcessor.Method.WRITE_FILE);
        message.writeString(path);
        message.writeBoolean(create);
        message.writeBoolean(append);
        message.writeByteArray(data);

        ClientCustomQueryRequest req = new ClientCustomQueryRequest(toReader(message));
        ClientResponse resp = req.process(connCtx);
        assertEquals(ClientStatus.FAILED, resp.status()); // error
        assertFalse(resp.error().isEmpty());
    }

    /** */
    @Test
    public void testRewriteFile() {
        long reqId = 22;

        String path = "/a/b/testRewriteFile";
        boolean create = true;
        boolean append = false;
        byte[] data1 = new byte[] {6, 5, 4, 3, 2, 1, 0};
        byte[] data2 = new byte[] {0, 1, 2, 3, 4, 5, 6};
        ms.putFile(path, data1);

        BinaryRawWriterEx message = createMessage(reqId, ModelStorateThinClientProcessor.PROCESSOR_ID, ModelStorateThinClientProcessor.Method.WRITE_FILE);
        message.writeString(path);
        message.writeBoolean(create);
        message.writeBoolean(append);
        message.writeByteArray(data2);

        ClientCustomQueryRequest req = new ClientCustomQueryRequest(toReader(message));
        ClientResponse resp = req.process(connCtx);
        BinaryRawWriterEx out = createWriter();
        resp.encode(connCtx, out);
        byte[] result = out.out().arrayCopy();

        assertTrue(ms.exists(path));
        assertArrayEquals(data2, ms.getFile(path));
        assertArrayEquals(getExpectedMessageHeader(reqId).out().arrayCopy(), result);
    }

    /** */
    @Test
    public void testAppendFile() {
        long reqId = 23;

        String path = "/a/b/testAppendFile";
        boolean create = false;
        boolean append = true;
        byte[] data1 = new byte[] {6, 5, 4, 3, 2, 1, 0};
        byte[] data2 = new byte[] {0, 1, 2, 3, 4, 5, 6};
        ms.putFile(path, data1);

        BinaryRawWriterEx message = createMessage(reqId, ModelStorateThinClientProcessor.PROCESSOR_ID, ModelStorateThinClientProcessor.Method.WRITE_FILE);
        message.writeString(path);
        message.writeBoolean(create);
        message.writeBoolean(append);
        message.writeByteArray(data2);

        ClientCustomQueryRequest req = new ClientCustomQueryRequest(toReader(message));
        ClientResponse resp = req.process(connCtx);
        BinaryRawWriterEx out = createWriter();
        resp.encode(connCtx, out);
        byte[] result = out.out().arrayCopy();

        assertTrue(ms.exists(path));
        byte[] expArray = new byte[data1.length + data2.length];
        System.arraycopy(data1, 0, expArray, 0, data1.length);
        System.arraycopy(data2, 0, expArray, data1.length, data2.length);
        assertArrayEquals(expArray, ms.getFile(path));
        assertArrayEquals(getExpectedMessageHeader(reqId).out().arrayCopy(), result);
    }

    /** */
    @Test
    public void testAppendWithCreateTrueFile() {
        long reqId = 24;

        String path = "/a/b/testAppendWithCreateTrueFile";
        boolean create = true;
        boolean append = true;
        byte[] data1 = new byte[] {6, 5, 4, 3, 2, 1, 0};
        byte[] data2 = new byte[] {0, 1, 2, 3, 4, 5, 6};
        ms.putFile(path, data1);

        BinaryRawWriterEx message = createMessage(reqId, ModelStorateThinClientProcessor.PROCESSOR_ID, ModelStorateThinClientProcessor.Method.WRITE_FILE);
        message.writeString(path);
        message.writeBoolean(create);
        message.writeBoolean(append);
        message.writeByteArray(data2);

        ClientCustomQueryRequest req = new ClientCustomQueryRequest(toReader(message));
        ClientResponse resp = req.process(connCtx);
        BinaryRawWriterEx out = createWriter();
        resp.encode(connCtx, out);
        byte[] result = out.out().arrayCopy();

        assertTrue(ms.exists(path));
        assertArrayEquals(data2, ms.getFile(path));
        assertArrayEquals(getExpectedMessageHeader(reqId).out().arrayCopy(), result);
    }

    /** */
    private BinaryRawWriterEx getExpectedMessageHeader(long reqId) {
        BinaryRawWriterEx expectedOut = createWriter();
        expectedOut.writeLong(reqId);
        expectedOut.writeShort((short)0);
        return expectedOut;
    }

    private BinaryRawWriterEx createMessage(long reqId,
        String processorId,
        ModelStorateThinClientProcessor.Method method) {
        BinaryRawWriterEx writer = createWriter();

        writer.writeLong(reqId);
        writer.writeString(processorId);
        writer.writeByte(method.id());
        return writer;
    }

    /** */
    private BinaryRawWriterEx createWriter() {
        return new BinaryWriterExImpl(
            bctx, new BinaryHeapOutputStream(1024),
            new BinaryWriterSchemaHolder(),
            new BinaryWriterHandles()
        );
    }

    /** */
    private BinaryRawReader toReader(BinaryRawWriterEx writer) {
        BinaryOutputStream bout = writer.out();
        byte[] bytes = bout.arrayCopy();
        return toReader(bytes);
    }

    /** */
    private BinaryRawReader toReader(byte[] buf) {
        return new BinaryReaderExImpl(
            bctx,
            new BinaryHeapInputStream(buf),
            getClass().getClassLoader(),
            true
        );
    }
}

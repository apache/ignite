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

package org.apache.ignite.internal.util;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteInterruptedException;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.GridPeerDeployAware;
import org.apache.ignite.internal.util.lang.IgniteThrowableFunction;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoveryNode;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.http.GridEmbeddedHttpServer;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Test;

import static java.util.Arrays.asList;
import static java.util.Objects.nonNull;
import static java.util.stream.Collectors.joining;
import static org.apache.ignite.testframework.GridTestUtils.readResource;
import static org.junit.Assert.assertArrayEquals;

/**
 * Grid utils tests.
 */
@GridCommonTest(group = "Utils")
public class IgniteUtilsSelfTest extends GridCommonAbstractTest {
    /** */
    public static final int[] EMPTY = new int[0];

    /** Maximum string length to be written at once. */
    private static final int MAX_STR_LEN = 0xFFFF / 4;

    /**
     * @return 120 character length string.
     */
    private String text120() {
        char[] chs = new char[120];

        Arrays.fill(chs, 'x');

        return new String(chs);
    }

    /**
     *
     */
    @Test
    public void testIsPow2() {
        assertTrue(U.isPow2(1));
        assertTrue(U.isPow2(2));
        assertTrue(U.isPow2(4));
        assertTrue(U.isPow2(8));
        assertTrue(U.isPow2(16));
        assertTrue(U.isPow2(16 * 16));
        assertTrue(U.isPow2(32 * 32));

        assertFalse(U.isPow2(-4));
        assertFalse(U.isPow2(-3));
        assertFalse(U.isPow2(-2));
        assertFalse(U.isPow2(-1));
        assertFalse(U.isPow2(0));
        assertFalse(U.isPow2(3));
        assertFalse(U.isPow2(5));
        assertFalse(U.isPow2(6));
        assertFalse(U.isPow2(7));
        assertFalse(U.isPow2(9));
    }

    /**
     *
     */
    @Test
    public void testNextPowOf2() {
        assertEquals(1, U.nextPowerOf2(0));
        assertEquals(1, U.nextPowerOf2(1));
        assertEquals(2, U.nextPowerOf2(2));
        assertEquals(4, U.nextPowerOf2(3));
        assertEquals(4, U.nextPowerOf2(4));

        assertEquals(8, U.nextPowerOf2(5));
        assertEquals(8, U.nextPowerOf2(6));
        assertEquals(8, U.nextPowerOf2(7));
        assertEquals(8, U.nextPowerOf2(8));

        assertEquals(32768, U.nextPowerOf2(32767));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAllLocalIps() throws Exception {
        Collection<String> ips = U.allLocalIps();

        System.out.println("All local IPs: " + ips);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAllLocalMACs() throws Exception {
        Collection<String> macs = U.allLocalMACs();

        System.out.println("All local MACs: " + macs);
    }

    /**
     * On linux NetworkInterface.getHardwareAddress() returns null from time to time.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testAllLocalMACsMultiThreaded() throws Exception {
        GridTestUtils.runMultiThreaded(new Runnable() {
            @Override public void run() {
                for (int i = 0; i < 30; i++) {
                    Collection<String> macs = U.allLocalMACs();

                    assertTrue("Mac address are not defined.", !macs.isEmpty());
                }
            }
        }, 32, "thread");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testByteArray2String() throws Exception {
        assertEquals("{0x0A,0x14,0x1E,0x28,0x32,0x3C,0x46,0x50,0x5A}",
            U.byteArray2String(new byte[]{10, 20, 30, 40, 50, 60, 70, 80, 90}, "0x%02X", ",0x%02X"));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testFormatMins() throws Exception {
        printFormatMins(0);
        printFormatMins(1);
        printFormatMins(2);
        printFormatMins(59);
        printFormatMins(60);
        printFormatMins(61);
        printFormatMins(60 * 24 - 1);
        printFormatMins(60 * 24);
        printFormatMins(60 * 24 + 1);
        printFormatMins(5 * 60 * 24 - 1);
        printFormatMins(5 * 60 * 24);
        printFormatMins(5 * 60 * 24 + 1);
    }

    /**
     * Helper method for {@link #testFormatMins()}
     *
     * @param mins Minutes to test.
     */
    private void printFormatMins(long mins) {
        System.out.println("For " + mins + " minutes: " + X.formatMins(mins));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDownloadUrlFromHttp() throws Exception {
        GridEmbeddedHttpServer srv = null;
        try {
            String urlPath = "/testDownloadUrl/";
            srv = GridEmbeddedHttpServer.startHttpServer().withFileDownloadingHandler(urlPath,
                GridTestUtils.resolveIgnitePath("/modules/core/src/test/config/tests.properties"));

            File file = new File(System.getProperty("java.io.tmpdir") + File.separator + "url-http.file");

            file = U.downloadUrl(new URL(srv.getBaseUrl() + urlPath), file);

            assert file.exists();
            assert file.delete();
        }
        finally {
            if (srv != null)
                srv.stop(1);
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDownloadUrlFromHttps() throws Exception {
        GridEmbeddedHttpServer srv = null;
        try {
            String urlPath = "/testDownloadUrl/";
            srv = GridEmbeddedHttpServer.startHttpsServer().withFileDownloadingHandler(urlPath,
                GridTestUtils.resolveIgnitePath("modules/core/src/test/config/tests.properties"));

            File file = new File(System.getProperty("java.io.tmpdir") + File.separator + "url-http.file");

            file = U.downloadUrl(new URL(srv.getBaseUrl() + urlPath), file);

            assert file.exists();
            assert file.delete();
        }
        finally {
            if (srv != null)
                srv.stop(1);
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDownloadUrlFromLocalFile() throws Exception {
        File file = new File(System.getProperty("java.io.tmpdir") + File.separator + "url-http.file");

        file = U.downloadUrl(
            GridTestUtils.resolveIgnitePath("modules/core/src/test/config/tests.properties").toURI().toURL(), file);

        assert file.exists();
        assert file.delete();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testOs() throws Exception {
        System.out.println("OS string: " + U.osString());
        System.out.println("JDK string: " + U.jdkString());
        System.out.println("OS/JDK string: " + U.osJdkString());

        System.out.println("Is Windows: " + U.isWindows());
        System.out.println("Is Windows 95: " + U.isWindows95());
        System.out.println("Is Windows 98: " + U.isWindows98());
        System.out.println("Is Windows NT: " + U.isWindowsNt());
        System.out.println("Is Windows 2000: " + U.isWindows2k());
        System.out.println("Is Windows 2003: " + U.isWindows2003());
        System.out.println("Is Windows XP: " + U.isWindowsXp());
        System.out.println("Is Windows Vista: " + U.isWindowsVista());
        System.out.println("Is Linux: " + U.isLinux());
        System.out.println("Is Mac OS: " + U.isMacOs());
        System.out.println("Is Netware: " + U.isNetWare());
        System.out.println("Is Solaris: " + U.isSolaris());
        System.out.println("Is Solaris SPARC: " + U.isSolarisSparc());
        System.out.println("Is Solaris x86: " + U.isSolarisX86());
        System.out.println("Is Windows7: " + U.isWindows7());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testJavaSerialization() throws Exception {
        ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
        ObjectOutputStream objOut = new ObjectOutputStream(byteOut);

        objOut.writeObject(new byte[] {1, 2, 3, 4, 5, 5});

        objOut.flush();

        byte[] sBytes = byteOut.toByteArray();

        ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(sBytes));

        in.readObject();
    }

    /**
     *
     */
    @Test
    public void testHidePassword() {
        Collection<String> uriList = new ArrayList<>();

        uriList.add("ftp://anonymous:111111;freq=5000@unknown.host:21/pub/gg-test");
        uriList.add("ftp://anonymous:111111;freq=5000@localhost:21/pub/gg-test");

        uriList.add("http://freq=5000@localhost/tasks");
        uriList.add("http://freq=5000@unknownhost.host/tasks");

        for (String uri : uriList)
            X.println(uri + " -> " + U.hidePassword(uri));
    }

    /**
     * Test job to test possible indefinite recursion in detecting peer deploy aware.
     */
    private class SelfReferencedJob extends ComputeJobAdapter implements GridPeerDeployAware {
        /** */
        private SelfReferencedJob ref;

        /** */
        private SelfReferencedJob[] arr;

        /** */
        private Collection<SelfReferencedJob> col;

        /** */
        private ClusterNode node;

        /** */
        private ClusterGroup subGrid;

        /**
         * @param ignite Grid.
         */
        private SelfReferencedJob(Ignite ignite) throws IgniteCheckedException {
            node = ignite.cluster().localNode();

            ref = this;

            arr = new SelfReferencedJob[]{this, this};

            col = asList(this, this, this);

            newContext();

            subGrid = ignite.cluster().forNodes(Collections.singleton(node));
        }

        /** {@inheritDoc} */
        @Override public Object execute() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public Class<?> deployClass() {
            return getClass();
        }

        /** {@inheritDoc} */
        @Override public ClassLoader classLoader() {
            return getClass().getClassLoader();
        }
    }

    /**
     * @throws Exception If test fails.
     */
    @Test
    public void testDetectPeerDeployAwareInfiniteRecursion() throws Exception {
        Ignite g = startGrid(1);

        try {
            final SelfReferencedJob job = new SelfReferencedJob(g);

            GridPeerDeployAware d = U.detectPeerDeployAware(U.peerDeployAware(job));

            assert d != null;
            assert SelfReferencedJob.class == d.deployClass();
            assert d.classLoader() == SelfReferencedJob.class.getClassLoader();
        }
        finally {
            stopGrid(1);
        }
    }

    /**
     * @param r Runnable.
     * @return Job created for given runnable.
     */
    private static ComputeJob job(final Runnable r) {
        return new ComputeJobAdapter() {
            @Nullable @Override public Object execute() {
                r.run();

                return null;
            }
        };
    }

    /**
     * @throws Exception If test failed.
     */
    @Test
    public void testPeerDeployAware0() throws Exception {
        Collection<Object> col = new ArrayList<>();

        col.add(null);
        col.add(null);
        col.add(null);

        GridPeerDeployAware pda = U.peerDeployAware0(col);

        assert pda != null;

        col.clear();

        col.add(null);

        pda = U.peerDeployAware0(col);

        assert pda != null;

        col.clear();

        pda = U.peerDeployAware0(col);

        assert pda != null;

        col.clear();

        col.add(null);
        col.add("Test");
        col.add(null);

        pda = U.peerDeployAware0(col);

        assert pda != null;

        col.clear();

        col.add("Test");

        pda = U.peerDeployAware0(col);

        assert pda != null;

        col.clear();

        col.add("Test");
        col.add(this);

        pda = U.peerDeployAware0(col);

        assert pda != null;

        col.clear();

        col.add(null);
        col.add("Test");
        col.add(null);
        col.add(this);
        col.add(null);

        pda = U.peerDeployAware0(col);

        assert pda != null;
    }

    /**
     * Test UUID to bytes array conversion.
     */
    @Test
    public void testsGetBytes() {
        for (int i = 0; i < 100; i++) {
            UUID id = UUID.randomUUID();

            byte[] bytes = U.uuidToBytes(id);
            BigInteger n = new BigInteger(bytes);

            assert n.shiftRight(Long.SIZE).longValue() == id.getMostSignificantBits();
            assert n.longValue() == id.getLeastSignificantBits();
        }
    }

    /**
     *
     */
    @SuppressWarnings("ZeroLengthArrayAllocation")
    @Test
    public void testReadByteArray() {
        assertTrue(Arrays.equals(new byte[0], U.readByteArray(ByteBuffer.allocate(0))));
        assertTrue(Arrays.equals(new byte[0], U.readByteArray(ByteBuffer.allocate(0), ByteBuffer.allocate(0))));

        Random rnd = new Random();

        byte[] bytes = new byte[13];

        rnd.nextBytes(bytes);

        assertTrue(Arrays.equals(bytes, U.readByteArray(ByteBuffer.wrap(bytes))));
        assertTrue(Arrays.equals(bytes, U.readByteArray(ByteBuffer.wrap(bytes), ByteBuffer.allocate(0))));
        assertTrue(Arrays.equals(bytes, U.readByteArray(ByteBuffer.allocate(0), ByteBuffer.wrap(bytes))));

        for (int i = 0; i < 1000; i++) {
            int n = rnd.nextInt(100);

            bytes = new byte[n];

            rnd.nextBytes(bytes);

            ByteBuffer[] bufs = new ByteBuffer[1 + rnd.nextInt(10)];

            int x = 0;

            for (int j = 0; j < bufs.length - 1; j++) {
                int size = x == n ? 0 : rnd.nextInt(n - x);

                bufs[j] = (ByteBuffer)ByteBuffer.wrap(bytes).position(x).limit(x += size);
            }

            bufs[bufs.length - 1] = (ByteBuffer)ByteBuffer.wrap(bytes).position(x).limit(n);

            assertTrue(Arrays.equals(bytes, U.readByteArray(bufs)));
        }
    }

    /**
     *
     */
    @SuppressWarnings("ZeroLengthArrayAllocation")
    @Test
    public void testHashCodeFromBuffers() {
        assertEquals(Arrays.hashCode(new byte[0]), U.hashCode(ByteBuffer.allocate(0)));
        assertEquals(Arrays.hashCode(new byte[0]), U.hashCode(ByteBuffer.allocate(0), ByteBuffer.allocate(0)));

        Random rnd = new Random();

        for (int i = 0; i < 1000; i++) {
            ByteBuffer[] bufs = new ByteBuffer[1 + rnd.nextInt(15)];

            for (int j = 0; j < bufs.length; j++) {
                byte[] bytes = new byte[rnd.nextInt(25)];

                rnd.nextBytes(bytes);

                bufs[j] = ByteBuffer.wrap(bytes);
            }

            assertEquals(U.hashCode(bufs), Arrays.hashCode(U.readByteArray(bufs)));
        }
    }

    /**
     * Test annotation look up.
     */
    @Test
    public void testGetAnnotations() {
        assert U.getAnnotation(A1.class, Ann1.class) != null;
        assert U.getAnnotation(A2.class, Ann1.class) != null;

        assert U.getAnnotation(A1.class, Ann2.class) != null;
        assert U.getAnnotation(A2.class, Ann2.class) != null;

        assert U.getAnnotation(A3.class, Ann1.class) == null;
        assert U.getAnnotation(A3.class, Ann2.class) != null;
    }

    /**
     *
     */
    @Test
    public void testUnique() {
        int[][][] arrays = new int[][][]{
            new int[][]{EMPTY, EMPTY, EMPTY},
            new int[][]{new int[]{1, 2, 3}, EMPTY, new int[]{1, 2, 3}},
            new int[][]{new int[]{1, 2, 3}, new int[]{1, 2, 3}, new int[]{1, 2, 3}},
            new int[][]{new int[]{1, 2, 3}, new int[]{1, 3}, new int[]{1, 2, 3}},
            new int[][]{new int[]{1, 2, 30, 40, 50}, new int[]{2, 40}, new int[]{1, 2, 30, 40, 50}},
            new int[][]{new int[]{-100, -13, 1, 2, 5, 30, 40, 50}, new int[]{1, 2, 6, 100, 113},
                new int[]{-100, -13, 1, 2, 5, 6, 30, 40, 50, 100, 113}}
        };

        for (int[][] a : arrays) {
            assertArrayEquals(a[2], U.unique(a[0], a[0].length, a[1], a[1].length));

            assertArrayEquals(a[2], U.unique(a[1], a[1].length, a[0], a[0].length));
        }

        assertArrayEquals(new int[]{1, 2, 3, 4}, U.unique(new int[]{1, 2, 3, 8}, 3, new int[]{2, 4, 5}, 2));
        assertArrayEquals(new int[]{2, 4}, U.unique(new int[]{1, 2, 3, 8}, 0, new int[]{2, 4, 5}, 2));
        assertArrayEquals(new int[]{1, 2, 4, 5}, U.unique(new int[]{1, 2, 3, 8}, 2, new int[]{2, 4, 5, 6}, 3));
        assertArrayEquals(new int[]{1, 2}, U.unique(new int[]{1, 2, 3, 8}, 2, new int[]{2, 4, 5, 6}, 0));
    }

    /**
     *
     */
    @Test
    public void testDifference() {
        int[][][] arrays = new int[][][]{
            new int[][]{EMPTY, EMPTY, EMPTY},
            new int[][]{new int[]{1, 2, 3}, EMPTY, new int[]{1, 2, 3}},
            new int[][]{EMPTY, new int[]{1, 2, 3}, EMPTY},
            new int[][]{new int[]{1, 2, 3}, new int[]{1, 2, 3}, EMPTY},
            new int[][]{new int[]{-100, -50, 1, 2, 3}, new int[]{-50, -1, 1, 3}, new int[]{-100, 2}},
            new int[][]{new int[]{-100, 1, 2, 30, 40, 50}, new int[]{2, 40}, new int[]{-100, 1, 30, 50}},
            new int[][]{new int[]{-1, 1, 2, 30, 40, 50}, new int[]{1, 2, 100, 113}, new int[]{-1, 30, 40, 50}}
        };

        for (int[][] a : arrays)
            assertArrayEquals(a[2], U.difference(a[0], a[0].length, a[1], a[1].length));

        assertArrayEquals(new int[]{1, 2}, U.difference(new int[]{1, 2, 30, 40, 50}, 3, new int[]{30, 40}, 2));
        assertArrayEquals(EMPTY, U.difference(new int[]{1, 2, 30, 40, 50}, 0, new int[]{30, 40}, 2));
        assertArrayEquals(new int[]{1, 2, 40}, U.difference(new int[]{1, 2, 30, 40, 50}, 4, new int[]{30, 40}, 1));
        assertArrayEquals(new int[]{1, 2, 30, 40}, U.difference(new int[]{1, 2, 30, 40, 50}, 4, new int[]{30, 40}, 0));
    }

    /**
     *
     */
    @Test
    public void testCopyIfExceeded() {
        int[][] arrays = new int[][]{new int[]{13, 14, 17, 11}, new int[]{13}, EMPTY};

        for (int[] a : arrays) {
            int[] b = Arrays.copyOf(a, a.length);

            assertEquals(a, U.copyIfExceeded(a, a.length));
            assertArrayEquals(b, U.copyIfExceeded(a, a.length));

            for (int j = 0; j < a.length - 1; j++)
                assertArrayEquals(Arrays.copyOf(b, j), U.copyIfExceeded(a, j));
        }
    }

    /**
     *
     */
    @Test
    public void testIsIncreasingArray() {
        assertTrue(U.isIncreasingArray(EMPTY, 0));
        assertTrue(U.isIncreasingArray(new int[]{Integer.MIN_VALUE, -10, 1, 13, Integer.MAX_VALUE}, 5));
        assertTrue(U.isIncreasingArray(new int[]{1, 2, 3, -1, 5}, 0));
        assertTrue(U.isIncreasingArray(new int[]{1, 2, 3, -1, 5}, 3));
        assertFalse(U.isIncreasingArray(new int[]{1, 2, 3, -1, 5}, 4));
        assertFalse(U.isIncreasingArray(new int[]{1, 2, 3, -1, 5}, 5));
        assertFalse(U.isIncreasingArray(new int[]{1, 2, 3, 3, 5}, 4));
        assertTrue(U.isIncreasingArray(new int[]{1, -1}, 1));
        assertFalse(U.isIncreasingArray(new int[]{1, -1}, 2));
        assertTrue(U.isIncreasingArray(new int[]{13, 13, 13}, 1));
        assertFalse(U.isIncreasingArray(new int[]{13, 13, 13}, 2));
        assertFalse(U.isIncreasingArray(new int[]{13, 13, 13}, 3));
    }

    /**
     *
     */
    @Test
    public void testIsNonDecreasingArray() {
        assertTrue(U.isNonDecreasingArray(EMPTY, 0));
        assertTrue(U.isNonDecreasingArray(new int[]{Integer.MIN_VALUE, -10, 1, 13, Integer.MAX_VALUE}, 5));
        assertTrue(U.isNonDecreasingArray(new int[]{1, 2, 3, -1, 5}, 0));
        assertTrue(U.isNonDecreasingArray(new int[]{1, 2, 3, -1, 5}, 3));
        assertFalse(U.isNonDecreasingArray(new int[]{1, 2, 3, -1, 5}, 4));
        assertFalse(U.isNonDecreasingArray(new int[]{1, 2, 3, -1, 5}, 5));
        assertTrue(U.isNonDecreasingArray(new int[]{1, 2, 3, 3, 5}, 4));
        assertTrue(U.isNonDecreasingArray(new int[]{1, -1}, 1));
        assertFalse(U.isNonDecreasingArray(new int[]{1, -1}, 2));
        assertTrue(U.isNonDecreasingArray(new int[]{13, 13, 13}, 1));
        assertTrue(U.isNonDecreasingArray(new int[]{13, 13, 13}, 2));
        assertTrue(U.isNonDecreasingArray(new int[]{13, 13, 13}, 3));
    }

    /**
     * Test InetAddress Comparator.
     */
    @Test
    public void testInetAddressesComparator() {
        List<InetSocketAddress> ips = new ArrayList<InetSocketAddress>() {
            {
                add(new InetSocketAddress("127.0.0.1", 1));
                add(new InetSocketAddress("10.0.0.1", 1));
                add(new InetSocketAddress("172.16.0.1", 1));
                add(new InetSocketAddress("192.168.0.1", 1));
                add(new InetSocketAddress("100.0.0.1", 1));
                add(new InetSocketAddress("XXX", 1));
            }
        };

        Collections.sort(ips, U.inetAddressesComparator(true));

        assertTrue(ips.get(0).getAddress().isLoopbackAddress());
        assertTrue(ips.get(ips.size() - 1).isUnresolved());

        Collections.sort(ips, U.inetAddressesComparator(false));

        assertTrue(ips.get(ips.size() - 2).getAddress().isLoopbackAddress());
        assertTrue(ips.get(ips.size() - 1).isUnresolved());
    }

    @Test
    public void testMD5Calculation() throws Exception {
        String md5 = U.calculateMD5(new ByteArrayInputStream("Corrupted information.".getBytes()));

        assertEquals("d7dbe555be2eee7fa658299850169fa1", md5);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testResolveLocalAddresses() throws Exception {
        InetAddress inetAddress = InetAddress.getByName("0.0.0.0");

        IgniteBiTuple<Collection<String>, Collection<String>> addrs = U.resolveLocalAddresses(inetAddress);

        Collection<String> hostNames = addrs.get2();

        assertFalse(hostNames.contains(null));
        assertFalse(hostNames.contains(""));
        assertFalse(hostNames.contains("127.0.0.1"));

        assertFalse(F.exist(hostNames, new IgnitePredicate<String>() {
            @Override public boolean apply(String hostName) {
                return hostName.contains("localhost") || hostName.contains("0:0:0:0:0:0:0:1");
            }
        }));
    }

    /**
     *
     */
    @Test
    public void testToSocketAddressesNoDuplicates() {
        Collection<String> addrs = new ArrayList<>();

        addrs.add("127.0.0.1");
        addrs.add("localhost");

        Collection<String> hostNames = new ArrayList<>();
        int port = 1234;

        assertEquals(1, U.toSocketAddresses(addrs, hostNames, port).size());
    }

    /**
     * Composes a test String of given tlength.
     *
     * @param len The length.
     * @return The String.
     */
    private static String composeString(int len) {
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < len; i++)
            sb.append((char)i);

        String x = sb.toString();

        assertEquals(len, x.length());

        return x;
    }

    /**
     * Writes the given String to a DataOutput, reads from DataInput, then checks if they are the same.
     *
     * @param s0 The String to check serialization for.
     * @throws Exception On error.
     */
    private static void checkString(String s0) throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutput dout = new DataOutputStream(baos);

        writeUTF(dout, s0);

        DataInput din = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));

        String s1 = readUTF(din);

        assertEquals(s0, s1);
    }

    /**
     * Write UTF string which can be {@code null}.
     *
     * @param out Output stream.
     * @param val Value.
     * @throws IOException If failed.
     */
    public static void writeUTF(DataOutput out, @Nullable String val) throws IOException {
        if (val == null)
            out.writeInt(-1);
        else {
            out.writeInt(val.length());

            if (val.length() <= MAX_STR_LEN)
                out.writeUTF(val); // Optimized write in 1 chunk.
            else {
                int written = 0;

                while (written < val.length()) {
                    int partLen = Math.min(val.length() - written, MAX_STR_LEN);

                    String part = val.substring(written, written + partLen);

                    out.writeUTF(part);

                    written += partLen;
                }
            }
        }
    }

    /**
     * Read UTF string which can be {@code null}.
     *
     * @param in Input stream.
     * @return Value.
     * @throws IOException If failed.
     */
    public static String readUTF(DataInput in) throws IOException {
        int len = in.readInt(); // May be zero.

        if (len < 0)
            return null;
        else {
            if (len <= MAX_STR_LEN)
                return in.readUTF();

            StringBuilder sb = new StringBuilder(len);

            do {
                sb.append(in.readUTF());
            }
            while (sb.length() < len);

            assert sb.length() == len;

            return sb.toString();
        }
    }

    /**
     * Tests long String serialization/deserialization,
     *
     * @throws Exception If failed.
     */
    @Test
    public void testLongStringWriteUTF() throws Exception {
        checkString(null);
        checkString("");

        checkString("a");

        checkString("Quick brown fox jumps over the lazy dog.");

        String x = composeString(0xFFFF / 4 - 1);
        checkString(x);

        x = composeString(0xFFFF / 4);
        checkString(x);

        x = composeString(0xFFFF / 4 + 1);
        checkString(x);
    }

    /**
     *
     */
    @Test
    public void testCeilPow2() throws Exception {
        assertEquals(2, U.ceilPow2(2));
        assertEquals(4, U.ceilPow2(3));
        assertEquals(4, U.ceilPow2(4));
        assertEquals(8, U.ceilPow2(5));
        assertEquals(8, U.ceilPow2(6));
        assertEquals(8, U.ceilPow2(7));
        assertEquals(8, U.ceilPow2(8));
        assertEquals(16, U.ceilPow2(9));
        assertEquals(1 << 15, U.ceilPow2((1 << 15) - 1));
        assertEquals(1 << 15, U.ceilPow2(1 << 15));
        assertEquals(1 << 16, U.ceilPow2((1 << 15) + 1));
        assertEquals(1 << 26, U.ceilPow2((1 << 26) - 100));
        assertEquals(1 << 26, U.ceilPow2(1 << 26));
        assertEquals(1 << 27, U.ceilPow2((1 << 26) + 100));

        for (int i = (int)Math.pow(2, 30); i < Integer.MAX_VALUE; i++)
            assertEquals((int)Math.pow(2, 30), U.ceilPow2(i));

        for (int i = Integer.MIN_VALUE; i < 0; i++)
            assertEquals(0, U.ceilPow2(i));
    }

    /**
     *
     */
    @Test
    public void testIsOldestNodeVersionAtLeast() {
        IgniteProductVersion v240 = IgniteProductVersion.fromString("2.4.0");
        IgniteProductVersion v241 = IgniteProductVersion.fromString("2.4.1");
        IgniteProductVersion v250 = IgniteProductVersion.fromString("2.5.0");
        IgniteProductVersion v250ts = IgniteProductVersion.fromString("2.5.0-b1-3");

        TcpDiscoveryNode node240 = new TcpDiscoveryNode();
        node240.version(v240);

        TcpDiscoveryNode node241 = new TcpDiscoveryNode();
        node241.version(v241);

        TcpDiscoveryNode node250 = new TcpDiscoveryNode();
        node250.version(v250);

        TcpDiscoveryNode node250ts = new TcpDiscoveryNode();
        node250ts.version(v250ts);

        assertTrue(U.isOldestNodeVersionAtLeast(v240, asList(node240, node241, node250, node250ts)));
        assertFalse(U.isOldestNodeVersionAtLeast(v241, asList(node240, node241, node250, node250ts)));
        assertTrue(U.isOldestNodeVersionAtLeast(v250, asList(node250, node250ts)));
        assertTrue(U.isOldestNodeVersionAtLeast(v250ts, asList(node250, node250ts)));
    }

    /**
     *
     */
    @Test
    public void testDoInParallel() throws Throwable {
        CyclicBarrier barrier = new CyclicBarrier(3);

        ExecutorService executorService = Executors.newFixedThreadPool(3);

        try {
            IgniteUtils.doInParallel(3,
                executorService,
                asList(1, 2, 3),
                i -> {
                    try {
                        barrier.await(1, TimeUnit.SECONDS);
                    }
                    catch (Exception e) {
                        throw new IgniteCheckedException(e);
                    }

                    return null;
                }
            );
        } finally {
            executorService.shutdownNow();
        }
    }

    /**
     *
     */
    @Test
    public void testDoInParallelBatch() {
        CyclicBarrier barrier = new CyclicBarrier(3);

        ExecutorService executorService = Executors.newFixedThreadPool(3);

        try {
            IgniteUtils.doInParallel(2,
                executorService,
                asList(1, 2, 3),
                i -> {
                    try {
                        barrier.await(400, TimeUnit.MILLISECONDS);
                    }
                    catch (Exception e) {
                        throw new IgniteCheckedException(e);
                    }

                    return null;
                }
            );

            fail("Should throw timeout exception");
        }
        catch (Exception e) {
            assertTrue(e.toString(), X.hasCause(e, TimeoutException.class));
        } finally {
            executorService.shutdownNow();
        }
    }

    /**
     * Test optimal splitting on batch sizes.
     */
    @Test
    public void testOptimalBatchSize() {
        assertArrayEquals(new int[]{1}, IgniteUtils.calculateOptimalBatchSizes(1, 1));

        assertArrayEquals(new int[]{2}, IgniteUtils.calculateOptimalBatchSizes(1, 2));

        assertArrayEquals(new int[]{1, 1, 1, 1}, IgniteUtils.calculateOptimalBatchSizes(6, 4));

        assertArrayEquals(new int[]{1}, IgniteUtils.calculateOptimalBatchSizes(4, 1));

        assertArrayEquals(new int[]{1, 1}, IgniteUtils.calculateOptimalBatchSizes(4, 2));

        assertArrayEquals(new int[]{1, 1, 1}, IgniteUtils.calculateOptimalBatchSizes(4, 3));

        assertArrayEquals(new int[]{1, 1, 1, 1}, IgniteUtils.calculateOptimalBatchSizes(4, 4));

        assertArrayEquals(new int[]{2, 1, 1, 1}, IgniteUtils.calculateOptimalBatchSizes(4, 5));

        assertArrayEquals(new int[]{2, 2, 1, 1}, IgniteUtils.calculateOptimalBatchSizes(4, 6));

        assertArrayEquals(new int[]{2, 2, 2, 1}, IgniteUtils.calculateOptimalBatchSizes(4, 7));

        assertArrayEquals(new int[]{2, 2, 2, 2}, IgniteUtils.calculateOptimalBatchSizes(4, 8));

        assertArrayEquals(new int[]{3, 2, 2, 2}, IgniteUtils.calculateOptimalBatchSizes(4, 9));

        assertArrayEquals(new int[]{3, 3, 2, 2}, IgniteUtils.calculateOptimalBatchSizes(4, 10));
    }

    /**
     * Test parallel execution in order.
     */
    @Test
    public void testDoInParallelResultsOrder() throws IgniteCheckedException {
        ExecutorService executorService = Executors.newFixedThreadPool(4);

        try {
            for (int parallelism = 1; parallelism < 16; parallelism++)
                for (int size = 0; size < 10_000; size++)
                    testOrder(executorService, size, parallelism);
        } finally {
            executorService.shutdownNow();
        }
    }

    /**
     * Test parallel execution steal job.
     */
    @Test
    public void testDoInParallelWithStealingJob() throws IgniteCheckedException {
        // Pool size should be less that input data collection.
        ExecutorService executorService = Executors.newFixedThreadPool(1);

        CountDownLatch mainThreadLatch = new CountDownLatch(1);
        CountDownLatch poolThreadLatch = new CountDownLatch(1);

        // Busy one thread from the pool.
        executorService.submit(new Runnable() {
            @Override public void run() {
                try {
                    poolThreadLatch.await();
                }
                catch (InterruptedException e) {
                    throw new IgniteInterruptedException(e);
                }
            }
        });

        List<Integer> data = asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        AtomicInteger taskProcessed = new AtomicInteger();

        long threadId = Thread.currentThread().getId();

        AtomicInteger curThreadCnt = new AtomicInteger();
        AtomicInteger poolThreadCnt = new AtomicInteger();

        Collection<Integer> res = U.doInParallel(10,
            executorService,
            data,
            new IgniteThrowableFunction<Integer, Integer>() {
                @Override public Integer apply(Integer cnt) throws IgniteInterruptedCheckedException {
                    // Release thread in pool in the middle of range.
                    if (taskProcessed.getAndIncrement() == (data.size() / 2) - 1) {
                        poolThreadLatch.countDown();

                        try {
                            // Await thread in thread pool complete task.
                            mainThreadLatch.await();
                        }
                        catch (InterruptedException e) {
                            Thread.currentThread().interrupt();

                            throw new IgniteInterruptedCheckedException(e);
                        }
                    }

                    // Increment if executed in current thread.
                    if (Thread.currentThread().getId() == threadId)
                        curThreadCnt.incrementAndGet();
                    else {
                        poolThreadCnt.incrementAndGet();

                        if (taskProcessed.get() == data.size())
                            mainThreadLatch.countDown();
                    }

                    return -cnt;
                }
            });

        Assert.assertEquals(curThreadCnt.get() + poolThreadCnt.get(), data.size());
        Assert.assertEquals(5, curThreadCnt.get());
        Assert.assertEquals(5, poolThreadCnt.get());
        Assert.assertEquals(asList(0, -1, -2, -3, -4, -5, -6, -7, -8, -9), res);
    }

    /**
     * Test parallel execution steal job.
     */
    @Test
    public void testDoInParallelWithStealingJobRunTaskInExecutor() throws Exception {
        // Pool size should be less that input data collection.
        ExecutorService executorService = Executors.newFixedThreadPool(2);

        Future<?> f1 = executorService.submit(() -> runTask(executorService));
        Future<?> f2 = executorService.submit(() -> runTask(executorService));
        Future<?> f3 = executorService.submit(() -> runTask(executorService));

        f1.get();
        f2.get();
        f3.get();
    }

    /**
     *
     * @param executorService Executor service.
     */
    private void runTask(ExecutorService executorService) {
        List<Integer> data = asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        long threadId = Thread.currentThread().getId();

        AtomicInteger curThreadCnt = new AtomicInteger();

        Collection<Integer> res;

        // Future for avoiding fast execution in only executor threads.
        // Here we try to pass a number of tasks more that executor size,
        // but there is a case when all task will be completed after last submit return control and
        // current thread can not steal task because all task will be already finished.
        GridFutureAdapter fut = new GridFutureAdapter();

        try {
            res = U.doInParallel(10,
                executorService,
                data,
                new IgniteThrowableFunction<Integer, Integer>() {
                    @Override public Integer apply(Integer cnt) {
                        if (Thread.currentThread().getId() == threadId) {
                            fut.onDone();

                            curThreadCnt.incrementAndGet();
                        }
                        else {
                            try {
                                fut.get();
                            }
                            catch (IgniteCheckedException e) {
                                throw U.convertException(e);
                            }
                        }

                        return -cnt;
                    }
                });
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }

        Assert.assertTrue(curThreadCnt.get() > 0);
        Assert.assertEquals(asList(0, -1, -2, -3, -4, -5, -6, -7, -8, -9), res);
    }

    /**
     * Template method to test parallel execution
     * @param executorService ExecutorService.
     * @param size Size.
     * @param parallelism Parallelism.
     * @throws IgniteCheckedException Exception.
     */
    private void testOrder(ExecutorService executorService, int size, int parallelism) throws IgniteCheckedException {
        List<Integer> list = new ArrayList<>();
        for (int i = 0; i < size; i++)
            list.add(i);

        Collection<Integer> results = IgniteUtils.doInParallel(
            parallelism,
            executorService,
            list,
            i -> i * 2
        );

        assertEquals(list.size(), results.size());

        final int[] i = {0};
        results.forEach(new Consumer<Integer>() {
            @Override public void accept(Integer integer) {
                assertEquals(2 * list.get(i[0]), integer.intValue());
                i[0]++;
            }
        });
    }

    /**
     *
     */
    @Test
    public void testDoInParallelException() {
        String expectedException = "ExpectedException";

        ExecutorService executorService = Executors.newFixedThreadPool(1);

        try {
            IgniteUtils.doInParallel(
                1,
                executorService,
                asList(1, 2, 3),
                i -> {
                    if (Integer.valueOf(1).equals(i))
                        throw new IgniteCheckedException(expectedException);

                    return null;
                }
            );

            fail("Should throw ParallelExecutionException");
        }
        catch (IgniteCheckedException e) {
            assertEquals(expectedException, e.getMessage());
        } finally {
            executorService.shutdownNow();
        }
    }

    /**
     * Testing methods {@link IgniteUtils#writeLongString} and
     * {@link IgniteUtils#readLongString} using resource files, where each line is
     * needed to test different cases:
     * 1){@code null}. <br/>
     *
     * 2)Empty line. <br/>
     *
     * 3)Simple strings. <br/>
     *
     * 4)Various combinations of strings with one, two, and three-byte
     * characters with size greater than {@link IgniteUtils#UTF_BYTE_LIMIT}. <br/>
     *
     * @throws Exception If failed.
     */
    @Test
    public void testReadWriteBigUTF() throws Exception {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            readLines("org.apache.ignite.util/bigUtf.txt", readLine -> {
                baos.reset();

                DataOutput dOut = new DataOutputStream(baos);
                U.writeLongString(dOut, readLine);

                DataInputStream dIn = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
                String readBigUTF = U.readLongString(dIn);

                assertEquals(readLine, readBigUTF);
            });
        }
    }

    /**
     * Testing method {@link IgniteUtils#writeCutString} using resource files,
     * where each line is needed to test different cases: <br/>
     * 1){@code null}. <br/>
     *
     * 2)Empty line. <br/>
     *
     * 3)Simple strings. <br/>
     *
     * 4)String containing single-byte characters of size
     * {@link IgniteUtils#UTF_BYTE_LIMIT}. <br/>
     *
     * 5)String containing single-byte characters of size more than
     * {@link IgniteUtils#UTF_BYTE_LIMIT}. <br/>
     *
     * 6)String containing two-byte characters of size
     * {@link IgniteUtils#UTF_BYTE_LIMIT}. <br/>
     *
     * 7)String containing two-byte characters of size more than
     * {@link IgniteUtils#UTF_BYTE_LIMIT}. <br/>
     *
     * 8)String containing three-byte characters of size
     * {@link IgniteUtils#UTF_BYTE_LIMIT}. <br/>
     *
     * 9)String containing three-byte characters of size more than
     * {@link IgniteUtils#UTF_BYTE_LIMIT}. <br/>
     *
     * @throws Exception If failed.
     */
    @Test
    public void testWriteLimitUTF() throws Exception {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            readLines("org.apache.ignite.util/limitUtf.txt", readLine -> {
                baos.reset();

                DataOutput dOut = new DataOutputStream(baos);
                U.writeCutString(dOut, readLine);

                DataInputStream dIn = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
                String readUTF = U.readString(dIn);

                if (nonNull(readLine)) {
                    AtomicInteger utfBytes = new AtomicInteger();

                    readLine = readLine.chars()
                        .filter(c -> utfBytes.addAndGet(U.utfBytes((char)c)) <= U.UTF_BYTE_LIMIT)
                        .mapToObj(c -> String.valueOf((char)c)).collect(joining());
                }

                assertEquals(readLine, readUTF);
            });
        }
    }

    /**
     * Reading lines from a resource file and passing them to consumer.
     * If read string is {@code "null"}, it is converted to {@code null}.
     *
     * @param rsrcName Resource name.
     * @param consumer Consumer.
     * @throws Exception If failed.
     */
    private void readLines(String rsrcName, ThrowableConsumer<String> consumer) throws Exception {
        byte[] content = readResource(getClass().getClassLoader(), rsrcName);

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(content)))) {
            String readLine;

            while (nonNull(readLine = reader.readLine())) {
                if ("null".equals(readLine))
                    readLine = null;

                consumer.accept(readLine);
            }
        }
    }

    /**
     * Test enum.
     */
    private enum TestEnum {
        E1,
        E2,
        E3
    }

    @Documented @Retention(RetentionPolicy.RUNTIME) @Target(ElementType.TYPE)
    private @interface Ann1 {}

    @Documented @Retention(RetentionPolicy.RUNTIME) @Target(ElementType.TYPE)
    private @interface Ann2 {}

    private static class A1 implements I3, I5 {}

    private static class A2 extends A1 {}

    private static class A3 implements I5 {}

    @Ann1 private interface I1 {}

    private interface I2 extends I1 {}

    private interface I3 extends I2 {}

    @Ann2 private interface I4 {}

    private interface I5 extends I4 {}

    /**
     * Represents an operation that accepts a single input argument and returns
     * no result. Unlike most other functional interfaces,
     * {@code ThrowableConsumer} is expected to operate via side-effects.
     *
     * Also it is able to throw {@link Exception} unlike {@link Consumer}.
     *
     * @param <T> The type of the input to the operation.
     */
    @FunctionalInterface
    private static interface ThrowableConsumer<T> {
        /**
         * Performs this operation on the given argument.
         *
         * @param t the input argument.
         */
        void accept(@Nullable T t) throws Exception;
    }
}

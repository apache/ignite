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
import java.io.FileOutputStream;
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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
import java.util.stream.IntStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteInterruptedException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.ConsumerX;
import org.apache.ignite.internal.util.lang.GridPeerDeployAware;
import org.apache.ignite.internal.util.lang.IgniteThrowableFunction;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoveryNode;
import org.apache.ignite.testframework.GridTestClassLoader;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;
import org.apache.ignite.thread.IgniteThreadFactory;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Test;

import static java.util.Arrays.asList;
import static java.util.Objects.nonNull;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.joining;
import static org.apache.ignite.internal.util.IgniteUtils.bytesToInt;
import static org.apache.ignite.internal.util.IgniteUtils.bytesToLong;
import static org.apache.ignite.internal.util.IgniteUtils.bytesToShort;
import static org.apache.ignite.internal.util.IgniteUtils.intToBytes;
import static org.apache.ignite.internal.util.IgniteUtils.longToBytes;
import static org.apache.ignite.internal.util.IgniteUtils.shortToBytes;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;
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
        System.out.println("For " + mins + " minutes: " + U.formatMins(mins));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testOs() throws Exception {
        System.out.println("OS string: " + U.osString());
        System.out.println("JDK string: " + U.jdkString());

        System.out.println("Is Windows: " + U.isWindows());
        System.out.println("Is Linux: " + U.isLinux());
        System.out.println("Is Mac OS: " + U.isMacOs());
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

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testResolveLocalAddresses() throws Exception {
        InetAddress inetAddr = InetAddress.getByName("0.0.0.0");

        IgniteBiTuple<Collection<String>, Collection<String>> addrs = U.resolveLocalAddresses(inetAddr);

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

        ExecutorService executorSrvc = Executors.newFixedThreadPool(3,
            new IgniteThreadFactory("testscope", "ignite-utils-test"));

        try {
            IgniteUtils.doInParallel(3,
                executorSrvc,
                asList(1, 2, 3),
                i -> {
                    try {
                        barrier.await(1, SECONDS);
                    }
                    catch (Exception e) {
                        throw new IgniteCheckedException(e);
                    }

                    return null;
                }
            );
        }
        finally {
            executorSrvc.shutdownNow();
        }
    }

    /**
     *
     */
    @Test
    public void testDoInParallelBatch() {
        CyclicBarrier barrier = new CyclicBarrier(3);

        ExecutorService executorSrvc = Executors.newFixedThreadPool(3,
            new IgniteThreadFactory("testscope", "ignite-utils-test"));

        try {
            IgniteUtils.doInParallel(2,
                executorSrvc,
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
        }
        finally {
            executorSrvc.shutdownNow();
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
        ExecutorService executorSrvc = Executors.newFixedThreadPool(4,
            new IgniteThreadFactory("testscope", "ignite-utils-test"));

        try {
            for (int parallelism = 1; parallelism < 16; parallelism++)
                for (int size = 0; size < 10_000; size++)
                    testOrder(executorSrvc, size, parallelism);
        }
        finally {
            executorSrvc.shutdownNow();
        }
    }

    /**
     * Test parallel execution steal job.
     */
    @Test
    public void testDoInParallelWithStealingJob() throws IgniteCheckedException {
        // Pool size should be less that input data collection.
        ExecutorService executorSrvc = Executors
            .newSingleThreadExecutor(new IgniteThreadFactory("testscope", "ignite-utils-test"));

        CountDownLatch mainThreadLatch = new CountDownLatch(1);
        CountDownLatch poolThreadLatch = new CountDownLatch(1);

        // Busy one thread from the pool.
        executorSrvc.submit(new Runnable() {
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
            executorSrvc,
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
        ExecutorService executorSrvc = Executors.newFixedThreadPool(2,
            new IgniteThreadFactory("testscope", "ignite-utils-test"));

        Future<?> f1 = executorSrvc.submit(() -> runTask(executorSrvc));
        Future<?> f2 = executorSrvc.submit(() -> runTask(executorSrvc));
        Future<?> f3 = executorSrvc.submit(() -> runTask(executorSrvc));

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
        String expectedEx = "ExpectedException";

        ExecutorService executorSrvc = Executors
            .newSingleThreadExecutor(new IgniteThreadFactory("testscope", "ignite-utils-test"));

        try {
            IgniteUtils.doInParallel(
                1,
                executorSrvc,
                asList(1, 2, 3),
                i -> {
                    if (Integer.valueOf(1).equals(i))
                        throw new IgniteCheckedException(expectedEx);

                    return null;
                }
            );

            fail("Should throw ParallelExecutionException");
        }
        catch (IgniteCheckedException e) {
            assertEquals(expectedEx, e.getMessage());
        }
        finally {
            executorSrvc.shutdownNow();
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
     * characters with size greater than <code>65535</code>. <br/>
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
     * Test of {@link U#humanReadableDuration}.
     */
    @Test
    public void testHumanReadableDuration() {
        assertEquals("0ms", U.humanReadableDuration(0));
        assertEquals("10ms", U.humanReadableDuration(10));

        assertEquals("1s", U.humanReadableDuration(SECONDS.toMillis(1)));
        assertEquals("1s", U.humanReadableDuration(SECONDS.toMillis(1) + 10));
        assertEquals("12s", U.humanReadableDuration(SECONDS.toMillis(12)));

        assertEquals("1m", U.humanReadableDuration(MINUTES.toMillis(1)));
        assertEquals("2m", U.humanReadableDuration(MINUTES.toMillis(2)));
        assertEquals("1m5s", U.humanReadableDuration(SECONDS.toMillis(65)));
        assertEquals("1m5s", U.humanReadableDuration(SECONDS.toMillis(65) + 10));

        assertEquals("1h", U.humanReadableDuration(HOURS.toMillis(1)));
        assertEquals("3h", U.humanReadableDuration(HOURS.toMillis(3)));
        assertEquals(
            "1h5m12s",
            U.humanReadableDuration(MINUTES.toMillis(65) + SECONDS.toMillis(12) + 10)
        );

        assertEquals("1d", U.humanReadableDuration(DAYS.toMillis(1)));
        assertEquals("15d", U.humanReadableDuration(DAYS.toMillis(15)));
        assertEquals("1d4h", U.humanReadableDuration(HOURS.toMillis(28)));
        assertEquals(
            "4d6h15m",
            U.humanReadableDuration(DAYS.toMillis(4) + HOURS.toMillis(6) + MINUTES.toMillis(15))
        );
    }

    /**
     * Test of {@link U#humanReadableByteCount}.
     */
    @Test
    public void testHumanReadableByteCount() {
        assertEquals("0.0 B", U.humanReadableByteCount(0));
        assertEquals("10.0 B", U.humanReadableByteCount(10));

        assertEquals("1.0 KB", U.humanReadableByteCount(1024));
        assertEquals("15.0 KB", U.humanReadableByteCount(15 * 1024));
        assertEquals("15.0 KB", U.humanReadableByteCount(15 * 1024 + 10));

        assertEquals("1.0 MB", U.humanReadableByteCount(1024 * 1024));
        assertEquals("6.0 MB", U.humanReadableByteCount(6 * 1024 * 1024));
        assertEquals("6.1 MB", U.humanReadableByteCount(6 * 1024 * 1024 + 130 * 1024));
    }

    /**
     * Test to verify the {@link U#uncompressedSize}.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testUncompressedSize() throws Exception {
        File zipFile = new File(System.getProperty("java.io.tmpdir"), "test.zip");

        try {
            assertThrows(log, () -> U.uncompressedSize(zipFile), IOException.class, null);

            byte[] raw = IntStream.range(0, 10).mapToObj(i -> zipFile.getAbsolutePath() + i)
                .collect(joining()).getBytes(StandardCharsets.UTF_8);

            try (FileOutputStream fos = new FileOutputStream(zipFile)) {
                fos.write(U.zip(raw));

                fos.flush();
            }

            assertEquals(raw.length, U.uncompressedSize(zipFile));
        }
        finally {
            assertTrue(U.delete(zipFile));
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
    private void readLines(String rsrcName, ConsumerX<String> consumer) throws Exception {
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
     * Tests that local hostname is ignored if {@link IgniteSystemProperties#IGNITE_IGNORE_LOCAL_HOST_NAME} is
     * set to {@code true}.
     *
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IgniteSystemProperties.IGNITE_LOCAL_HOST, value = "example.com")
    @WithSystemProperty(key = IgniteSystemProperties.IGNITE_IGNORE_LOCAL_HOST_NAME, value = "true")
    public void testResolveLocalAddressesWithHostNameDefined() throws Exception {
        testAddressResolveWithLocalHostDefined();
    }

    /**
     * Tests that local hostname is not ignored if {@link IgniteSystemProperties#IGNITE_IGNORE_LOCAL_HOST_NAME} is
     * set to {@code false}.
     *
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IgniteSystemProperties.IGNITE_LOCAL_HOST, value = "example.com")
    @WithSystemProperty(key = IgniteSystemProperties.IGNITE_IGNORE_LOCAL_HOST_NAME, value = "false")
    public void testResolveLocalAddressesWithHostNameDefinedAndLocalHostNameNotIgnored() throws Exception {
        testAddressResolveWithLocalHostDefined();
    }

    /**
     * Tests {@link IgniteUtils#isLambda(Class)} on lambdas.
     */
    @Test
    public void testIsLambdaOnLambdas() {
        Runnable someLambda = () -> {};

        int locVar = 0;
        Runnable capturingLocLambda = () -> {
            System.out.println(locVar);
        };

        Runnable capturingOuterClsLambda = () -> {
            System.out.println(repeatRule);
        };

        Runnable methodRef = this::testIsLambdaOnLambdas;

        assertTrue(IgniteUtils.isLambda(someLambda.getClass()));
        assertTrue(IgniteUtils.isLambda(capturingLocLambda.getClass()));
        assertTrue(IgniteUtils.isLambda(capturingOuterClsLambda.getClass()));
        assertTrue(IgniteUtils.isLambda(methodRef.getClass()));
    }

    /** Test nested class. */
    private static class TestNestedClass {
    }

    /** Test inner class. */
    private class TestInnerClass {
    }

    /**
     * Tests {@link IgniteUtils#isLambda(Class)} on non-lambda classes.
     */
    @Test
    public void testIsLambdaOnOrdinaryClasses() throws Exception {
        assertFalse(IgniteUtils.isLambda(Object.class));

        Runnable anonCls = new Runnable() {
            /** {@inheritDoc} */
            @Override public void run() {
                // No-op.
            }
        };

        assertFalse(IgniteUtils.isLambda(anonCls.getClass()));
        assertFalse(IgniteUtils.isLambda(TestEnum.class));

        // Loading only inner class with test classloader, while outer class
        // will be loaded with the default classloader. Thus, if we execute method like isAnonymousClass
        // on the loaded class, it will fail with the IncompatibleClassChangeError. That's why order in
        // IgniteUtils isLambda is important.
        GridTestClassLoader clsLdr = new GridTestClassLoader(
            TestNestedClass.class.getName(),
            TestInnerClass.class.getName()
        );

        Class<?> nestedCls = clsLdr.loadClass(TestNestedClass.class.getName());
        assertFalse(IgniteUtils.isLambda(nestedCls));

        Class<?> innerCls = clsLdr.loadClass(TestInnerClass.class.getName());
        assertFalse(IgniteUtils.isLambda(innerCls));
    }

    /**
     * Tests {@link IgniteUtils#resolveLocalAddresses(InetAddress)} with different values set to
     * {@link IgniteSystemProperties#IGNITE_LOCAL_HOST} and {@link IgniteSystemProperties#IGNITE_IGNORE_LOCAL_HOST_NAME}.
     *
     * @throws Exception If failed.
     */
    private void testAddressResolveWithLocalHostDefined() throws Exception {
        try {
            boolean ignoreLocHostname = IgniteSystemProperties.getBoolean(IgniteSystemProperties.IGNITE_IGNORE_LOCAL_HOST_NAME);
            String userDefinedHost = IgniteSystemProperties.getString(IgniteSystemProperties.IGNITE_LOCAL_HOST);

            InetSocketAddress inetSockAddr = new InetSocketAddress(userDefinedHost, 0);
            InetAddress addr = inetSockAddr.getAddress();
            IgniteBiTuple<Collection<String>, Collection<String>> locAddrs = IgniteUtils.resolveLocalAddresses(addr);

            if (ignoreLocHostname) {
                // If local hostname is ignored, then no hostname should be resolved.
                assertTrue(locAddrs.get2().isEmpty());
            }
            else {
                // If local hostname is not ignored, then we should receive example.com.
                assertFalse(locAddrs.get2().isEmpty());
                assertEquals("example.com", F.first(locAddrs.get2()));
            }
        }
        finally {
            // Clear local address cache as we have polluted it with this test.
            GridTestUtils.setFieldValue(IgniteUtils.class, "cachedLocalAddrAllHostNames", null);
            GridTestUtils.setFieldValue(IgniteUtils.class, "cachedLocalAddr", null);
        }
    }

    /**
     * Test enum.
     */
    private enum TestEnum {
        /** */
        E1,

        /** */
        E2,

        /** */
        E3
    }

    /** */
    @Documented @Retention(RetentionPolicy.RUNTIME) @Target(ElementType.TYPE)
    private @interface Ann1 {}

    /** */
    @Documented @Retention(RetentionPolicy.RUNTIME) @Target(ElementType.TYPE)
    private @interface Ann2 {}

    /** */
    private static class A1 implements I3, I5 {}

    /** */
    private static class A2 extends A1 {}

    /** */
    private static class A3 implements I5 {}

    /** */
    @Ann1 private interface I1 {}

    /** */
    private interface I2 extends I1 {}

    /** */
    private interface I3 extends I2 {}

    /** */
    @Ann2 private interface I4 {}

    /** */
    private interface I5 extends I4 {}

    /**
     * Test to verify the {@link U#hashToIndex(int, int)}.
     */
    @Test
    public void testHashToIndexDistribution() {
        assertEquals(0, U.hashToIndex(1, 1));
        assertEquals(0, U.hashToIndex(2, 1));
        assertEquals(1, U.hashToIndex(1, 2));
        assertEquals(0, U.hashToIndex(2, 2));

        assertEquals(1, U.hashToIndex(1, 4));
        assertEquals(2, U.hashToIndex(2, 4));
        assertEquals(3, U.hashToIndex(3, 4));
        assertEquals(0, U.hashToIndex(4, 4));
        assertEquals(1, U.hashToIndex(5, 4));
        assertEquals(0, U.hashToIndex(8, 4));
        assertEquals(3, U.hashToIndex(15, 4));

        assertEquals(1, U.hashToIndex(-1, 4));
        assertEquals(2, U.hashToIndex(-2, 4));
        assertEquals(3, U.hashToIndex(-3, 4));
        assertEquals(0, U.hashToIndex(-4, 4));
        assertEquals(1, U.hashToIndex(-5, 4));
        assertEquals(0, U.hashToIndex(-8, 4));
        assertEquals(3, U.hashToIndex(-15, 4));
    }

    /** Test {@link U#staticField(Class, String)} throws on unknown field. */
    @Test(expected = IgniteCheckedException.class)
    public void testReadUnknownStaticFieldFailed() throws Exception {
        U.staticField(String.class, "unknown_field");
    }

    /**
     * Test UUID conversions from string to binary and back.
     *
     */
    @Test
    public void testUuidConvertions() {
        Map<String, byte[]> map = new LinkedHashMap<>();

        map.put("2ec84557-f7c4-4a2e-aea8-251eb13acff3", new byte[] {
            46, -56, 69, 87, -9, -60, 74, 46, -82, -88, 37, 30, -79, 58, -49, -13
        });
        map.put("4e17b7b5-79e7-4db5-ac45-a644ead95b9e", new byte[] {
            78, 23, -73, -75, 121, -25, 77, -75, -84, 69, -90, 68, -22, -39, 91, -98
        });
        map.put("412daadb-e9e6-443b-8b87-8d7895fc2e53", new byte[] {
            65, 45, -86, -37, -23, -26, 68, 59, -117, -121, -115, 120, -107, -4, 46, 83
        });
        map.put("e71aabf4-4aad-4280-b4e9-3c310be0cb88", new byte[] {
            -25, 26, -85, -12, 74, -83, 66, -128, -76, -23, 60, 49, 11, -32, -53, -120
        });
        map.put("d4454cda-a81f-490f-9424-9bdfcc9cf610", new byte[] {
            -44, 69, 76, -38, -88, 31, 73, 15, -108, 36, -101, -33, -52, -100, -10, 16
        });
        map.put("3a584450-5e85-4b69-9f9d-043d89fef23b", new byte[] {
            58, 88, 68, 80, 94, -123, 75, 105, -97, -99, 4, 61, -119, -2, -14, 59
        });
        map.put("6c8baaec-f173-4a60-b566-240a87d7f81d", new byte[] {
            108, -117, -86, -20, -15, 115, 74, 96, -75, 102, 36, 10, -121, -41, -8, 29
        });
        map.put("d99c7102-79f7-4fb4-a665-d331cf285c20", new byte[] {
            -39, -100, 113, 2, 121, -9, 79, -76, -90, 101, -45, 49, -49, 40, 92, 32
        });
        map.put("007d56c7-5c8b-4279-a700-7f3f95946dde", new byte[] {
            0, 125, 86, -57, 92, -117, 66, 121, -89, 0, 127, 63, -107, -108, 109, -34
        });
        map.put("15627963-d8f9-4423-bedc-f6f89f7d3433", new byte[] {
            21, 98, 121, 99, -40, -7, 68, 35, -66, -36, -10, -8, -97, 125, 52, 51
        });

        for (Map.Entry<String, byte[]> e : map.entrySet()) {
            UUID uuid = UUID.fromString(e.getKey());
            UUID uuidFromBytes = IgniteUtils.bytesToUuid(e.getValue(), 0);

            assertEquals(uuid, uuidFromBytes);
            assertEquals(e.getKey(), uuid.toString());
            assertEquals(e.getKey(), uuidFromBytes.toString());

            byte[] bytes = IgniteUtils.uuidToBytes(uuid);

            assertTrue(e.getKey(), Arrays.equals(e.getValue(), bytes));
        }
    }

    /** */
    @Test
    public void testShortToBytes() {
        Map<String, Short> map = new HashMap<>();

        map.put("00-00", (short)0);
        map.put("00-0F", (short)0x0F);
        map.put("FF-F1", (short)-0x0F);
        map.put("27-10", (short)10000);
        map.put("D8-F0", (short)-10000);
        map.put("80-00", Short.MIN_VALUE);
        map.put("7F-FF", Short.MAX_VALUE);

        for (Map.Entry<String, Short> entry : map.entrySet()) {
            byte[] b = asByteArray(entry.getKey());

            Assert.assertArrayEquals(b, shortToBytes(entry.getValue()));
            Assert.assertEquals((short)entry.getValue(), bytesToShort(b, 0));

            byte[] tmp = new byte[2];

            shortToBytes(entry.getValue(), tmp, 0);
            Assert.assertArrayEquals(b, tmp);
        }
    }

    /** */
    @Test
    public void testIntToBytes() {
        Map<String, Integer> map = new HashMap<>();

        map.put("00-00-00-00", 0);
        map.put("00-FF-FF-FF", 0xFFFFFF);
        map.put("FF-00-00-01", -0xFFFFFF);
        map.put("3B-9A-CA-00", 1000000000);
        map.put("C4-65-36-00", -1000000000);
        map.put("80-00-00-00", Integer.MIN_VALUE);
        map.put("7F-FF-FF-FF", Integer.MAX_VALUE);

        for (Map.Entry<String, Integer> entry : map.entrySet()) {
            byte[] b = asByteArray(entry.getKey());

            Assert.assertArrayEquals(b, intToBytes(entry.getValue()));
            Assert.assertEquals((int)entry.getValue(), bytesToInt(b, 0));

            byte[] tmp = new byte[4];

            intToBytes(entry.getValue(), tmp, 0);
            Assert.assertArrayEquals(b, tmp);
        }
    }

    /** */
    @Test
    public void testLongToBytes() {
        Map<String, Long> map = new LinkedHashMap<>();

        map.put("00-00-00-00-00-00-00-00", 0L);
        map.put("00-00-00-00-00-FF-FF-FF", 0xFFFFFFL);
        map.put("FF-FF-FF-FF-FF-00-00-01", -0xFFFFFFL);
        map.put("00-00-00-00-3B-9A-CA-00", 1000000000L);
        map.put("FF-FF-FF-FF-C4-65-36-00", -1000000000L);
        map.put("00-00-AA-AA-AA-AA-AA-AA", 0xAAAAAAAAAAAAL);
        map.put("FF-FF-55-55-55-55-55-56", -0xAAAAAAAAAAAAL);
        map.put("0D-E0-B6-B3-A7-64-00-00", 1000000000000000000L);
        map.put("F2-1F-49-4C-58-9C-00-00", -1000000000000000000L);
        map.put("80-00-00-00-00-00-00-00", Long.MIN_VALUE);
        map.put("7F-FF-FF-FF-FF-FF-FF-FF", Long.MAX_VALUE);

        for (Map.Entry<String, Long> entry : map.entrySet()) {
            byte[] b = asByteArray(entry.getKey());

            Assert.assertArrayEquals(b, longToBytes(entry.getValue()));
            Assert.assertEquals((long)entry.getValue(), bytesToLong(b, 0));

            byte[] tmp = new byte[8];

            longToBytes(entry.getValue(), tmp, 0);
            Assert.assertArrayEquals(b, tmp);
        }
    }

    /** */
    private byte[] asByteArray(String text) {
        String[] split = text.split("-");
        byte[] b = new byte[split.length];

        for (int i = 0; i < split.length; i++)
            b[i] = (byte)Integer.parseInt(split[i], 16);

        return b;
    }
}

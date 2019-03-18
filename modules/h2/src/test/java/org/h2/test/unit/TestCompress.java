/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.h2.compress.CompressLZF;
import org.h2.compress.Compressor;
import org.h2.engine.Constants;
import org.h2.store.fs.FileUtils;
import org.h2.test.TestBase;
import org.h2.tools.CompressTool;
import org.h2.util.IOUtils;
import org.h2.util.New;
import org.h2.util.Task;

/**
 * Data compression tests.
 */
public class TestCompress extends TestBase {

    private boolean testPerformance;
    private final byte[] buff = new byte[10];

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    @Override
    public void test() throws Exception {
        if (testPerformance) {
            testDatabase();
            System.exit(0);
            return;
        }
        testVariableSizeInt();
        testMultiThreaded();
        if (config.big) {
            for (int i = 0; i < 100; i++) {
                test(i);
            }
            for (int i = 100; i < 10000; i += i + i + 1) {
                test(i);
            }
        } else {
            test(0);
            test(1);
            test(7);
            test(50);
            test(200);
        }
        test(4000000);
        testVariableEnd();
    }

    private void testVariableSizeInt() {
        assertEquals(1, CompressTool.getVariableIntLength(0));
        assertEquals(2, CompressTool.getVariableIntLength(0x80));
        assertEquals(3, CompressTool.getVariableIntLength(0x4000));
        assertEquals(4, CompressTool.getVariableIntLength(0x200000));
        assertEquals(5, CompressTool.getVariableIntLength(0x10000000));
        assertEquals(5, CompressTool.getVariableIntLength(-1));
        for (int x = 0; x < 0x20000; x++) {
            testVar(x);
            testVar(Integer.MIN_VALUE + x);
            testVar(Integer.MAX_VALUE - x);
            testVar(0x200000 + x - 100);
            testVar(0x10000000 + x - 100);
        }
    }

    private void testVar(int x) {
        int len = CompressTool.getVariableIntLength(x);
        int l2 = CompressTool.writeVariableInt(buff, 0, x);
        assertEquals(len, l2);
        int x2 = CompressTool.readVariableInt(buff, 0);
        assertEquals(x2, x);
    }

    private void testMultiThreaded() throws Exception {
        Task[] tasks = new Task[3];
        for (int i = 0; i < tasks.length; i++) {
            Task t = new Task() {
                @Override
                public void call() {
                    CompressTool tool = CompressTool.getInstance();
                    byte[] b = new byte[1024];
                    Random r = new Random();
                    while (!stop) {
                        r.nextBytes(b);
                        byte[] test = tool.expand(tool.compress(b, "LZF"));
                        assertEquals(b, test);
                    }
                }
            };
            tasks[i] = t;
            t.execute();
        }
        Thread.sleep(1000);
        for (Task t : tasks) {
            t.get();
        }
    }

    private void testVariableEnd() {
        CompressTool utils = CompressTool.getInstance();
        StringBuilder b = new StringBuilder();
        for (int i = 0; i < 90; i++) {
            b.append('0');
        }
        String prefix = b.toString();
        for (int i = 0; i < 100; i++) {
            b = new StringBuilder(prefix);
            for (int j = 0; j < i; j++) {
                b.append((char) ('1' + j));
            }
            String test = b.toString();
            byte[] in = test.getBytes();
            assertEquals(in, utils.expand(utils.compress(in, "LZF")));
        }
    }

    private void testDatabase() throws Exception {
        deleteDb("memFS:compress");
        Connection conn = getConnection("memFS:compress");
        Statement stat = conn.createStatement();
        ResultSet rs;
        rs = stat.executeQuery("select table_name from information_schema.tables");
        Statement stat2 = conn.createStatement();
        while (rs.next()) {
            String table = rs.getString(1);
            if (!"COLLATIONS".equals(table)) {
                stat2.execute("create table " + table +
                        " as select * from information_schema." + table);
            }
        }
        conn.close();
        Compressor compress = new CompressLZF();
        int pageSize = Constants.DEFAULT_PAGE_SIZE;
        byte[] buff2 = new byte[pageSize];
        byte[] test = new byte[2 * pageSize];
        compress.compress(buff2, pageSize, test, 0);
        for (int j = 0; j < 4; j++) {
            long time = System.nanoTime();
            for (int i = 0; i < 1000; i++) {
                InputStream in = FileUtils.newInputStream("memFS:compress.h2.db");
                while (true) {
                    int len = in.read(buff2);
                    if (len < 0) {
                        break;
                    }
                    compress.compress(buff2, pageSize, test, 0);
                }
                in.close();
            }
            System.out.println("compress: " +
                    TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - time) +
                    " ms");
        }

        for (int j = 0; j < 4; j++) {
            ArrayList<byte[]> comp = New.arrayList();
            InputStream in = FileUtils.newInputStream("memFS:compress.h2.db");
            while (true) {
                int len = in.read(buff2);
                if (len < 0) {
                    break;
                }
                int b = compress.compress(buff2, pageSize, test, 0);
                byte[] data = Arrays.copyOf(test, b);
                comp.add(data);
            }
            in.close();
            byte[] result = new byte[pageSize];
            long time = System.nanoTime();
            for (int i = 0; i < 1000; i++) {
                for (int k = 0; k < comp.size(); k++) {
                    byte[] data = comp.get(k);
                    compress.expand(data, 0, data.length, result, 0, pageSize);
                }
            }
            System.out.println("expand: " +
                    TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - time) +
                    " ms");
        }
    }

    private void test(int len) throws IOException {
        testByteArray(len);
        testByteBuffer(len);
    }

    private void testByteArray(int len) throws IOException {
        Random r = new Random(len);
        for (int pattern = 0; pattern < 4; pattern++) {
            byte[] b = new byte[len];
            switch (pattern) {
            case 0:
                // leave empty
                break;
            case 1: {
                r.nextBytes(b);
                break;
            }
            case 2: {
                for (int x = 0; x < len; x++) {
                    b[x] = (byte) (x & 10);
                }
                break;
            }
            case 3: {
                for (int x = 0; x < len; x++) {
                    b[x] = (byte) (x / 10);
                }
                break;
            }
            default:
            }
            if (r.nextInt(2) < 1) {
                for (int x = 0; x < len; x++) {
                    if (r.nextInt(20) < 1) {
                        b[x] = (byte) (r.nextInt(255));
                    }
                }
            }
            CompressTool utils = CompressTool.getInstance();
            // level 9 is highest, strategy 2 is huffman only
            for (String a : new String[] { "LZF", "No",
                    "Deflate", "Deflate level 9 strategy 2" }) {
                long time = System.nanoTime();
                byte[] out = utils.compress(b, a);
                byte[] test = utils.expand(out);
                if (testPerformance) {
                    System.out.println("p:" +
                            pattern +
                            " len: " +
                            out.length +
                            " time: " +
                            TimeUnit.NANOSECONDS.toMillis(System.nanoTime() -
                                    time) + " " + a);
                }
                assertEquals(b.length, test.length);
                assertEquals(b, test);
                Arrays.fill(test, (byte) 0);
                CompressTool.expand(out, test, 0);
                assertEquals(b, test);
            }
            for (String a : new String[] { null, "LZF", "DEFLATE", "ZIP", "GZIP" }) {
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                OutputStream out2 = CompressTool.wrapOutputStream(out, a, "test");
                IOUtils.copy(new ByteArrayInputStream(b), out2);
                out2.close();
                InputStream in = new ByteArrayInputStream(out.toByteArray());
                in = CompressTool.wrapInputStream(in, a, "test");
                out.reset();
                IOUtils.copy(in, out);
                assertEquals(b, out.toByteArray());
            }
        }
    }

    private void testByteBuffer(int len) {
        if (len < 4) {
            return;
        }
        Random r = new Random(len);
        CompressLZF comp = new CompressLZF();
        for (int pattern = 0; pattern < 4; pattern++) {
            byte[] b = new byte[len];
            switch (pattern) {
            case 0:
                // leave empty
                break;
            case 1: {
                r.nextBytes(b);
                break;
            }
            case 2: {
                for (int x = 0; x < len; x++) {
                    b[x] = (byte) (x & 10);
                }
                break;
            }
            case 3: {
                for (int x = 0; x < len; x++) {
                    b[x] = (byte) (x / 10);
                }
                break;
            }
            default:
            }
            if (r.nextInt(2) < 1) {
                for (int x = 0; x < len; x++) {
                    if (r.nextInt(20) < 1) {
                        b[x] = (byte) (r.nextInt(255));
                    }
                }
            }
            ByteBuffer buff = ByteBuffer.wrap(b);
            byte[] temp = new byte[100 + b.length * 2];
            int compLen = comp.compress(buff, 0, temp, 0);
            ByteBuffer test = ByteBuffer.wrap(temp, 0, compLen);
            byte[] exp = new byte[b.length];
            CompressLZF.expand(test, ByteBuffer.wrap(exp));
            assertEquals(b, exp);
        }
    }

}

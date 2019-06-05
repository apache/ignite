/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.store;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;
import java.util.TreeMap;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.h2.store.fs.FileUtils;
import org.h2.test.TestBase;

/**
 * Tests the MVStore.
 */
public class TestRandomMapOps extends TestBase {

    private static final boolean LOG = false;
    private int op;

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase test = TestBase.createCaller().init();
        test.config.big = true;
        test.test();
    }

    @Override
    public void test() throws Exception {
        testMap("memFS:randomOps.h3");
        FileUtils.delete("memFS:randomOps.h3");
    }

    private void testMap(String fileName) {
        int best = Integer.MAX_VALUE;
        int bestSeed = 0;
        Throwable failException = null;
        int size = getSize(100, 1000);
        for (int seed = 0; seed < 100; seed++) {
            FileUtils.delete(fileName);
            Throwable ex = null;
            try {
                testOps(fileName, size, seed);
                continue;
            } catch (Exception e) {
                ex = e;
            } catch (AssertionError e) {
                ex = e;
            }
            if (op < best) {
                trace(seed);
                bestSeed = seed;
                best = op;
                size = best;
                failException = ex;
                // System.out.println("seed:" + seed + " op:" + op + " " + ex);
            }
        }
        if (failException != null) {
            throw (AssertionError) new AssertionError("seed = " + bestSeed
                    + " op = " + best).initCause(failException);
        }
    }

    private void testOps(String fileName, int size, int seed) {
        FileUtils.delete(fileName);
        MVStore s = openStore(fileName);
        MVMap<Integer, byte[]> m = s.openMap("data");
        Random r = new Random(seed);
        op = 0;
        TreeMap<Integer, byte[]> map = new TreeMap<>();
        for (; op < size; op++) {
            int k = r.nextInt(100);
            byte[] v = new byte[r.nextInt(10) * 10];
            int type = r.nextInt(12);
            switch (type) {
            case 0:
            case 1:
            case 2:
            case 3:
                log(op, k, v, "m.put({0}, {1})");
                m.put(k, v);
                map.put(k, v);
                break;
            case 4:
            case 5:
                log(op, k, v, "m.remove({0})");
                m.remove(k);
                map.remove(k);
                break;
            case 6:
                log(op, k, v, "s.compact(90, 1024)");
                s.compact(90, 1024);
                break;
            case 7:
                log(op, k, v, "m.clear()");
                m.clear();
                map.clear();
                break;
            case 8:
                log(op, k, v, "s.commit()");
                s.commit();
                break;
            case 9:
                log(op, k, v, "s.commit()");
                s.commit();
                log(op, k, v, "s.close()");
                s.close();
                log(op, k, v, "s = openStore(fileName)");
                s = openStore(fileName);
                log(op, k, v, "m = s.openMap(\"data\")");
                m = s.openMap("data");
                break;
            case 10:
                log(op, k, v, "s.commit()");
                s.commit();
                log(op, k, v, "s.compactMoveChunks()");
                s.compactMoveChunks();
                break;
            case 11:
                log(op, k, v, "m.getKeyIndex({0})");
                ArrayList<Integer> keyList = new ArrayList<>(map.keySet());
                int index = Collections.binarySearch(keyList, k, null);
                int index2 = (int) m.getKeyIndex(k);
                assertEquals(index, index2);
                if (index >= 0) {
                    int k2 = m.getKey(index);
                    assertEquals(k2, k);
                }
                break;
            }
            assertEqualsMapValues(map.get(k), m.get(k));
            assertEquals(map.ceilingKey(k), m.ceilingKey(k));
            assertEquals(map.floorKey(k), m.floorKey(k));
            assertEquals(map.higherKey(k), m.higherKey(k));
            assertEquals(map.lowerKey(k), m.lowerKey(k));
            assertEquals(map.isEmpty(), m.isEmpty());
            assertEquals(map.size(), m.size());
            if (!map.isEmpty()) {
                assertEquals(map.firstKey(), m.firstKey());
                assertEquals(map.lastKey(), m.lastKey());
            }
        }
        s.close();
    }

    private static MVStore openStore(String fileName) {
        MVStore s = new MVStore.Builder().fileName(fileName).
                pageSplitSize(50).autoCommitDisabled().open();
        s.setRetentionTime(1000);
        return s;
    }

    private void assertEqualsMapValues(byte[] x, byte[] y) {
        if (x == null || y == null) {
            if (x != y) {
                assertTrue(x == y);
            }
        } else {
            assertEquals(x.length, y.length);
        }
    }

    /**
     * Log the operation
     *
     * @param op the operation id
     * @param k the key
     * @param v the value
     * @param msg the message
     */
    private static void log(int op, int k, byte[] v, String msg) {
        if (LOG) {
            msg = MessageFormat.format(msg, k,
                    v == null ? null : "new byte[" + v.length + "]");
            System.out.println(msg + "; // op " + op);
        }
    }

}

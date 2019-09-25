/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Random;

import org.h2.api.JavaObjectSerializer;
import org.h2.store.DataHandler;
import org.h2.store.FileStore;
import org.h2.store.LobStorageBackend;
import org.h2.test.TestBase;
import org.h2.util.SmallLRUCache;
import org.h2.util.TempFileDeleter;
import org.h2.util.ValueHashMap;
import org.h2.value.CompareMode;
import org.h2.value.Value;
import org.h2.value.ValueDouble;
import org.h2.value.ValueInt;

/**
 * Tests the value hash map.
 */
public class TestValueHashMap extends TestBase implements DataHandler {

    CompareMode compareMode = CompareMode.getInstance(null, 0);

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    @Override
    public void test() {
        testNotANumber();
        testRandomized();
    }

    private void testNotANumber() {
        ValueHashMap<Integer> map = ValueHashMap.newInstance();
        for (int i = 1; i < 100; i++) {
            double d = Double.longBitsToDouble(0x7ff0000000000000L | i);
            ValueDouble v = ValueDouble.get(d);
            map.put(v, null);
            assertEquals(1, map.size());
        }
    }

    private void testRandomized() {
        ValueHashMap<Value> map = ValueHashMap.newInstance();
        HashMap<Value, Value> hash = new HashMap<>();
        Random random = new Random(1);
        Comparator<Value> vc = new Comparator<Value>() {
            @Override
            public int compare(Value v1, Value v2) {
                return v1.compareTo(v2, compareMode);
            }
        };
        for (int i = 0; i < 10000; i++) {
            int op = random.nextInt(10);
            Value key = ValueInt.get(random.nextInt(100));
            Value value = ValueInt.get(random.nextInt(100));
            switch (op) {
            case 0:
                map.put(key, value);
                hash.put(key, value);
                break;
            case 1:
                map.remove(key);
                hash.remove(key);
                break;
            case 2:
                Value v1 = map.get(key);
                Value v2 = hash.get(key);
                assertTrue(v1 == null ? v2 == null : v1.equals(v2));
                break;
            case 3: {
                ArrayList<Value> a1 = map.keys();
                ArrayList<Value> a2 = new ArrayList<>(hash.keySet());
                assertEquals(a1.size(), a2.size());
                Collections.sort(a1, vc);
                Collections.sort(a2, vc);
                for (int j = 0; j < a1.size(); j++) {
                    assertTrue(a1.get(j).equals(a2.get(j)));
                }
                break;
            }
            case 4:
                ArrayList<Value> a1 = map.values();
                ArrayList<Value> a2 = new ArrayList<>(hash.values());
                assertEquals(a1.size(), a2.size());
                Collections.sort(a1, vc);
                Collections.sort(a2, vc);
                for (int j = 0; j < a1.size(); j++) {
                    assertTrue(a1.get(j).equals(a2.get(j)));
                }
                break;
            default:
            }
        }
    }

    @Override
    public String getDatabasePath() {
        return null;
    }

    @Override
    public FileStore openFile(String name, String mode, boolean mustExist) {
        return null;
    }

    @Override
    public void checkPowerOff() {
        // nothing to do
    }

    @Override
    public void checkWritingAllowed() {
        // nothing to do
    }

    @Override
    public int getMaxLengthInplaceLob() {
        return 0;
    }

    @Override
    public String getLobCompressionAlgorithm(int type) {
        return null;
    }

    @Override
    public Object getLobSyncObject() {
        return this;
    }

    @Override
    public SmallLRUCache<String, String[]> getLobFileListCache() {
        return null;
    }

    @Override
    public TempFileDeleter getTempFileDeleter() {
        return TempFileDeleter.getInstance();
    }

    @Override
    public LobStorageBackend getLobStorage() {
        return null;
    }

    @Override
    public int readLob(long lobId, byte[] hmac, long offset, byte[] buff,
            int off, int length) {
        return -1;
    }

    @Override
    public JavaObjectSerializer getJavaObjectSerializer() {
        return null;
    }

    @Override
    public CompareMode getCompareMode() {
        return compareMode;
    }
}

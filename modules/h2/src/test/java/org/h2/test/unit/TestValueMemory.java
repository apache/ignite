/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.io.ByteArrayInputStream;
import java.io.StringReader;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.Random;
import org.h2.api.JavaObjectSerializer;
import org.h2.engine.Constants;
import org.h2.store.DataHandler;
import org.h2.store.FileStore;
import org.h2.store.LobStorageFrontend;
import org.h2.test.TestBase;
import org.h2.test.utils.MemoryFootprint;
import org.h2.tools.SimpleResultSet;
import org.h2.util.SmallLRUCache;
import org.h2.util.TempFileDeleter;
import org.h2.util.Utils;
import org.h2.value.CompareMode;
import org.h2.value.DataType;
import org.h2.value.Value;
import org.h2.value.ValueArray;
import org.h2.value.ValueBoolean;
import org.h2.value.ValueByte;
import org.h2.value.ValueBytes;
import org.h2.value.ValueDate;
import org.h2.value.ValueDecimal;
import org.h2.value.ValueDouble;
import org.h2.value.ValueFloat;
import org.h2.value.ValueGeometry;
import org.h2.value.ValueInt;
import org.h2.value.ValueJavaObject;
import org.h2.value.ValueLong;
import org.h2.value.ValueNull;
import org.h2.value.ValueResultSet;
import org.h2.value.ValueShort;
import org.h2.value.ValueString;
import org.h2.value.ValueStringFixed;
import org.h2.value.ValueStringIgnoreCase;
import org.h2.value.ValueTime;
import org.h2.value.ValueTimestamp;
import org.h2.value.ValueTimestampTimeZone;
import org.h2.value.ValueUuid;

/**
 * Tests the memory consumption of values. Values can estimate how much memory
 * they occupy, and this tests if this estimation is correct.
 */
public class TestValueMemory extends TestBase implements DataHandler {

    private final Random random = new Random(1);
    private final SmallLRUCache<String, String[]> lobFileListCache = SmallLRUCache
            .newInstance(128);
    private LobStorageFrontend lobStorage;

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        // run using -javaagent:ext/h2-1.2.139.jar
        TestBase test = TestBase.createCaller().init();
        test.config.traceTest = true;
        test.test();
    }

    @Override
    public void test() throws SQLException {
        testCompare();
        for (int i = 0; i < Value.TYPE_COUNT; i++) {
            if (i == 23) {
                // this used to be "TIMESTAMP UTC", which was a short-lived
                // experiment
                continue;
            }
            Value v = create(i);
            String s = "type: " + v.getType() +
                    " calculated: " + v.getMemory() +
                    " real: " + MemoryFootprint.getObjectSize(v) + " " +
                    v.getClass().getName() + ": " + v.toString();
            trace(s);
        }
        for (int i = 0; i < Value.TYPE_COUNT; i++) {
            if (i == 23) {
                // this used to be "TIMESTAMP UTC", which was a short-lived
                // experiment
                continue;
            }
            Value v = create(i);
            if (v == ValueNull.INSTANCE && i == Value.GEOMETRY) {
                // jts not in the classpath, OK
                continue;
            }
            assertEquals(i, v.getType());
            testType(i);
        }
    }

    private void testCompare() {
        ValueDecimal a = ValueDecimal.get(new BigDecimal("0.0"));
        ValueDecimal b = ValueDecimal.get(new BigDecimal("-0.00"));
        assertTrue(a.hashCode() != b.hashCode());
        assertFalse(a.equals(b));
    }

    private void testType(int type) throws SQLException {
        System.gc();
        System.gc();
        long first = Utils.getMemoryUsed();
        ArrayList<Value> list = new ArrayList<>();
        long memory = 0;
        while (memory < 1000000) {
            Value v = create(type);
            memory += v.getMemory() + Constants.MEMORY_POINTER;
            list.add(v);
        }
        Object[] array = list.toArray();
        IdentityHashMap<Object, Object> map = new IdentityHashMap<>();
        for (Object a : array) {
            map.put(a, a);
        }
        int size = map.size();
        map.clear();
        map = null;
        list = null;
        System.gc();
        System.gc();
        long used = Utils.getMemoryUsed() - first;
        memory /= 1024;
        if (config.traceTest || used > memory * 3) {
            String msg = "Type: " + type + " Used memory: " + used +
                    " calculated: " + memory + " length: " + array.length + " size: " + size;
            if (config.traceTest) {
                trace(msg);
            }
            if (used > memory * 3) {
                fail(msg);
            }
        }
    }
    private Value create(int type) throws SQLException {
        switch (type) {
        case Value.NULL:
            return ValueNull.INSTANCE;
        case Value.BOOLEAN:
            return ValueBoolean.FALSE;
        case Value.BYTE:
            return ValueByte.get((byte) random.nextInt());
        case Value.SHORT:
            return ValueShort.get((short) random.nextInt());
        case Value.INT:
            return ValueInt.get(random.nextInt());
        case Value.LONG:
            return ValueLong.get(random.nextLong());
        case Value.DECIMAL:
            return ValueDecimal.get(new BigDecimal(random.nextInt()));
            // + "12123344563456345634565234523451312312"
        case Value.DOUBLE:
            return ValueDouble.get(random.nextDouble());
        case Value.FLOAT:
            return ValueFloat.get(random.nextFloat());
        case Value.TIME:
            return ValueTime.get(new java.sql.Time(random.nextLong()));
        case Value.DATE:
            return ValueDate.get(new java.sql.Date(random.nextLong()));
        case Value.TIMESTAMP:
            return ValueTimestamp.fromMillis(random.nextLong());
        case Value.TIMESTAMP_TZ:
            // clamp to max legal value
            long nanos = Math.max(Math.min(random.nextLong(),
                    24L * 60 * 60 * 1000 * 1000 * 1000 - 1), 0);
            int timeZoneOffsetMins = (int) (random.nextFloat() * (24 * 60))
                    - (12 * 60);
            return ValueTimestampTimeZone.fromDateValueAndNanos(
                    random.nextLong(), nanos, (short) timeZoneOffsetMins);
        case Value.BYTES:
            return ValueBytes.get(randomBytes(random.nextInt(1000)));
        case Value.STRING:
            return ValueString.get(randomString(random.nextInt(100)));
        case Value.STRING_IGNORECASE:
            return ValueStringIgnoreCase.get(randomString(random.nextInt(100)));
        case Value.BLOB: {
            int len = (int) Math.abs(random.nextGaussian() * 10);
            byte[] data = randomBytes(len);
            return getLobStorage().createBlob(new ByteArrayInputStream(data), len);
        }
        case Value.CLOB: {
            int len = (int) Math.abs(random.nextGaussian() * 10);
            String s = randomString(len);
            return getLobStorage().createClob(new StringReader(s), len);
        }
        case Value.ARRAY: {
            int len = random.nextInt(20);
            Value[] list = new Value[len];
            for (int i = 0; i < list.length; i++) {
                list[i] = create(Value.STRING);
            }
            return ValueArray.get(list);
        }
        case Value.RESULT_SET:
            return ValueResultSet.get(new SimpleResultSet());
        case Value.JAVA_OBJECT:
            return ValueJavaObject.getNoCopy(null, randomBytes(random.nextInt(100)), this);
        case Value.UUID:
            return ValueUuid.get(random.nextLong(), random.nextLong());
        case Value.STRING_FIXED:
            return ValueStringFixed.get(randomString(random.nextInt(100)));
        case Value.GEOMETRY:
            if (DataType.GEOMETRY_CLASS == null) {
                return ValueNull.INSTANCE;
            }
            return ValueGeometry.get("POINT (" + random.nextInt(100) + " " +
                    random.nextInt(100) + ")");
        default:
            throw new AssertionError("type=" + type);
        }
    }

    private byte[] randomBytes(int len) {
        byte[] data = new byte[len];
        if (random.nextBoolean()) {
            // don't initialize always (compression)
            random.nextBytes(data);
        }
        return data;
    }

    private String randomString(int len) {
        char[] chars = new char[len];
        if (random.nextBoolean()) {
            // don't initialize always (compression)
            for (int i = 0; i < chars.length; i++) {
                chars[i] = (char) (random.nextGaussian() * 100);
            }
        }
        return new String(chars);
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
    public String getDatabasePath() {
        return getBaseDir() + "/valueMemory";
    }

    @Override
    public String getLobCompressionAlgorithm(int type) {
        return "LZF";
    }

    @Override
    public Object getLobSyncObject() {
        return this;
    }

    @Override
    public int getMaxLengthInplaceLob() {
        return 100;
    }

    @Override
    public FileStore openFile(String name, String mode, boolean mustExist) {
        return FileStore.open(this, name, mode);
    }

    @Override
    public SmallLRUCache<String, String[]> getLobFileListCache() {
        return lobFileListCache;
    }

    @Override
    public TempFileDeleter getTempFileDeleter() {
        return TempFileDeleter.getInstance();
    }

    @Override
    public LobStorageFrontend getLobStorage() {
        if (lobStorage == null) {
            lobStorage = new LobStorageFrontend(this);
        }
        return lobStorage;
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
        return CompareMode.getInstance(null, 0);
    }
}
